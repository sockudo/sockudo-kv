//! Sentinel Leader Election
//!
//! Implements Raft-like leader election for failover coordination:
//! - Request votes from other Sentinels via SENTINEL is-master-down-by-addr
//! - Grant votes (one vote per epoch)
//! - Become leader with majority votes

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

use super::events::{SentinelEvent, SentinelEventPublisher};
use super::state::{MasterInstance, SentinelState};

/// Connection timeout for vote requests
const VOTE_REQUEST_TIMEOUT_MS: u64 = 500;

/// Leader election manager
pub struct LeaderElection {
    state: Arc<SentinelState>,
    events: Arc<SentinelEventPublisher>,
}

impl LeaderElection {
    /// Create a new leader election manager
    pub fn new(state: Arc<SentinelState>, events: Arc<SentinelEventPublisher>) -> Self {
        Self { state, events }
    }

    /// Request votes from other Sentinels for a master failover
    /// Returns (votes_received, leader_runid, leader_epoch)
    pub async fn request_votes(&self, master: &Arc<MasterInstance>) -> VoteResult {
        let my_id = String::from_utf8_lossy(&self.state.myid).to_string();
        let master_ip = master.ip.read().clone();
        let master_port = master.port.load(Ordering::Relaxed);

        // Increment epoch
        let epoch = self.state.increment_epoch();

        log::info!(
            "Starting leader election for master {} at epoch {}",
            master.name,
            epoch
        );
        self.events.publish(SentinelEvent::TryFailover {
            master_name: master.name.clone(),
            ip: master_ip.clone(),
            port: master_port,
        });

        // Collect votes from all sentinels
        let mut votes: u32 = 1; // We vote for ourselves
        let mut responses = Vec::new();

        // Request votes in parallel
        let mut handles = Vec::new();

        for entry in master.sentinels.iter() {
            let sentinel = entry.value().clone();

            // Skip sentinels in SDOWN
            if sentinel.flags.is_sdown() {
                continue;
            }

            let master_name = master.name.clone();
            let master_ip = master_ip.clone();
            let my_id_clone = my_id.clone();

            handles.push(tokio::spawn(async move {
                request_vote_from_sentinel(
                    &sentinel.ip,
                    sentinel.port,
                    &master_name,
                    &master_ip,
                    master_port,
                    epoch,
                    &my_id_clone,
                )
                .await
            }));
        }

        // Collect results
        for handle in handles {
            match handle.await {
                Ok(Ok(response)) => {
                    if response.vote_granted {
                        votes += 1;
                    }
                    responses.push(response);
                }
                Ok(Err(e)) => {
                    log::debug!("Vote request failed: {}", e);
                }
                Err(e) => {
                    log::debug!("Vote request task panicked: {}", e);
                }
            }
        }

        // Check if we have majority
        let total_sentinels = master.sentinels.len() + 1; // +1 for ourselves
        let majority = (total_sentinels / 2) + 1;
        let is_leader = votes as usize >= majority;

        if is_leader {
            log::info!(
                "Elected as leader for master {} (votes: {}/{})",
                master.name,
                votes,
                total_sentinels
            );
            self.events.publish(SentinelEvent::ElectedLeader {
                master_name: master.name.clone(),
                ip: master_ip.clone(),
                port: master_port,
            });
        } else {
            log::info!(
                "Not elected as leader for master {} (votes: {}/{}, need {})",
                master.name,
                votes,
                total_sentinels,
                majority
            );
        }

        VoteResult {
            votes,
            epoch,
            is_leader,
            total_sentinels: total_sentinels as u32,
        }
    }

    /// Handle an incoming vote request
    pub fn handle_vote_request(
        &self,
        master_name: &str,
        master_ip: &str,
        master_port: u16,
        request_epoch: u64,
        requester_runid: &str,
    ) -> VoteResponse {
        let master = match self.state.get_master(master_name) {
            Some(m) => m,
            None => {
                return VoteResponse {
                    vote_granted: false,
                    leader_runid: String::new(),
                    leader_epoch: 0,
                };
            }
        };

        // Check if master is in SDOWN
        if !master.flags.is_sdown() {
            log::debug!(
                "Vote request rejected: master {} is not in SDOWN",
                master_name
            );
            return VoteResponse {
                vote_granted: false,
                leader_runid: String::new(),
                leader_epoch: 0,
            };
        }

        // Check if requester IP/port matches our knowledge
        let our_master_ip = master.ip.read().clone();
        let our_master_port = master.port.load(Ordering::Relaxed);

        if master_ip != our_master_ip || master_port != our_master_port {
            log::debug!("Vote request rejected: master address mismatch");
            return VoteResponse {
                vote_granted: false,
                leader_runid: String::new(),
                leader_epoch: 0,
            };
        }

        // Check if we already voted in this epoch
        let current_epoch = self.state.epoch();
        let leader_epoch = master.leader_epoch.load(Ordering::Relaxed);

        if request_epoch <= leader_epoch {
            // Already voted in this or higher epoch
            let current_leader = master.failover_leader.read().clone();
            log::debug!(
                "Already voted in epoch {} for {:?}",
                leader_epoch,
                current_leader
            );
            return VoteResponse {
                vote_granted: false,
                leader_runid: current_leader.unwrap_or_default(),
                leader_epoch,
            };
        }

        // Grant vote
        *master.failover_leader.write() = Some(requester_runid.to_string());
        master.leader_epoch.store(request_epoch, Ordering::Relaxed);

        // Update our epoch if needed
        if request_epoch > current_epoch {
            self.state
                .current_epoch
                .store(request_epoch, Ordering::SeqCst);
        }

        log::info!(
            "Granted vote to {} for master {} at epoch {}",
            requester_runid,
            master_name,
            request_epoch
        );

        VoteResponse {
            vote_granted: true,
            leader_runid: requester_runid.to_string(),
            leader_epoch: request_epoch,
        }
    }

    /// Check if this sentinel is the current leader for a master
    pub fn is_leader(&self, master: &MasterInstance) -> bool {
        let leader = master.failover_leader.read();
        if let Some(leader_id) = leader.as_ref() {
            let my_id = String::from_utf8_lossy(&self.state.myid);
            leader_id == &*my_id
        } else {
            false
        }
    }
}

/// Result of a vote request round
#[derive(Debug, Clone)]
pub struct VoteResult {
    /// Number of votes received
    pub votes: u32,
    /// Epoch used for this election
    pub epoch: u64,
    /// Whether we became leader
    pub is_leader: bool,
    /// Total number of sentinels
    pub total_sentinels: u32,
}

/// Response to a vote request
#[derive(Debug, Clone)]
pub struct VoteResponse {
    /// Whether vote was granted
    pub vote_granted: bool,
    /// The leader's runid (if known)
    pub leader_runid: String,
    /// The epoch of the vote
    pub leader_epoch: u64,
}

/// Request a vote from a specific sentinel
async fn request_vote_from_sentinel(
    ip: &str,
    port: u16,
    _master_name: &str,
    master_ip: &str,
    master_port: u16,
    epoch: u64,
    requester_runid: &str,
) -> Result<VoteResponse, std::io::Error> {
    let addr = format!("{}:{}", ip, port);

    let connect_result = timeout(
        Duration::from_millis(VOTE_REQUEST_TIMEOUT_MS),
        TcpStream::connect(&addr),
    )
    .await;

    let mut stream = match connect_result {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => return Err(e),
        Err(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "connect timeout",
            ));
        }
    };

    // SENTINEL is-master-down-by-addr <ip> <port> <current-epoch> <runid>
    // When runid is "*", it's just asking about SDOWN status
    // When runid is a specific ID, it's requesting a vote
    let cmd = format!(
        "*6\r\n$8\r\nSENTINEL\r\n$22\r\nis-master-down-by-addr\r\n${}\r\n{}\r\n${}\r\n{}\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
        master_ip.len(),
        master_ip,
        master_port.to_string().len(),
        master_port,
        epoch.to_string().len(),
        epoch,
        requester_runid.len(),
        requester_runid
    );

    stream.write_all(cmd.as_bytes()).await?;

    // Read response
    // Format: *3\r\n:<down_state>\r\n$<len>\r\n<leader_runid>\r\n:<leader_epoch>\r\n
    let mut buf = [0u8; 256];
    let read_result = timeout(
        Duration::from_millis(VOTE_REQUEST_TIMEOUT_MS),
        stream.read(&mut buf),
    )
    .await;

    let n = match read_result {
        Ok(Ok(n)) if n > 0 => n,
        Ok(Ok(_)) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "empty response",
            ));
        }
        Ok(Err(e)) => return Err(e),
        Err(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "read timeout",
            ));
        }
    };

    let response = String::from_utf8_lossy(&buf[..n]);
    parse_vote_response(&response)
}

/// Parse the response from SENTINEL is-master-down-by-addr
fn parse_vote_response(response: &str) -> Result<VoteResponse, std::io::Error> {
    // Response format: *3\r\n:<down_state>\r\n$<len>\r\n<leader_runid or *>\r\n:<leader_epoch>\r\n
    let lines: Vec<&str> = response.split("\r\n").collect();

    if lines.len() < 5 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid response format",
        ));
    }

    // Parse down_state (1 = down)
    let _down_state = lines
        .get(1)
        .and_then(|s| s.strip_prefix(':'))
        .and_then(|s| s.parse::<i32>().ok())
        .unwrap_or(0);

    // Parse leader_runid
    let leader_runid = lines.get(3).map(|s| s.to_string()).unwrap_or_default();
    let vote_granted = !leader_runid.is_empty() && leader_runid != "*";

    // Parse leader_epoch
    let leader_epoch = lines
        .get(4)
        .and_then(|s| s.strip_prefix(':'))
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    Ok(VoteResponse {
        vote_granted,
        leader_runid,
        leader_epoch,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_vote_response() {
        let response = "*3\r\n:1\r\n$40\r\nabc123def456abc123def456abc123def456abcd\r\n:5\r\n";
        let result = parse_vote_response(response).unwrap();

        assert!(result.vote_granted);
        assert_eq!(
            result.leader_runid,
            "abc123def456abc123def456abc123def456abcd"
        );
        assert_eq!(result.leader_epoch, 5);
    }

    #[test]
    fn test_parse_vote_response_no_vote() {
        let response = "*3\r\n:1\r\n$1\r\n*\r\n:0\r\n";
        let result = parse_vote_response(response).unwrap();

        assert!(!result.vote_granted);
        assert_eq!(result.leader_runid, "*");
        assert_eq!(result.leader_epoch, 0);
    }
}
