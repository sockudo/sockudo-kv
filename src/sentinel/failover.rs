//! Sentinel Failover Manager
//!
//! Implements the failover state machine:
//! 1. WAIT_START - failover triggered, waiting to start
//! 2. SELECT_SLAVE - selecting the best replica
//! 3. SEND_SLAVEOF_NOONE - promoting selected replica
//! 4. WAIT_PROMOTION - waiting for replica to become master
//! 5. RECONF_SLAVES - reconfiguring other replicas
//! 6. UPDATE_CONFIG - broadcasting new configuration

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

use super::config::SharedSentinelConfig;
use super::election::LeaderElection;
use super::events::{SentinelEvent, SentinelEventPublisher};
use super::state::{
    FailoverState, MasterInstance, ReplicaInstance, SentinelState, current_time_ms,
};

/// Connection timeout
const CONNECT_TIMEOUT_MS: u64 = 1000;

/// Command timeout
const COMMAND_TIMEOUT_MS: u64 = 1000;

/// Promotion check interval
const PROMOTION_CHECK_INTERVAL_MS: u64 = 1000;

/// Maximum promotion wait time
const PROMOTION_MAX_WAIT_MS: u64 = 60000;

/// Sentinel failover manager
pub struct SentinelFailover {
    state: Arc<SentinelState>,
    // Note: config values are accessed via master.failover_timeout(), master.parallel_syncs(), etc.
    // No need to duplicate here since each master has its own config
    events: Arc<SentinelEventPublisher>,
    election: Arc<LeaderElection>,
}

impl SentinelFailover {
    /// Create a new failover manager
    pub fn new(
        state: Arc<SentinelState>,
        _config: Arc<SharedSentinelConfig>, // Kept for API compatibility but unused
        events: Arc<SentinelEventPublisher>,
        election: Arc<LeaderElection>,
    ) -> Self {
        Self {
            state,
            events,
            election,
        }
    }

    /// Start a failover for a master in ODOWN state
    pub async fn start_failover(&self, master: &Arc<MasterInstance>) -> Result<(), FailoverError> {
        // Check if failover is already in progress
        if master.flags.is_failover_in_progress() {
            return Err(FailoverError::AlreadyInProgress);
        }

        // Check if master is in ODOWN
        if !master.flags.is_odown() {
            return Err(FailoverError::NotInOdown);
        }

        // Mark failover as in progress
        master.flags.set_failover_in_progress(true);
        master.set_failover_state(FailoverState::WaitStart);
        master
            .failover_start_time
            .store(current_time_ms(), Ordering::Relaxed);

        // Request votes (leader election)
        let vote_result = self.election.request_votes(master).await;

        if !vote_result.is_leader {
            master.flags.set_failover_in_progress(false);
            master.set_failover_state(FailoverState::None);
            return Err(FailoverError::NotElected);
        }

        // Store failover epoch
        master
            .failover_epoch
            .store(vote_result.epoch, Ordering::Relaxed);

        // Run the failover state machine
        self.run_failover_state_machine(master).await
    }

    /// Force a manual failover (SENTINEL FAILOVER command)
    pub async fn force_failover(&self, master: &Arc<MasterInstance>) -> Result<(), FailoverError> {
        // Check if failover is already in progress
        if master.flags.is_failover_in_progress() {
            return Err(FailoverError::AlreadyInProgress);
        }

        // Mark failover as in progress (skip ODOWN check for manual failover)
        master.flags.set_failover_in_progress(true);
        master.set_failover_state(FailoverState::WaitStart);
        master
            .failover_start_time
            .store(current_time_ms(), Ordering::Relaxed);

        // Increment epoch
        let epoch = self.state.increment_epoch();
        master.failover_epoch.store(epoch, Ordering::Relaxed);

        // We're the leader for manual failover
        *master.failover_leader.write() =
            Some(String::from_utf8_lossy(&self.state.myid).to_string());
        master.leader_epoch.store(epoch, Ordering::Relaxed);

        let master_ip = master.ip.read().clone();
        let master_port = master.port.load(Ordering::Relaxed);

        self.events.publish(SentinelEvent::TryFailover {
            master_name: master.name.clone(),
            ip: master_ip.clone(),
            port: master_port,
        });

        // Run the failover state machine
        self.run_failover_state_machine(master).await
    }

    /// Run the failover state machine
    async fn run_failover_state_machine(
        &self,
        master: &Arc<MasterInstance>,
    ) -> Result<(), FailoverError> {
        let failover_timeout = master.failover_timeout();
        let start_time = master.failover_start_time.load(Ordering::Relaxed);

        // SELECT_SLAVE state
        master.set_failover_state(FailoverState::SelectSlave);
        let master_ip = master.ip.read().clone();
        let master_port = master.port.load(Ordering::Relaxed);

        self.events
            .publish(SentinelEvent::FailoverStateSelectSlave {
                master_name: master.name.clone(),
                ip: master_ip.clone(),
                port: master_port,
            });

        // Select best replica
        let selected_replica = match master.select_replica_for_failover() {
            Some(r) => r,
            None => {
                log::error!(
                    "No suitable replica found for failover of master {}",
                    master.name
                );
                self.events.publish(SentinelEvent::NoGoodSlave {
                    master_name: master.name.clone(),
                    ip: master_ip.clone(),
                    port: master_port,
                });
                self.abort_failover(master, "no suitable replica");
                return Err(FailoverError::NoSuitableReplica);
            }
        };

        // Store promoted replica
        *master.promoted_replica.write() = Some(selected_replica.key());

        self.events.publish(SentinelEvent::SelectedSlave {
            slave_ip: selected_replica.ip.clone(),
            slave_port: selected_replica.port,
            master_name: master.name.clone(),
            master_ip: master_ip.clone(),
            master_port,
        });

        log::info!(
            "Selected replica {}:{} for promotion",
            selected_replica.ip,
            selected_replica.port
        );

        // SEND_SLAVEOF_NOONE state
        master.set_failover_state(FailoverState::SendSlaveofNoone);

        self.events
            .publish(SentinelEvent::FailoverStateSendSlaveofNoone {
                slave_ip: selected_replica.ip.clone(),
                slave_port: selected_replica.port,
                master_name: master.name.clone(),
                master_ip: master_ip.clone(),
                master_port,
            });

        // Send REPLICAOF NO ONE to the selected replica
        if let Err(e) = self.send_replicaof_no_one(&selected_replica).await {
            log::error!(
                "Failed to send REPLICAOF NO ONE to {}:{}: {}",
                selected_replica.ip,
                selected_replica.port,
                e
            );

            // Check timeout
            if current_time_ms() - start_time > failover_timeout {
                self.abort_failover(master, "timeout");
                return Err(FailoverError::Timeout);
            }
        }

        // WAIT_PROMOTION state
        master.set_failover_state(FailoverState::WaitPromotion);
        selected_replica.flags.set_promoted(true);

        self.events
            .publish(SentinelEvent::FailoverStateWaitPromotion {
                slave_ip: selected_replica.ip.clone(),
                slave_port: selected_replica.port,
                master_name: master.name.clone(),
                master_ip: master_ip.clone(),
                master_port,
            });

        // Wait for replica to become master
        let promotion_result = self
            .wait_for_promotion(&selected_replica, failover_timeout)
            .await;

        if !promotion_result {
            log::error!(
                "Replica {}:{} did not become master in time",
                selected_replica.ip,
                selected_replica.port
            );
            self.abort_failover(master, "promotion timeout");
            return Err(FailoverError::PromotionFailed);
        }

        log::info!(
            "Replica {}:{} successfully promoted to master",
            selected_replica.ip,
            selected_replica.port
        );

        // RECONF_SLAVES state
        master.set_failover_state(FailoverState::ReconfSlaves);

        self.events
            .publish(SentinelEvent::FailoverStateReconfSlaves {
                master_name: master.name.clone(),
                ip: master_ip.clone(),
                port: master_port,
            });

        // Reconfigure other replicas
        self.reconfigure_replicas(master, &selected_replica).await;

        // UPDATE_CONFIG state - update master address
        master.set_failover_state(FailoverState::UpdateConfig);

        let old_ip = master.ip.read().clone();
        let old_port = master.port.load(Ordering::Relaxed);

        *master.ip.write() = selected_replica.ip.clone();
        master.port.store(selected_replica.port, Ordering::Relaxed);
        master.config_epoch.fetch_add(1, Ordering::SeqCst);

        // Publish switch-master event
        self.events.publish(SentinelEvent::SwitchMaster {
            master_name: master.name.clone(),
            old_ip: old_ip.clone(),
            old_port,
            new_ip: selected_replica.ip.clone(),
            new_port: selected_replica.port,
        });

        self.events.publish(SentinelEvent::FailoverEnd {
            master_name: master.name.clone(),
            ip: selected_replica.ip.clone(),
            port: selected_replica.port,
        });

        // Clear failover state
        master.flags.set_failover_in_progress(false);
        master.flags.set_odown(false);
        master.flags.set_sdown(false);
        master.set_failover_state(FailoverState::None);

        // Remove promoted replica from replicas list (it's now the master)
        master.replicas.remove(&selected_replica.key());

        // Add old master as a replica
        let old_replica = Arc::new(ReplicaInstance::new(old_ip.clone(), old_port));
        master
            .replicas
            .insert(format!("{}:{}", old_ip, old_port), old_replica);

        master.update_counts();

        log::info!(
            "Failover of master {} completed. New master: {}:{}",
            master.name,
            selected_replica.ip,
            selected_replica.port
        );

        Ok(())
    }

    /// Send REPLICAOF NO ONE to a replica
    async fn send_replicaof_no_one(&self, replica: &ReplicaInstance) -> Result<(), std::io::Error> {
        let addr = format!("{}:{}", replica.ip, replica.port);

        let connect_result = timeout(
            Duration::from_millis(CONNECT_TIMEOUT_MS),
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

        // REPLICAOF NO ONE
        let cmd = b"*3\r\n$9\r\nREPLICAOF\r\n$2\r\nNO\r\n$3\r\nONE\r\n";
        stream.write_all(cmd).await?;

        // Read response
        let mut buf = [0u8; 64];
        let read_result = timeout(
            Duration::from_millis(COMMAND_TIMEOUT_MS),
            stream.read(&mut buf),
        )
        .await;

        match read_result {
            Ok(Ok(n)) if n > 0 => {
                let response = String::from_utf8_lossy(&buf[..n]);
                if response.starts_with('+') || response.starts_with("OK") {
                    Ok(())
                } else {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "command failed",
                    ))
                }
            }
            Ok(Ok(_)) => Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "empty response",
            )),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "read timeout",
            )),
        }
    }

    /// Wait for a replica to become master
    async fn wait_for_promotion(&self, replica: &ReplicaInstance, timeout_ms: u64) -> bool {
        let start = current_time_ms();

        while current_time_ms() - start < timeout_ms.min(PROMOTION_MAX_WAIT_MS) {
            // Check INFO for role change
            if let Ok(info) = self.get_info(&replica.ip, replica.port).await {
                if info.contains("role:master") {
                    return true;
                }
            }

            sleep(Duration::from_millis(PROMOTION_CHECK_INTERVAL_MS)).await;
        }

        false
    }

    /// Get INFO from an instance
    async fn get_info(&self, ip: &str, port: u16) -> Result<String, std::io::Error> {
        let addr = format!("{}:{}", ip, port);

        let connect_result = timeout(
            Duration::from_millis(CONNECT_TIMEOUT_MS),
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

        // INFO replication
        let cmd = b"*2\r\n$4\r\nINFO\r\n$11\r\nreplication\r\n";
        stream.write_all(cmd).await?;

        // Read response
        let mut response = Vec::with_capacity(1024);
        let mut buf = [0u8; 512];

        let read_result = timeout(Duration::from_millis(COMMAND_TIMEOUT_MS * 2), async {
            loop {
                match stream.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        response.extend_from_slice(&buf[..n]);
                        if n < buf.len() {
                            break;
                        }
                    }
                    Err(e) => return Err(e),
                }
            }
            Ok(())
        })
        .await;

        match read_result {
            Ok(Ok(())) => Ok(String::from_utf8_lossy(&response).to_string()),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "read timeout",
            )),
        }
    }

    /// Reconfigure other replicas to follow the new master
    async fn reconfigure_replicas(
        &self,
        master: &Arc<MasterInstance>,
        new_master: &ReplicaInstance,
    ) {
        let parallel_syncs = master.parallel_syncs() as usize;
        let replicas: Vec<Arc<ReplicaInstance>> = master
            .replicas
            .iter()
            .map(|e| e.value().clone())
            .filter(|r| r.key() != new_master.key())
            .filter(|r| !r.flags.is_sdown())
            .collect();

        // Reconfigure in batches based on parallel_syncs
        for chunk in replicas.chunks(parallel_syncs) {
            let mut handles = Vec::new();

            for replica in chunk {
                let replica = replica.clone();
                let new_master_ip = new_master.ip.clone();
                let new_master_port = new_master.port;
                let master_name = master.name.clone();
                let events = self.events.clone();

                handles.push(tokio::spawn(async move {
                    events.publish(SentinelEvent::SlaveReconfSent {
                        slave_ip: replica.ip.clone(),
                        slave_port: replica.port,
                        master_name: master_name.clone(),
                        master_ip: new_master_ip.clone(),
                        master_port: new_master_port,
                    });

                    replica.flags.set_reconf_sent(true);

                    let result =
                        send_replicaof(&replica.ip, replica.port, &new_master_ip, new_master_port)
                            .await;

                    if result.is_ok() {
                        replica.flags.set_reconf_inprog(true);
                        events.publish(SentinelEvent::SlaveReconfInprog {
                            slave_ip: replica.ip.clone(),
                            slave_port: replica.port,
                            master_name: master_name.clone(),
                            master_ip: new_master_ip.clone(),
                            master_port: new_master_port,
                        });

                        // Wait briefly for replication to start
                        sleep(Duration::from_millis(100)).await;

                        replica.flags.set_reconf_done(true);
                        replica.flags.set_reconf_inprog(false);
                        replica.flags.set_reconf_sent(false);

                        events.publish(SentinelEvent::SlaveReconfDone {
                            slave_ip: replica.ip.clone(),
                            slave_port: replica.port,
                            master_name,
                            master_ip: new_master_ip,
                            master_port: new_master_port,
                        });
                    }

                    result
                }));
            }

            // Wait for this batch to complete
            for handle in handles {
                let _ = handle.await;
            }
        }
    }

    /// Abort a failover
    fn abort_failover(&self, master: &Arc<MasterInstance>, reason: &str) {
        let ip = master.ip.read().clone();
        let port = master.port.load(Ordering::Relaxed);

        log::warn!("Aborting failover of master {}: {}", master.name, reason);

        master.flags.set_failover_in_progress(false);
        master.set_failover_state(FailoverState::None);
        *master.promoted_replica.write() = None;

        if reason == "timeout" {
            self.events.publish(SentinelEvent::FailoverEndForTimeout {
                master_name: master.name.clone(),
                ip,
                port,
            });
        } else {
            self.events.publish(SentinelEvent::FailoverAborted {
                master_name: master.name.clone(),
                ip,
                port,
            });
        }
    }
}

/// Send REPLICAOF command to a replica
async fn send_replicaof(
    ip: &str,
    port: u16,
    master_ip: &str,
    master_port: u16,
) -> Result<(), std::io::Error> {
    let addr = format!("{}:{}", ip, port);

    let connect_result = timeout(
        Duration::from_millis(CONNECT_TIMEOUT_MS),
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

    // REPLICAOF <master_ip> <master_port>
    let cmd = format!(
        "*3\r\n$9\r\nREPLICAOF\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
        master_ip.len(),
        master_ip,
        master_port.to_string().len(),
        master_port
    );

    stream.write_all(cmd.as_bytes()).await?;

    // Read response
    let mut buf = [0u8; 64];
    let read_result = timeout(
        Duration::from_millis(COMMAND_TIMEOUT_MS),
        stream.read(&mut buf),
    )
    .await;

    match read_result {
        Ok(Ok(n)) if n > 0 => {
            let response = String::from_utf8_lossy(&buf[..n]);
            if response.starts_with('+') {
                Ok(())
            } else {
                Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "command failed",
                ))
            }
        }
        Ok(Ok(_)) => Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "empty response",
        )),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "read timeout",
        )),
    }
}

/// Failover error types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailoverError {
    /// Failover already in progress
    AlreadyInProgress,
    /// Master not in ODOWN state
    NotInOdown,
    /// Not elected as leader
    NotElected,
    /// No suitable replica found
    NoSuitableReplica,
    /// Timeout during failover
    Timeout,
    /// Promotion failed
    PromotionFailed,
    /// Connection error
    ConnectionError(String),
}

impl std::fmt::Display for FailoverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyInProgress => write!(f, "failover already in progress"),
            Self::NotInOdown => write!(f, "master is not in ODOWN state"),
            Self::NotElected => write!(f, "not elected as failover leader"),
            Self::NoSuitableReplica => write!(f, "no suitable replica for failover"),
            Self::Timeout => write!(f, "failover timeout"),
            Self::PromotionFailed => write!(f, "replica promotion failed"),
            Self::ConnectionError(msg) => write!(f, "connection error: {}", msg),
        }
    }
}

impl std::error::Error for FailoverError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_failover_error_display() {
        assert_eq!(
            FailoverError::AlreadyInProgress.to_string(),
            "failover already in progress"
        );
        assert_eq!(
            FailoverError::NoSuitableReplica.to_string(),
            "no suitable replica for failover"
        );
    }
}
