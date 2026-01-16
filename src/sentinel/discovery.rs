//! Sentinel Auto-Discovery
//!
//! Implements auto-discovery of:
//! - Other Sentinels via Pub/Sub hello messages
//! - Replicas from master INFO output
//!
//! Hello message format:
//! <sentinel_ip>,<sentinel_port>,<sentinel_runid>,<current_epoch>,
//! <master_name>,<master_ip>,<master_port>,<master_config_epoch>

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{interval, timeout};

use super::config::SharedSentinelConfig;
use super::events::{SentinelEvent, SentinelEventPublisher};
use super::state::{MasterInstance, SentinelInstance, SentinelState, current_time_ms};

/// Hello message publish interval (2 seconds)
const HELLO_PERIOD_MS: u64 = 2000;

/// Hello channel name
const SENTINEL_HELLO_CHANNEL: &str = "__sentinel__:hello";

/// Connection timeout
const CONNECT_TIMEOUT_MS: u64 = 500;

/// Discovery service for auto-discovering sentinels and replicas
pub struct DiscoveryService {
    state: Arc<SentinelState>,
    config: Arc<SharedSentinelConfig>,
    events: Arc<SentinelEventPublisher>,
}

impl DiscoveryService {
    /// Create a new discovery service
    pub fn new(
        state: Arc<SentinelState>,
        config: Arc<SharedSentinelConfig>,
        events: Arc<SentinelEventPublisher>,
    ) -> Self {
        Self {
            state,
            config,
            events,
        }
    }

    /// Start the discovery loop
    pub async fn run(&self) {
        let mut hello_interval = interval(Duration::from_millis(HELLO_PERIOD_MS));

        loop {
            hello_interval.tick().await;
            self.publish_hello_to_all().await;
        }
    }

    /// Publish hello message to all monitored masters and replicas
    pub async fn publish_hello_to_all(&self) {
        for entry in self.state.masters.iter() {
            let master = entry.value().clone();

            // Publish to master
            let master_ip = master.ip.read().clone();
            let master_port = master.port.load(Ordering::Relaxed);

            if let Err(e) = self.publish_hello(&master_ip, master_port, &master).await {
                log::debug!(
                    "Failed to publish hello to master {}:{}: {}",
                    master_ip,
                    master_port,
                    e
                );
            }

            // Publish to each replica
            for replica_entry in master.replicas.iter() {
                let replica = replica_entry.value();
                if let Err(e) = self.publish_hello(&replica.ip, replica.port, &master).await {
                    log::debug!(
                        "Failed to publish hello to replica {}:{}: {}",
                        replica.ip,
                        replica.port,
                        e
                    );
                }
            }
        }
    }

    /// Publish hello message to a specific instance
    async fn publish_hello(
        &self,
        ip: &str,
        port: u16,
        master: &MasterInstance,
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

        // Build hello message
        let message = self.build_hello_message(master);

        // PUBLISH __sentinel__:hello <message>
        let channel = SENTINEL_HELLO_CHANNEL;
        let cmd = format!(
            "*3\r\n$7\r\nPUBLISH\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
            channel.len(),
            channel,
            message.len(),
            message
        );

        stream.write_all(cmd.as_bytes()).await?;

        // Read response (we don't really care about it)
        let mut buf = [0u8; 64];
        let _ = timeout(Duration::from_millis(100), stream.read(&mut buf)).await;

        Ok(())
    }

    /// Build hello message for a master
    fn build_hello_message(&self, master: &MasterInstance) -> String {
        let config = self.config.read();
        let sentinel_ip = config.get_announce_ip();
        let sentinel_port = config.get_announce_port();
        let current_epoch = self.state.epoch();

        let master_ip = master.ip.read().clone();
        let master_port = master.port.load(Ordering::Relaxed);
        let master_config_epoch = master.config_epoch.load(Ordering::Relaxed);

        // Format: sentinel_ip,sentinel_port,sentinel_runid,current_epoch,
        //         master_name,master_ip,master_port,master_config_epoch
        format!(
            "{},{},{},{},{},{},{},{}",
            sentinel_ip,
            sentinel_port,
            String::from_utf8_lossy(&self.state.myid),
            current_epoch,
            master.name,
            master_ip,
            master_port,
            master_config_epoch
        )
    }

    /// Process an incoming hello message
    pub fn process_hello(&self, master_name: &str, message: &str) {
        let parts: Vec<&str> = message.split(',').collect();
        if parts.len() < 8 {
            log::debug!("Invalid hello message format: {}", message);
            return;
        }

        let sentinel_ip = parts[0];
        let sentinel_port: u16 = match parts[1].parse() {
            Ok(p) => p,
            Err(_) => return,
        };
        let sentinel_runid = parts[2];
        let current_epoch: u64 = parts[3].parse().unwrap_or(0);
        let msg_master_name = parts[4];
        let master_ip = parts[5];
        let master_port: u16 = parts[6].parse().unwrap_or(0);
        let master_config_epoch: u64 = parts[7].parse().unwrap_or(0);

        // Ignore our own hello
        if sentinel_runid == String::from_utf8_lossy(&self.state.myid) {
            return;
        }

        // Check if this is for a master we're monitoring
        if msg_master_name != master_name {
            return;
        }

        let master = match self.state.get_master(master_name) {
            Some(m) => m,
            None => return,
        };

        // Update current epoch if newer
        let our_epoch = self.state.epoch();
        if current_epoch > our_epoch {
            self.state
                .current_epoch
                .store(current_epoch, Ordering::SeqCst);
            log::info!(
                "Updated current epoch to {} from sentinel {}:{}",
                current_epoch,
                sentinel_ip,
                sentinel_port
            );
            self.events.publish(SentinelEvent::NewEpoch {
                epoch: current_epoch,
            });
        }

        // Update master config epoch if newer
        let our_master_epoch = master.config_epoch.load(Ordering::Relaxed);
        if master_config_epoch > our_master_epoch {
            master
                .config_epoch
                .store(master_config_epoch, Ordering::SeqCst);

            // Also update master address if it changed
            let our_master_ip = master.ip.read().clone();
            let our_master_port = master.port.load(Ordering::Relaxed);

            if master_ip != our_master_ip || master_port != our_master_port {
                log::info!(
                    "Master {} address updated from {}:{} to {}:{} (epoch {})",
                    master_name,
                    our_master_ip,
                    our_master_port,
                    master_ip,
                    master_port,
                    master_config_epoch
                );

                *master.ip.write() = master_ip.to_string();
                master.port.store(master_port, Ordering::Relaxed);

                self.events.publish(SentinelEvent::SwitchMaster {
                    master_name: master_name.to_string(),
                    old_ip: our_master_ip,
                    old_port: our_master_port,
                    new_ip: master_ip.to_string(),
                    new_port: master_port,
                });
            }
        }

        // Add or update sentinel
        let sentinel_key = format!("{}:{}", sentinel_ip, sentinel_port);

        // Check for duplicate by runid
        let mut to_remove = Vec::new();
        for entry in master.sentinels.iter() {
            let existing = entry.value();
            let existing_runid = existing.runid.read().clone();

            // Same runid but different address - remove old entry
            if existing_runid == sentinel_runid && entry.key() != &sentinel_key {
                to_remove.push(entry.key().clone());
            }
            // Same address but different runid - update runid
            else if entry.key() == &sentinel_key && existing_runid != sentinel_runid {
                *existing.runid.write() = sentinel_runid.to_string();
                existing
                    .last_hello_time
                    .store(current_time_ms(), Ordering::Relaxed);
            }
        }

        // Remove duplicates
        for key in to_remove {
            if let Some((_, removed)) = master.sentinels.remove(&key) {
                log::info!(
                    "Removed duplicate sentinel {}:{} for master {}",
                    removed.ip,
                    removed.port,
                    master_name
                );

                let master_ip = master.ip.read().clone();
                let master_port = master.port.load(Ordering::Relaxed);
                self.events.publish(SentinelEvent::DupSentinel {
                    master_name: master_name.to_string(),
                    sentinel_ip: removed.ip.clone(),
                    sentinel_port: removed.port,
                    master_ip,
                    master_port,
                });
            }
        }

        // Add new sentinel if not exists
        if !master.sentinels.contains_key(&sentinel_key) {
            let sentinel = Arc::new(SentinelInstance::new(
                Bytes::from(sentinel_runid.to_string()),
                sentinel_ip.to_string(),
                sentinel_port,
            ));
            *sentinel.runid.write() = sentinel_runid.to_string();

            master.sentinels.insert(sentinel_key, sentinel);
            master.update_counts();

            log::info!(
                "Discovered new sentinel {}:{} for master {}",
                sentinel_ip,
                sentinel_port,
                master_name
            );

            let master_ip = master.ip.read().clone();
            let master_port = master.port.load(Ordering::Relaxed);
            self.events.publish(SentinelEvent::Sentinel {
                master_name: master_name.to_string(),
                sentinel_ip: sentinel_ip.to_string(),
                sentinel_port,
                master_ip,
                master_port,
            });
        } else {
            // Update last hello time
            if let Some(sentinel) = master.sentinels.get(&sentinel_key) {
                sentinel
                    .last_hello_time
                    .store(current_time_ms(), Ordering::Relaxed);
            }
        }
    }

    /// Start a subscription handler for hello messages on a master
    pub async fn start_hello_subscription(&self, master: Arc<MasterInstance>) {
        let master_ip = master.ip.read().clone();
        let master_port = master.port.load(Ordering::Relaxed);
        let master_name = master.name.clone();
        let discovery = self.clone_inner();

        tokio::spawn(async move {
            loop {
                if let Err(e) = discovery
                    .subscribe_and_listen(&master_ip, master_port, &master_name)
                    .await
                {
                    log::debug!(
                        "Hello subscription to {}:{} failed: {}",
                        master_ip,
                        master_port,
                        e
                    );
                }
                // Retry after delay
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }

    /// Subscribe to hello channel and listen for messages
    async fn subscribe_and_listen(
        &self,
        ip: &str,
        port: u16,
        master_name: &str,
    ) -> Result<(), std::io::Error> {
        let addr = format!("{}:{}", ip, port);
        let mut stream = TcpStream::connect(&addr).await?;

        // SUBSCRIBE __sentinel__:hello
        let cmd = format!(
            "*2\r\n$9\r\nSUBSCRIBE\r\n${}\r\n{}\r\n",
            SENTINEL_HELLO_CHANNEL.len(),
            SENTINEL_HELLO_CHANNEL
        );
        stream.write_all(cmd.as_bytes()).await?;

        // Read messages in a loop
        let mut buf = [0u8; 4096];
        loop {
            let n = stream.read(&mut buf).await?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "connection closed",
                ));
            }

            // Parse RESP array for message
            // Format: *3\r\n$7\r\nmessage\r\n$<channel_len>\r\n<channel>\r\n$<msg_len>\r\n<message>\r\n
            let data = String::from_utf8_lossy(&buf[..n]);
            if let Some(message) = parse_pubsub_message(&data) {
                self.process_hello(master_name, &message);
            }
        }
    }

    /// Create a clone for spawning tasks
    fn clone_inner(&self) -> Self {
        Self {
            state: self.state.clone(),
            config: self.config.clone(),
            events: self.events.clone(),
        }
    }
}

/// Parse a Pub/Sub message from RESP format
fn parse_pubsub_message(data: &str) -> Option<String> {
    // Simple parser for *3\r\n$7\r\nmessage\r\n...\r\n$<len>\r\n<message>\r\n
    let lines: Vec<&str> = data.split("\r\n").collect();

    // Find "message" and extract the actual message
    for (i, line) in lines.iter().enumerate() {
        if *line == "message" {
            // Message is 4 lines after "message": $<len>, <channel>, $<len>, <message>
            if i + 4 < lines.len() {
                return Some(lines[i + 4].to_string());
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_pubsub_message() {
        let data = "*3\r\n$7\r\nmessage\r\n$20\r\n__sentinel__:hello\r\n$50\r\n127.0.0.1,26379,abc123,1,mymaster,127.0.0.1,6379,0\r\n";
        let msg = parse_pubsub_message(data);
        assert!(msg.is_some());
        assert!(msg.unwrap().contains("127.0.0.1,26379"));
    }

    #[test]
    fn test_hello_message_parsing() {
        let msg = "127.0.0.1,26379,abc123def456,5,mymaster,192.168.1.1,6379,3";
        let parts: Vec<&str> = msg.split(',').collect();

        assert_eq!(parts.len(), 8);
        assert_eq!(parts[0], "127.0.0.1");
        assert_eq!(parts[1], "26379");
        assert_eq!(parts[4], "mymaster");
        assert_eq!(parts[7], "3");
    }
}
