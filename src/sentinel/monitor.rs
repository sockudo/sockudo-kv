//! Sentinel Monitoring Service
//!
//! Handles PING/PONG health checks including:
//! - PING masters, replicas, and other sentinels every second
//! - INFO polling for state updates
//! - SDOWN detection based on down-after-milliseconds
//! - ODOWN detection via quorum consensus

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{interval, timeout};

use super::config::SharedSentinelConfig;
use super::events::{SentinelEvent, SentinelEventPublisher};
use super::state::{
    MasterInstance, ReplicaInstance, SentinelInstance, SentinelState, current_time_ms,
};
use super::tilt::TiltManager;

/// Interval between PING commands (1 second)
const PING_PERIOD_MS: u64 = 1000;

/// Interval between INFO commands (10 seconds)
const INFO_PERIOD_MS: u64 = 10000;

/// Connection timeout for health checks
const CONNECT_TIMEOUT_MS: u64 = 500;

/// Command timeout
const COMMAND_TIMEOUT_MS: u64 = 500;

/// Sentinel monitor service
pub struct SentinelMonitor {
    state: Arc<SentinelState>,
    _config: Arc<SharedSentinelConfig>, // Reserved for future use (intervals, timeouts, etc.)
    events: Arc<SentinelEventPublisher>,
    tilt: Arc<TiltManager>,
}

impl SentinelMonitor {
    /// Create a new sentinel monitor
    pub fn new(
        state: Arc<SentinelState>,
        config: Arc<SharedSentinelConfig>,
        events: Arc<SentinelEventPublisher>,
        tilt: Arc<TiltManager>,
    ) -> Self {
        Self {
            state,
            _config: config,
            events,
            tilt,
        }
    }

    /// Start the monitoring loop
    pub async fn run(&self) {
        let mut ping_interval = interval(Duration::from_millis(PING_PERIOD_MS));
        let mut info_interval = interval(Duration::from_millis(INFO_PERIOD_MS));

        loop {
            tokio::select! {
                _ = ping_interval.tick() => {
                    // Check TILT mode on every tick
                    self.tilt.check_tilt();

                    // Perform health checks
                    self.ping_all_instances().await;
                    self.check_sdown_instances();
                    self.check_odown_masters();
                }
                _ = info_interval.tick() => {
                    // Refresh INFO from all instances
                    self.refresh_all_info().await;
                }
            }
        }
    }

    /// PING all monitored instances
    async fn ping_all_instances(&self) {
        for entry in self.state.masters.iter() {
            let master = entry.value().clone();

            // Ping master
            self.ping_master(&master).await;

            // Ping replicas
            for replica_entry in master.replicas.iter() {
                let replica = replica_entry.value().clone();
                self.ping_replica(&replica, &master.name).await;
            }

            // Ping other sentinels
            for sentinel_entry in master.sentinels.iter() {
                let sentinel = sentinel_entry.value().clone();
                self.ping_sentinel(&sentinel, &master.name).await;
            }
        }
    }

    /// PING a master instance
    async fn ping_master(&self, master: &Arc<MasterInstance>) {
        let ip = master.ip.read().clone();
        let port = master.port.load(Ordering::Relaxed);

        master
            .last_ping_time
            .store(current_time_ms(), Ordering::Relaxed);

        match self.send_ping(&ip, port).await {
            Ok(reply) => {
                if is_valid_ping_reply(&reply) {
                    let now = current_time_ms();
                    master.last_pong_time.store(now, Ordering::Relaxed);
                    master.last_ok_ping_reply.store(now, Ordering::Relaxed);
                    master.flags.set_disconnected(false);
                }
            }
            Err(_) => {
                master.flags.set_disconnected(true);
            }
        }
    }

    /// PING a replica instance
    async fn ping_replica(&self, replica: &Arc<ReplicaInstance>, _master_name: &str) {
        replica
            .last_ping_time
            .store(current_time_ms(), Ordering::Relaxed);

        match self.send_ping(&replica.ip, replica.port).await {
            Ok(reply) => {
                if is_valid_ping_reply(&reply) {
                    let now = current_time_ms();
                    replica.last_pong_time.store(now, Ordering::Relaxed);
                    replica.flags.set_disconnected(false);
                }
            }
            Err(_) => {
                replica.flags.set_disconnected(true);
            }
        }
    }

    /// PING a sentinel instance
    async fn ping_sentinel(&self, sentinel: &Arc<SentinelInstance>, _master_name: &str) {
        sentinel
            .last_ping_time
            .store(current_time_ms(), Ordering::Relaxed);

        match self.send_ping(&sentinel.ip, sentinel.port).await {
            Ok(reply) => {
                if is_valid_ping_reply(&reply) {
                    let now = current_time_ms();
                    sentinel.last_pong_time.store(now, Ordering::Relaxed);
                    sentinel.flags.set_disconnected(false);
                }
            }
            Err(_) => {
                sentinel.flags.set_disconnected(true);
            }
        }
    }

    /// Send PING command and receive response
    async fn send_ping(&self, ip: &str, port: u16) -> Result<String, std::io::Error> {
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

        // Send PING command
        let ping_cmd = b"*1\r\n$4\r\nPING\r\n";
        stream.write_all(ping_cmd).await?;

        // Read response
        let mut buf = [0u8; 64];
        let read_result = timeout(
            Duration::from_millis(COMMAND_TIMEOUT_MS),
            stream.read(&mut buf),
        )
        .await;

        match read_result {
            Ok(Ok(n)) if n > 0 => Ok(String::from_utf8_lossy(&buf[..n]).to_string()),
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

    /// Check SDOWN status for all instances
    fn check_sdown_instances(&self) {
        // Skip if in TILT mode
        if !self.tilt.actions_allowed() {
            return;
        }

        let now = current_time_ms();

        for entry in self.state.masters.iter() {
            let master = entry.value();
            let down_after = master.down_after_ms();
            let master_ip = master.ip.read().clone();
            let master_port = master.port.load(Ordering::Relaxed);

            // Check master SDOWN
            let last_reply = master.last_ok_ping_reply.load(Ordering::Relaxed);
            let elapsed = now.saturating_sub(last_reply);

            let was_sdown = master.flags.is_sdown();
            let is_sdown = elapsed > down_after;

            if is_sdown && !was_sdown {
                master.flags.set_sdown(true);
                master.sdown_since.store(now, Ordering::Relaxed);
                log::warn!(
                    "Master {} {}:{} is now SDOWN (no reply for {} ms)",
                    master.name,
                    master_ip,
                    master_port,
                    elapsed
                );
                self.events
                    .publish_sdown("master", &master.name, &master_ip, master_port, None);
            } else if !is_sdown && was_sdown {
                master.flags.set_sdown(false);
                log::info!(
                    "Master {} {}:{} is no longer SDOWN",
                    master.name,
                    master_ip,
                    master_port
                );
                self.events.publish_sdown_cleared(
                    "master",
                    &master.name,
                    &master_ip,
                    master_port,
                    None,
                );
            }

            // Check replicas SDOWN
            for replica_entry in master.replicas.iter() {
                let replica = replica_entry.value();
                let last_reply = replica.last_pong_time.load(Ordering::Relaxed);
                let elapsed = now.saturating_sub(last_reply);

                let was_sdown = replica.flags.is_sdown();
                let is_sdown = elapsed > down_after;

                if is_sdown && !was_sdown {
                    replica.flags.set_sdown(true);
                    log::warn!("Replica {}:{} is now SDOWN", replica.ip, replica.port);
                    self.events.publish_sdown(
                        "slave",
                        &replica.key(),
                        &replica.ip,
                        replica.port,
                        Some((&master.name, &master_ip, master_port)),
                    );
                } else if !is_sdown && was_sdown {
                    replica.flags.set_sdown(false);
                    log::info!("Replica {}:{} is no longer SDOWN", replica.ip, replica.port);
                    self.events.publish_sdown_cleared(
                        "slave",
                        &replica.key(),
                        &replica.ip,
                        replica.port,
                        Some((&master.name, &master_ip, master_port)),
                    );
                }
            }

            // Check sentinels SDOWN
            for sentinel_entry in master.sentinels.iter() {
                let sentinel = sentinel_entry.value();
                let last_reply = sentinel.last_pong_time.load(Ordering::Relaxed);
                let elapsed = now.saturating_sub(last_reply);

                let was_sdown = sentinel.flags.is_sdown();
                let is_sdown = elapsed > down_after;

                if is_sdown && !was_sdown {
                    sentinel.flags.set_sdown(true);
                    log::warn!("Sentinel {}:{} is now SDOWN", sentinel.ip, sentinel.port);
                    self.events.publish_sdown(
                        "sentinel",
                        &sentinel.key(),
                        &sentinel.ip,
                        sentinel.port,
                        Some((&master.name, &master_ip, master_port)),
                    );
                } else if !is_sdown && was_sdown {
                    sentinel.flags.set_sdown(false);
                    log::info!(
                        "Sentinel {}:{} is no longer SDOWN",
                        sentinel.ip,
                        sentinel.port
                    );
                    self.events.publish_sdown_cleared(
                        "sentinel",
                        &sentinel.key(),
                        &sentinel.ip,
                        sentinel.port,
                        Some((&master.name, &master_ip, master_port)),
                    );
                }
            }
        }
    }

    /// Check ODOWN status for masters
    fn check_odown_masters(&self) {
        // Skip if in TILT mode
        if !self.tilt.actions_allowed() {
            return;
        }

        let now = current_time_ms();

        for entry in self.state.masters.iter() {
            let master = entry.value();

            // Master must be in SDOWN to be ODOWN
            if !master.flags.is_sdown() {
                if master.flags.is_odown() {
                    master.flags.set_odown(false);
                    let ip = master.ip.read().clone();
                    let port = master.port.load(Ordering::Relaxed);
                    log::info!("Master {} {}:{} is no longer ODOWN", master.name, ip, port);
                    self.events.publish(SentinelEvent::ODownCleared {
                        master_name: master.name.clone(),
                        ip,
                        port,
                    });
                }
                continue;
            }

            // Check if quorum is reached
            let was_odown = master.flags.is_odown();
            let is_odown = master.is_quorum_reached();

            if is_odown && !was_odown {
                master.flags.set_odown(true);
                master.odown_since.store(now, Ordering::Relaxed);
                let ip = master.ip.read().clone();
                let port = master.port.load(Ordering::Relaxed);
                let quorum = master.quorum();
                log::error!(
                    "Master {} {}:{} is now ODOWN (quorum {} reached)",
                    master.name,
                    ip,
                    port,
                    quorum
                );
                self.events.publish_odown(&master.name, &ip, port, quorum);
            }
        }
    }

    /// Refresh INFO from all instances
    async fn refresh_all_info(&self) {
        for entry in self.state.masters.iter() {
            let master = entry.value().clone();

            // Get INFO from master
            if let Ok(info) = self
                .get_info(&master.ip.read(), master.port.load(Ordering::Relaxed))
                .await
            {
                self.process_master_info(&master, &info);
            }

            // Get INFO from replicas
            for replica_entry in master.replicas.iter() {
                let replica = replica_entry.value().clone();
                if let Ok(info) = self.get_info(&replica.ip, replica.port).await {
                    self.process_replica_info(&replica, &info);
                }
            }
        }
    }

    /// Send INFO command and get response
    async fn get_info(&self, ip: &str, port: u16) -> Result<String, std::io::Error> {
        let addr = format!("{}:{}", ip, port);

        let connect_result = timeout(
            Duration::from_millis(CONNECT_TIMEOUT_MS * 2),
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

        // Send INFO command
        let info_cmd = b"*1\r\n$4\r\nINFO\r\n";
        stream.write_all(info_cmd).await?;

        // Read response (INFO can be large)
        let mut response = Vec::with_capacity(4096);
        let mut buf = [0u8; 1024];

        let read_result = timeout(Duration::from_millis(COMMAND_TIMEOUT_MS * 4), async {
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

    /// Process INFO response from a master
    fn process_master_info(&self, master: &Arc<MasterInstance>, info: &str) {
        let now = current_time_ms();
        master.info_refresh.store(now, Ordering::Relaxed);

        // Parse role
        if let Some(role) = parse_info_field(info, "role") {
            *master.role_reported.write() = role.to_string();
            master.role_reported_time.store(now, Ordering::Relaxed);
        }

        // Parse run_id
        if let Some(runid) = parse_info_field(info, "run_id") {
            *master.runid.write() = runid.to_string();
        }

        // Parse connected slaves count
        if let Some(slaves_str) = parse_info_field(info, "connected_slaves") {
            if let Ok(count) = slaves_str.parse::<u32>() {
                master.num_slaves.store(count, Ordering::Relaxed);
            }
        }

        // Parse slave entries (slave0, slave1, etc.)
        // Format: slave0:ip=127.0.0.1,port=6380,state=online,offset=1234,lag=0
        for i in 0..16 {
            let key = format!("slave{}", i);
            if let Some(slave_info) = parse_info_field(info, &key) {
                if let Some((ip, port)) = parse_slave_info(slave_info) {
                    let replica_key = format!("{}:{}", ip, port);
                    if !master.replicas.contains_key(&replica_key) {
                        // Discovered new replica
                        let replica = Arc::new(ReplicaInstance::new(ip.clone(), port));
                        master.replicas.insert(replica_key, replica.clone());
                        master.update_counts();

                        log::info!(
                            "Discovered replica {}:{} for master {}",
                            ip,
                            port,
                            master.name
                        );

                        let master_ip = master.ip.read().clone();
                        let master_port = master.port.load(Ordering::Relaxed);
                        self.events.publish(SentinelEvent::Slave {
                            master_name: master.name.clone(),
                            slave_ip: ip,
                            slave_port: port,
                            master_ip,
                            master_port,
                        });
                    }
                }
            } else {
                break;
            }
        }
    }

    /// Process INFO response from a replica
    fn process_replica_info(&self, replica: &Arc<ReplicaInstance>, info: &str) {
        let now = current_time_ms();
        replica.info_refresh.store(now, Ordering::Relaxed);

        // Parse role
        if let Some(role) = parse_info_field(info, "role") {
            *replica.role_reported.write() = role.to_string();
            replica.role_reported_time.store(now, Ordering::Relaxed);
        }

        // Parse run_id
        if let Some(runid) = parse_info_field(info, "run_id") {
            *replica.runid.write() = runid.to_string();
        }

        // Parse master link status
        if let Some(status) = parse_info_field(info, "master_link_status") {
            replica
                .master_link_status
                .store(status == "up", Ordering::Relaxed);
        }

        // Parse master link down since
        if let Some(down_time) = parse_info_field(info, "master_link_down_since_seconds") {
            if let Ok(secs) = down_time.parse::<u64>() {
                replica
                    .master_link_down_time
                    .store(secs * 1000, Ordering::Relaxed);
            }
        }

        // Parse slave priority
        if let Some(priority) = parse_info_field(info, "slave_priority") {
            if let Ok(p) = priority.parse::<u32>() {
                replica.replica_priority.store(p, Ordering::Relaxed);
            }
        }

        // Parse replication offset
        if let Some(offset) = parse_info_field(info, "slave_repl_offset") {
            if let Ok(o) = offset.parse::<i64>() {
                replica.repl_offset.store(o, Ordering::Relaxed);
            }
        }
    }
}

/// Check if a PING reply is valid
fn is_valid_ping_reply(reply: &str) -> bool {
    // Valid replies: +PONG, -LOADING, -MASTERDOWN
    reply.starts_with("+PONG") || reply.starts_with("-LOADING") || reply.starts_with("-MASTERDOWN")
}

/// Parse a field from INFO output
fn parse_info_field<'a>(info: &'a str, field: &str) -> Option<&'a str> {
    for line in info.lines() {
        if let Some(value) = line.strip_prefix(field) {
            if let Some(value) = value.strip_prefix(':') {
                return Some(value.trim());
            }
        }
    }
    None
}

/// Parse slave info from INFO output
fn parse_slave_info(slave_info: &str) -> Option<(String, u16)> {
    let mut ip = None;
    let mut port = None;

    for part in slave_info.split(',') {
        if let Some(value) = part.strip_prefix("ip=") {
            ip = Some(value.to_string());
        } else if let Some(value) = part.strip_prefix("port=") {
            port = value.parse().ok();
        }
    }

    match (ip, port) {
        (Some(i), Some(p)) => Some((i, p)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_valid_ping_reply() {
        assert!(is_valid_ping_reply("+PONG\r\n"));
        assert!(is_valid_ping_reply("-LOADING\r\n"));
        assert!(is_valid_ping_reply("-MASTERDOWN\r\n"));
        assert!(!is_valid_ping_reply("-ERR unknown\r\n"));
        assert!(!is_valid_ping_reply(""));
    }

    #[test]
    fn test_parse_info_field() {
        let info = "role:master\nconnected_slaves:2\nrun_id:abc123\n";

        assert_eq!(parse_info_field(info, "role"), Some("master"));
        assert_eq!(parse_info_field(info, "connected_slaves"), Some("2"));
        assert_eq!(parse_info_field(info, "unknown"), None);
    }

    #[test]
    fn test_parse_slave_info() {
        let slave = "ip=127.0.0.1,port=6380,state=online,offset=1234";
        let result = parse_slave_info(slave);
        assert_eq!(result, Some(("127.0.0.1".to_string(), 6380)));
    }
}
