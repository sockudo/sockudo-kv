//! Cluster Service - Dragonfly-style high performance
//!
//! Optimizations:
//! 1. Batch gossip slot updates (no per-slot locks)
//! 2. O(1) node lookup using IP:Port index
//! 3. Pre-allocated buffers
//! 4. Connection pooling (TODO: persistent connections)
//!
//! Handles:
//! 1. Cluster Bus Server (accepting connections from other nodes)
//! 2. Gossip Loop (sending PINGs)
//! 3. Message Processing

use bytes::{Bytes, BytesMut};
use log::{debug, error, info, warn};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

use tokio_rustls::rustls::ClientConfig;
use tokio_rustls::{
    TlsAcceptor, TlsConnector, client::TlsStream as ClientTlsStream,
    server::TlsStream as ServerTlsStream,
};

use crate::cluster::message::{
    CLUSTERMSG_TYPE_MEET, CLUSTERMSG_TYPE_PING, CLUSTERMSG_TYPE_PONG, ClusterMsg,
};
use crate::cluster_state::{ClusterLink, LinkDirection};
use crate::server_state::ServerState;

/// Stream type for cluster connection
enum ClusterStream {
    Tcp(TcpStream),
    ServerTls(ServerTlsStream<TcpStream>),
    ClientTls(ClientTlsStream<TcpStream>),
}

impl ClusterStream {
    async fn read_buf(&mut self, buf: &mut BytesMut) -> std::io::Result<usize> {
        match self {
            ClusterStream::Tcp(s) => s.read_buf(buf).await,
            ClusterStream::ServerTls(s) => s.read_buf(buf).await,
            ClusterStream::ClientTls(s) => s.read_buf(buf).await,
        }
    }

    async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            ClusterStream::Tcp(s) => s.write_all(buf).await,
            ClusterStream::ServerTls(s) => s.write_all(buf).await,
            ClusterStream::ClientTls(s) => s.write_all(buf).await,
        }
    }
}

/// Cluster Service
pub struct ClusterService {
    server: Arc<ServerState>,
    shutdown: Arc<Notify>,
    tls_acceptor: Option<TlsAcceptor>,
    tls_client_config: Option<Arc<ClientConfig>>,
}

impl ClusterService {
    pub fn new(
        server: Arc<ServerState>,
        tls_acceptor: Option<TlsAcceptor>,
        tls_client_config: Option<Arc<ClientConfig>>,
    ) -> Self {
        Self {
            server,
            shutdown: Arc::new(Notify::new()),
            tls_acceptor,
            tls_client_config,
        }
    }

    /// Start the cluster service
    pub async fn start(self: Arc<Self>) {
        let (enabled, port, cluster_port_cfg, node_timeout) = {
            let config = self.server.config.read();
            (
                config.cluster_enabled,
                config.port,
                config.cluster_port,
                config.cluster_node_timeout,
            )
        };

        if !enabled {
            return;
        }

        // Use configured cluster_port if set, otherwise port + 10000
        let cluster_port = if cluster_port_cfg > 0 {
            cluster_port_cfg
        } else {
            port + 10000
        };

        let bind_addr = format!("0.0.0.0:{}", cluster_port);

        info!("Starting Cluster Bus on {}", bind_addr);

        let listener = match TcpListener::bind(&bind_addr).await {
            Ok(l) => l,
            Err(e) => {
                error!("Failed to bind Cluster Bus: {}", e);
                return;
            }
        };

        // Spawn connection acceptor
        let service = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    res = listener.accept() => {
                        match res {
                            Ok((stream, addr)) => {
                                let service = service.clone();
                                tokio::spawn(async move {
                                    service.handle_connection(stream, addr).await;
                                });
                            }
                            Err(e) => {
                                error!("Cluster accept error: {}", e);
                                tokio::time::sleep(Duration::from_millis(100)).await;
                            }
                        }
                    }
                    _ = service.shutdown.notified() => {
                        break;
                    }
                }
            }
        });

        // Spawn gossip loop
        let service = self.clone();
        tokio::spawn(async move {
            service.gossip_loop().await;
        });

        // Spawn failure detection loop
        let service = self.clone();
        let timeout_ms = node_timeout;
        tokio::spawn(async move {
            service.failure_detection_loop(timeout_ms).await;
        });
    }

    /// Handle incoming cluster bus connection
    async fn handle_connection(&self, stream: TcpStream, addr: SocketAddr) {
        let mut stream = if let Some(acceptor) = &self.tls_acceptor {
            match acceptor.accept(stream).await {
                Ok(tls_stream) => ClusterStream::ServerTls(tls_stream),
                Err(e) => {
                    warn!("Cluster TLS handshake failed from {}: {}", addr, e);
                    return;
                }
            }
        } else {
            ClusterStream::Tcp(stream)
        };

        // Pre-allocate buffer
        let mut buf = BytesMut::with_capacity(4096);
        let peer_ip = addr.ip().to_string();

        // Track link (use a distinct ID for temporary connection until identified?)
        // Actually, we identify by IP:Port usually.
        let link_id = Bytes::from(format!("{}:{}", peer_ip, addr.port()));
        self.server.cluster.links.insert(
            link_id.clone(),
            ClusterLink {
                direction: LinkDirection::From,
                node_id: Bytes::new(), // Updated when we identify the node
                create_time: crate::storage::now_ms() as u64,
                events: "r".to_string(),
                send_buffer_allocated: 4096,
                send_buffer_used: 0,
            },
        );

        loop {
            match stream.read_buf(&mut buf).await {
                Ok(0) => break, // EOF
                Ok(_) => {
                    if buf.len() < 8 {
                        continue;
                    }

                    // Check signature
                    if &buf[0..4] != b"RCmb" {
                        warn!("Invalid cluster msg signature from {}", addr);
                        break;
                    }

                    self.process_packet(&mut buf, &mut stream, &peer_ip).await;
                    buf.clear();
                }
                Err(e) => {
                    warn!("Cluster read error from {}: {}", addr, e);
                    break;
                }
            }
        }

        // Remove link on disconnect
        self.server.cluster.links.remove(&link_id);
    }

    async fn process_packet(&self, buf: &mut BytesMut, stream: &mut ClusterStream, peer_ip: &str) {
        if buf.len() < 2128 {
            // Minimum: header + slots bitmap
            return;
        }

        self.server
            .cluster
            .messages_received
            .fetch_add(1, Ordering::Relaxed);

        let bytes = &buf[..];

        // Parse header fields
        let type_u16 = u16::from_be_bytes([bytes[12], bytes[13]]);
        let declared_msg_port = u16::from_be_bytes([bytes[10], bytes[11]]);
        let declared_cport = if bytes.len() > 162 {
            u16::from_be_bytes([bytes[160], bytes[161]])
        } else {
            declared_msg_port + 10000
        };

        // Extract sender ID (40 bytes at offset 40)
        let sender = Bytes::copy_from_slice(&bytes[40..80]);

        // Extract slots bitmap (2048 bytes at offset 80)
        let slots_bitmap = &bytes[80..2128];

        // Handle node identification using O(1) IP:Port index
        if sender != self.server.cluster.my_id {
            self.identify_node(&sender, peer_ip, declared_msg_port, declared_cport);
        }

        // Batch update slots from gossip (optimized - no per-slot locks)
        self.update_node_from_packet(&sender, slots_bitmap);

        // Handle message type
        match type_u16 {
            CLUSTERMSG_TYPE_PING | CLUSTERMSG_TYPE_MEET => {
                debug!(
                    "Received PING/MEET from {}",
                    String::from_utf8_lossy(&sender)
                );

                // Update link info
                if let Some(mut link) = self
                    .server
                    .cluster
                    .links
                    .get_mut(&Bytes::from(format!("{}:{}", peer_ip, declared_cport)))
                {
                    link.node_id = sender.clone();
                }

                // Reply with PONG
                let pong = self.build_pong();
                let mut out = BytesMut::with_capacity(3000);
                pong.serialize(&mut out);
                if let Err(e) = stream.write_all(&out).await {
                    warn!("Failed to send PONG: {}", e);
                } else {
                    self.server
                        .cluster
                        .messages_sent
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
            CLUSTERMSG_TYPE_PONG => {
                debug!("Received PONG from {}", String::from_utf8_lossy(&sender));
            }
            _ => {
                debug!(
                    "Received unknown message type {} from {}",
                    type_u16,
                    String::from_utf8_lossy(&sender)
                );
            }
        }
    }

    /// Identify or add a node using O(1) IP:Port index
    fn identify_node(&self, sender: &Bytes, peer_ip: &str, msg_port: u16, cport: u16) {
        // Check if we know this sender ID already
        if self.server.cluster.get_node(sender).is_some() {
            return; // Already known
        }

        // Try to find by IP:Port using O(1) index
        if let Some(old_id) = self.server.cluster.find_node_by_ip_port(peer_ip, msg_port) {
            if old_id != *sender {
                info!(
                    "Handshake: Renaming node {} -> {}",
                    String::from_utf8_lossy(&old_id),
                    String::from_utf8_lossy(sender)
                );
                self.server.cluster.rename_node(&old_id, sender);
            }
            return;
        }

        // Try cport
        if let Some(old_id) = self.server.cluster.find_node_by_ip_port(peer_ip, cport) {
            if old_id != *sender {
                info!(
                    "Handshake (cport): Renaming node {} -> {}",
                    String::from_utf8_lossy(&old_id),
                    String::from_utf8_lossy(sender)
                );
                self.server.cluster.rename_node(&old_id, sender);
            }
            return;
        }

        // Unknown node - add it
        info!(
            "Adding new node from gossip: {} at {}:{}",
            String::from_utf8_lossy(sender),
            peer_ip,
            msg_port
        );
        self.server.cluster.add_node_from_gossip(
            sender.clone(),
            peer_ip.to_string(),
            msg_port,
            cport,
        );
    }

    /// Update node from gossip packet using batch API
    fn update_node_from_packet(&self, sender: &Bytes, slots_bitmap: &[u8]) {
        if sender == &self.server.cluster.my_id {
            return;
        }

        // Update node timestamps
        if let Some(node) = self.server.cluster.get_node(sender) {
            node.pong_recv
                .store(crate::storage::now_ms() as u64, Ordering::Relaxed);
            node.link_state.store(1, Ordering::Relaxed);
        }

        // Use batch update API - no per-slot locks, single write lock acquisition
        self.server
            .cluster
            .batch_update_slots_from_gossip(sender, slots_bitmap);
    }

    /// Gossip loop - sends PINGs to random nodes
    async fn gossip_loop(&self) {
        let interval = Duration::from_millis(100); // 10 Hz

        // Pre-allocate reusable buffer
        let mut ping_buf = BytesMut::with_capacity(3000);
        let mut resp_buf = BytesMut::with_capacity(4096);

        loop {
            tokio::time::sleep(interval).await;

            // Get node count first to avoid allocation if empty
            let node_count = self.server.cluster.nodes.len();
            if node_count == 0 {
                continue;
            }

            // Pick a random node efficiently
            let target_idx = fastrand::usize(0..node_count);
            let target = self.server.cluster.nodes.iter().nth(target_idx);

            if let Some(entry) = target {
                let node = entry.value().clone();
                drop(entry); // Release DashMap lock early

                self.send_ping(&node, &mut ping_buf, &mut resp_buf).await;
            }
        }
    }

    /// Send PING to a node with reusable buffers
    async fn send_ping(
        &self,
        target: &crate::cluster_state::ClusterNode,
        ping_buf: &mut BytesMut,
        resp_buf: &mut BytesMut,
    ) {
        let addr = format!("{}:{}", target.ip, target.cport);

        let tcp_stream = match TcpStream::connect(&addr).await {
            Ok(s) => s,
            Err(e) => {
                debug!("Failed to connect to {}: {}", addr, e);
                // Mark node as potentially failing
                target.link_state.store(0, Ordering::Relaxed);
                return;
            }
        };

        let mut stream = if let Some(config) = &self.tls_client_config {
            let connector = TlsConnector::from(config.clone());
            let domain = match rustls::pki_types::ServerName::try_from(target.ip.as_str()) {
                Ok(n) => n.to_owned(),
                Err(_) => {
                    // Fallback/DNS issue? If IP is used as name it might fail depending on cert
                    // For now assume valid or use a dummy if we don't verify hostname strictly?
                    // Rustls usually requires valid DNS name.
                    // If we are connecting by IP, we might need a workaround or expect cert to have IP SAN.
                    // Let's assume valid.
                    // If invalid, log and fail.
                    warn!("Invalid DNS name for node {}", target.ip);
                    return;
                }
            };

            match connector.connect(domain, tcp_stream).await {
                Ok(tls_stream) => ClusterStream::ClientTls(tls_stream),
                Err(e) => {
                    warn!("Cluster TLS handshake to {} failed: {}", addr, e);
                    return;
                }
            }
        } else {
            ClusterStream::Tcp(tcp_stream)
        };

        // ... usage of stream ...
        // Build and send PING
        ping_buf.clear();
        let ping = self.build_ping();
        ping.serialize(ping_buf);

        if let Err(e) = stream.write_all(ping_buf).await {
            warn!("Failed to send PING to {}: {}", addr, e);
            return;
        }

        self.server
            .cluster
            .messages_sent
            .fetch_add(1, Ordering::Relaxed);

        // Track outgoing link
        let link_id = Bytes::from(format!("out:{}", addr));
        self.server.cluster.links.insert(
            link_id.clone(),
            ClusterLink {
                direction: LinkDirection::To,
                node_id: target.id.clone(),
                create_time: crate::storage::now_ms() as u64,
                events: "w".to_string(),
                send_buffer_allocated: ping_buf.capacity() as u64,
                send_buffer_used: ping_buf.len() as u64,
            },
        );

        // Wait for PONG with timeout
        resp_buf.clear();
        match tokio::time::timeout(Duration::from_millis(500), stream.read_buf(resp_buf)).await {
            Ok(Ok(n)) if n > 0 => {
                // Process PONG
                let peer_ip = target.ip.clone();
                self.process_packet(resp_buf, &mut stream, &peer_ip).await;
            }
            Ok(Ok(_)) => {
                // EOF
                debug!("Connection closed by {}", addr);
            }
            Ok(Err(e)) => {
                warn!("Read error from {}: {}", addr, e);
            }
            Err(_) => {
                debug!("Timeout waiting for PONG from {}", addr);
            }
        }

        // Remove link
        self.server.cluster.links.remove(&link_id);
    }

    fn build_ping(&self) -> ClusterMsg {
        self.build_msg(CLUSTERMSG_TYPE_PING)
    }

    fn build_pong(&self) -> ClusterMsg {
        self.build_msg(CLUSTERMSG_TYPE_PONG)
    }

    fn build_msg(&self, type_: u16) -> ClusterMsg {
        let mut msg = ClusterMsg::new(type_);
        msg.sender = self.server.cluster.my_id.clone();

        let config = self.server.config.read();

        // Use announced port if configured, otherwise actual port
        let actual_port = config.port;
        msg.port = self
            .server
            .cluster
            .get_announced_port(config.cluster_announce_port, actual_port);

        // Use announced bus port if configured
        msg.cport = self.server.cluster.get_announced_bus_port(
            config.cluster_announce_bus_port,
            config.cluster_port,
            actual_port,
        );

        // Use announced IP if configured
        let announced_ip = self
            .server
            .cluster
            .get_announced_ip(&config.cluster_announce_ip);
        let ip_bytes = announced_ip.as_bytes();
        let len = ip_bytes.len().min(46);
        msg.myip[..len].copy_from_slice(&ip_bytes[..len]);

        drop(config);

        // Serialize slots bitmap efficiently
        self.server.cluster.slots.to_bytes(&mut msg.myslots);

        msg.config_epoch = self.server.cluster.config_epoch.load(Ordering::Relaxed);
        msg.current_epoch = self.server.cluster.current_epoch.load(Ordering::Relaxed);

        msg
    }

    /// Failure detection loop - checks node timeouts and upgrades PFAIL to FAIL
    async fn failure_detection_loop(&self, timeout_ms: u64) {
        // Run at cluster_node_timeout / 10 interval (100ms min)
        let interval_ms = (timeout_ms / 10).max(100);
        let interval = Duration::from_millis(interval_ms);

        loop {
            tokio::time::sleep(interval).await;

            // Run timeout checks on all nodes
            let pfail_count = self.server.cluster.run_timeout_checks(timeout_ms);
            if pfail_count > 0 {
                debug!("Marked {} nodes as PFAIL due to timeout", pfail_count);
            }

            // Check PFAIL -> FAIL upgrades based on quorum
            for node_entry in self.server.cluster.nodes.iter() {
                let node = node_entry.value();
                if node.flags.pfail() && !node.flags.fail() {
                    crate::cluster::failover::check_pfail_to_fail(&self.server.cluster, node);
                }
            }

            // Update cluster state (ok/down) based on coverage
            let require_full_coverage = self.server.config.read().cluster_require_full_coverage;
            self.server
                .cluster
                .update_cluster_state(require_full_coverage);
        }
    }
}
