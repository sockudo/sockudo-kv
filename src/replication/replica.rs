use bytes::BytesMut;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::rustls::ClientConfig;
use tokio_rustls::{TlsConnector, client::TlsStream};

use super::ReplicationManager;
use super::rdb::load_rdb;
use crate::config::ServerConfig;
use crate::protocol::{Parser, RespValue};
use crate::server_state::ServerState;
use crate::storage::MultiStore;

/// Stream type for replication connection
enum ReplStream {
    Tcp(TcpStream),
    Tls(TlsStream<TcpStream>),
}

impl ReplStream {
    async fn read_buf(&mut self, buf: &mut BytesMut) -> std::io::Result<usize> {
        match self {
            ReplStream::Tcp(s) => s.read_buf(buf).await,
            ReplStream::Tls(s) => s.read_buf(buf).await,
        }
    }

    async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            ReplStream::Tcp(s) => s.write_all(buf).await,
            ReplStream::Tls(s) => s.write_all(buf).await,
        }
    }

    async fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            ReplStream::Tcp(s) => s.read_exact(buf).await,
            ReplStream::Tls(s) => s.read_exact(buf).await,
        }
    }

    fn set_nodelay(&self, nodelay: bool) -> std::io::Result<()> {
        match self {
            ReplStream::Tcp(s) => s.set_nodelay(nodelay),
            ReplStream::Tls(s) => s.get_ref().0.set_nodelay(nodelay),
        }
    }
}

/// Connect to master and start replication
pub async fn connect_to_master(
    repl: Arc<ReplicationManager>,
    multi_store: Arc<MultiStore>,
    host: &str,
    port: u16,
    tls_config: Option<Arc<ClientConfig>>,
    config: Arc<ServerConfig>,
    server_state: Option<Arc<ServerState>>,
) -> Result<(), String> {
    let addr = format!("{}:{}", host, port);

    let tcp_stream = TcpStream::connect(&addr)
        .await
        .map_err(|e| format!("Failed to connect to master: {}", e))?;

    let mut stream = if let Some(config) = tls_config {
        let connector = TlsConnector::from(config);
        let domain = rustls::pki_types::ServerName::try_from(host)
            .map_err(|_| "Invalid DNS name".to_string())?
            .to_owned();
        let tls_stream = connector
            .connect(domain, tcp_stream)
            .await
            .map_err(|e| format!("TLS handshake failed: {}", e))?;
        ReplStream::Tls(tls_stream)
    } else {
        ReplStream::Tcp(tcp_stream)
    };

    // Apply TCP_NODELAY setting (inverse of repl_disable_tcp_nodelay)
    let _ = stream.set_nodelay(!config.repl_disable_tcp_nodelay);

    // Step 1: PING
    send_command(&mut stream, &["PING"]).await?;
    let response = read_response(&mut stream).await?;
    if !matches!(response, RespValue::SimpleString(_)) {
        return Err("Master did not respond to PING".to_string());
    }

    // Step 2: AUTH (if masterauth is configured)
    if let Some(ref password) = config.masterauth {
        let auth_response = if let Some(ref username) = config.masteruser {
            // AUTH username password (Redis 6+ ACL)
            send_command(&mut stream, &["AUTH", username, password]).await?;
            read_response(&mut stream).await?
        } else {
            // AUTH password (legacy)
            send_command(&mut stream, &["AUTH", password]).await?;
            read_response(&mut stream).await?
        };

        // Check auth response
        match &auth_response {
            RespValue::SimpleString(s) if s.as_ref() == b"OK" => {}
            RespValue::Error(e) => {
                return Err(format!(
                    "Master authentication failed: {}",
                    String::from_utf8_lossy(e)
                ));
            }
            _ => {
                return Err("Unexpected AUTH response from master".to_string());
            }
        }
    }

    // Step 3: REPLCONF listening-port (use configured announce port or actual port)
    let listening_port = config
        .replica_announce_port
        .unwrap_or(config.port)
        .to_string();
    send_command(
        &mut stream,
        &["REPLCONF", "listening-port", &listening_port],
    )
    .await?;
    read_response(&mut stream).await?;

    // Step 3b: REPLCONF ip-address (if replica_announce_ip is configured)
    if let Some(ref announce_ip) = config.replica_announce_ip {
        send_command(&mut stream, &["REPLCONF", "ip-address", announce_ip]).await?;
        read_response(&mut stream).await?;
    }

    // Step 4: REPLCONF capa eof capa psync2
    send_command(&mut stream, &["REPLCONF", "capa", "eof", "capa", "psync2"]).await?;
    read_response(&mut stream).await?;

    // Step 4: PSYNC (? -1 for first sync, or repl_id offset for partial)
    let repl_id = if repl.offset() == 0 {
        "?"
    } else {
        &repl.repl_id()
    };
    let offset = if repl.offset() == 0 {
        -1
    } else {
        repl.offset()
    };
    send_command(&mut stream, &["PSYNC", repl_id, &offset.to_string()]).await?;

    // Read PSYNC response
    let psync_response = read_response(&mut stream).await?;
    let response_str = match &psync_response {
        RespValue::SimpleString(s) => String::from_utf8_lossy(s).to_string(),
        _ => return Err("Invalid PSYNC response".to_string()),
    };

    if response_str.starts_with("FULLRESYNC") {
        // Full resync - parse replication ID and offset
        let parts: Vec<&str> = response_str.split_whitespace().collect();
        if parts.len() >= 3 {
            *repl.repl_id.write() = parts[1].to_string();
            if let Ok(new_offset) = parts[2].parse::<i64>() {
                repl.master_repl_offset.store(new_offset, Ordering::Relaxed);
            }
        }

        // Read and load RDB file
        let rdb_data = read_rdb_data(&mut stream).await?;

        // Clear existing data and load RDB
        multi_store.flush_all();
        load_rdb(&rdb_data, &multi_store)?;

        // Cleanup temporary RDB files if configured
        if config.rdb_del_sync_files {
            cleanup_temp_rdb_files();
        }
    } else if response_str.starts_with("CONTINUE") {
        // Partial sync - parse new offset if provided
        let parts: Vec<&str> = response_str.split_whitespace().collect();
        if parts.len() >= 2
            && let Ok(new_offset) = parts[1].parse::<i64>()
        {
            repl.master_repl_offset.store(new_offset, Ordering::Relaxed);
        }
    }

    // Mark as connected
    repl.master_link_status.store(true, Ordering::Relaxed);
    if let Some(ref server) = server_state {
        server.replication_connected.store(true, Ordering::Relaxed);
    }

    // Configurable ACK interval (default: repl_ping_replica_period, typically 10s)
    let ack_period = config.repl_ping_replica_period.max(1);
    let mut ack_interval = tokio::time::interval(tokio::time::Duration::from_secs(ack_period));

    // Replication timeout (default 60s)
    let repl_timeout = if config.repl_timeout > 0 {
        Some(tokio::time::Duration::from_secs(config.repl_timeout))
    } else {
        None
    };
    let mut last_activity = tokio::time::Instant::now();

    // Read and apply commands from master
    let mut buf = BytesMut::with_capacity(64 * 1024);
    let mut current_db_index = 0;

    loop {
        tokio::select! {
            read_result = async {
                if let Some(timeout) = repl_timeout {
                    // Check if we've exceeded timeout since last activity
                    let deadline = last_activity + timeout;
                    match tokio::time::timeout_at(deadline, stream.read_buf(&mut buf)).await {
                        Ok(res) => res,
                        Err(_) => {
                            // Timeout!
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                "Replication timeout"
                            ));
                        }
                    }
                } else {
                    stream.read_buf(&mut buf).await
                }
            } => {
                match read_result {
                    Ok(0) => {
                        // Connection closed
                        repl.master_link_status.store(false, Ordering::Relaxed);
                        if let Some(ref server) = server_state {
                            server.replication_connected.store(false, Ordering::Relaxed);
                        }
                        break;
                    }
                    Ok(_n) => {
                        last_activity = tokio::time::Instant::now();

                        // Parse and apply commands
                        let mut bytes_processed = 0;
                        loop {
                            let buf_snapshot = buf.clone();
                            match Parser::parse(&mut buf) {
                                Ok(Some(value)) => {
                                    let consumed = buf_snapshot.len() - buf.len();
                                    bytes_processed += consumed;

                                    // Apply command
                                    if let Ok(cmd) = crate::protocol::Command::from_resp(value) {
                                        // Skip PING which master sends for keepalive
                                        if !cmd.is_command(b"PING") {
                                            if cmd.is_command(b"SELECT") && cmd.args.len() == 1 {
                                                if let Ok(idx) = std::str::from_utf8(&cmd.args[0]).unwrap_or("0").parse::<usize>()
                                                    && idx < multi_store.db_count() {
                                                        current_db_index = idx;
                                                    }
                                            } else {
                                                let store = multi_store.db(current_db_index);
                                                let exec_result = crate::commands::Dispatcher::execute_basic(&store, cmd);

                                                // Handle propagation error based on config
                                                if matches!(exec_result, crate::protocol::RespValue::Error(_)) {
                                                    match config.propagation_error_behavior.as_str() {
                                                        "panic" => {
                                                            panic!("Replication propagation error");
                                                        }
                                                        "panic-on-replicas" => {
                                                            // We are a replica, so panic
                                                            panic!("Replication propagation error on replica");
                                                        }
                                                        _ => {
                                                            // "ignore" - just continue
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                Ok(None) => break, // Need more data
                                Err(_) => break,
                            }
                        }

                        // Update offset
                        if bytes_processed > 0 {
                            repl.master_repl_offset.fetch_add(bytes_processed as i64, Ordering::Relaxed);
                        }
                    }
                    Err(e) => {
                        repl.master_link_status.store(false, Ordering::Relaxed);
                        if let Some(ref server) = server_state {
                            server.replication_connected.store(false, Ordering::Relaxed);
                        }
                        return Err(format!("Read error: {}", e));
                    }
                }
            }
            _ = ack_interval.tick() => {
                // Send REPLCONF ACK with current offset
                let offset = repl.offset();
                if send_command(&mut stream, &["REPLCONF", "ACK", &offset.to_string()]).await.is_err() {
                    // Ignore ACK errors, master may not respond
                }
            }
        }
    }

    Ok(())
}

/// Send RESP command to stream
async fn send_command(stream: &mut ReplStream, parts: &[&str]) -> Result<(), String> {
    let mut buf = Vec::with_capacity(256);
    buf.push(b'*');
    buf.extend_from_slice(itoa::Buffer::new().format(parts.len()).as_bytes());
    buf.extend_from_slice(b"\r\n");

    for part in parts {
        buf.push(b'$');
        buf.extend_from_slice(itoa::Buffer::new().format(part.len()).as_bytes());
        buf.extend_from_slice(b"\r\n");
        buf.extend_from_slice(part.as_bytes());
        buf.extend_from_slice(b"\r\n");
    }

    stream.write_all(&buf).await.map_err(|e| e.to_string())
}

/// Read RESP response from stream
async fn read_response(stream: &mut ReplStream) -> Result<RespValue, String> {
    let mut buf = BytesMut::with_capacity(4096);

    loop {
        let n = stream.read_buf(&mut buf).await.map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("Connection closed".to_string());
        }

        match Parser::parse(&mut buf) {
            Ok(Some(value)) => return Ok(value),
            Ok(None) => continue,
            Err(e) => return Err(e.to_string()),
        }
    }
}

/// Read RDB data from stream (bulk string format: $<length>\r\n<data>)
async fn read_rdb_data(stream: &mut ReplStream) -> Result<Vec<u8>, String> {
    let mut header = Vec::new();
    let mut byte = [0u8; 1];

    // Read $ prefix
    stream
        .read_exact(&mut byte)
        .await
        .map_err(|e| e.to_string())?;

    // Handle EOF marker for empty RDB
    if byte[0] == b'$' {
        // Standard bulk string format
    } else {
        return Err(format!("Expected $ for RDB, got {:?}", byte[0]));
    }

    // Read length until \r\n
    loop {
        stream
            .read_exact(&mut byte)
            .await
            .map_err(|e| e.to_string())?;
        if byte[0] == b'\r' {
            stream
                .read_exact(&mut byte)
                .await
                .map_err(|e| e.to_string())?; // \n
            break;
        }
        header.push(byte[0]);
    }

    // Parse length (may be -1 for empty)
    let length_str = String::from_utf8_lossy(&header);
    if length_str == "-1" {
        return Ok(Vec::new());
    }

    let length: usize = length_str
        .parse()
        .map_err(|_| format!("Invalid RDB length: {}", length_str))?;

    // Read RDB data
    let mut rdb_data = vec![0u8; length];
    stream
        .read_exact(&mut rdb_data)
        .await
        .map_err(|e| e.to_string())?;

    Ok(rdb_data)
}

/// Disconnect from master (called on REPLICAOF NO ONE)
pub fn disconnect_from_master(repl: &ReplicationManager) {
    repl.master_link_status.store(false, Ordering::Relaxed);
    *repl.master_host.write() = None;
    *repl.master_port.write() = None;
}

/// Cleanup temporary RDB files used for synchronization
fn cleanup_temp_rdb_files() {
    // Best effort cleanup without glob dependency
    match std::fs::read_dir(".") {
        Ok(entries) => {
            for entry in entries.flatten() {
                if let Ok(file_type) = entry.file_type() {
                    if file_type.is_file() {
                        if let Some(name) = entry.file_name().to_str() {
                            if name.starts_with("temp-") && name.ends_with(".rdb") {
                                if let Err(e) = std::fs::remove_file(entry.path()) {
                                    eprintln!("Failed to remove temp RDB file {:?}: {}", name, e);
                                } else {
                                    println!("Removed temp RDB file: {}", name);
                                }
                            }
                        }
                    }
                }
            }
        }
        Err(e) => {
            eprintln!("Failed to read directory for cleanup: {}", e);
        }
    }
}
