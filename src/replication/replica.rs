//! Replica-side replication handling
//!
//! Handles connecting to master and receiving replication stream.

use bytes::BytesMut;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::ReplicationManager;
use super::rdb::load_rdb;
use crate::protocol::{Parser, RespValue};
use crate::storage::MultiStore;

/// Connect to master and start replication
pub async fn connect_to_master(
    repl: Arc<ReplicationManager>,
    multi_store: Arc<MultiStore>,
    host: String,
    port: u16,
) -> Result<(), String> {
    let addr = format!("{}:{}", host, port);

    let mut stream = TcpStream::connect(&addr)
        .await
        .map_err(|e| format!("Failed to connect to master: {}", e))?;

    // Step 1: PING
    send_command(&mut stream, &["PING"]).await?;
    let response = read_response(&mut stream).await?;
    if !matches!(response, RespValue::SimpleString(_)) {
        return Err("Master did not respond to PING".to_string());
    }

    // Step 2: REPLCONF listening-port
    send_command(&mut stream, &["REPLCONF", "listening-port", "6379"]).await?;
    read_response(&mut stream).await?;

    // Step 3: REPLCONF capa eof capa psync2
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
    } else if response_str.starts_with("CONTINUE") {
        // Partial sync - parse new offset if provided
        let parts: Vec<&str> = response_str.split_whitespace().collect();
        if parts.len() >= 2
            && let Ok(new_offset) = parts[1].parse::<i64>() {
                repl.master_repl_offset.store(new_offset, Ordering::Relaxed);
            }
    }

    // Mark as connected
    repl.master_link_status.store(true, Ordering::Relaxed);

    // Spawn periodic REPLCONF ACK
    let repl_clone = repl.clone();
    let mut ack_interval = tokio::time::interval(tokio::time::Duration::from_secs(1));

    // Read and apply commands from master
    let mut buf = BytesMut::with_capacity(64 * 1024);
    let mut current_db_index = 0;

    loop {
        tokio::select! {
            read_result = stream.read_buf(&mut buf) => {
                match read_result {
                    Ok(0) => {
                        // Connection closed
                        repl.master_link_status.store(false, Ordering::Relaxed);
                        break;
                    }
                    Ok(_n) => {
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
                                                let _ = crate::commands::Dispatcher::execute_basic(&store, cmd);
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
                        return Err(format!("Read error: {}", e));
                    }
                }
            }
            _ = ack_interval.tick() => {
                // Send REPLCONF ACK with current offset
                let offset = repl_clone.offset();
                if let Err(_) = send_command(&mut stream, &["REPLCONF", "ACK", &offset.to_string()]).await {
                    // Ignore ACK errors, master may not respond
                }
            }
        }
    }

    Ok(())
}

/// Send RESP command to stream
async fn send_command(stream: &mut TcpStream, parts: &[&str]) -> Result<(), String> {
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
async fn read_response(stream: &mut TcpStream) -> Result<RespValue, String> {
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
async fn read_rdb_data(stream: &mut TcpStream) -> Result<Vec<u8>, String> {
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
