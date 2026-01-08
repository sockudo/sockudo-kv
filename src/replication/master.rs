//! Master-side replication handling
//!
//! Handles replica connections, PSYNC, and command streaming.

use bytes::Bytes;
use std::sync::Arc;

use super::{ConnectedReplica, ReplicationManager};
use crate::protocol::RespValue;

/// Handle PSYNC command from replica
pub fn handle_psync(
    repl: &Arc<ReplicationManager>,
    repl_id: &[u8],
    offset: &[u8],
) -> PsyncResponse {
    let requested_id = String::from_utf8_lossy(repl_id);
    let requested_offset: i64 = std::str::from_utf8(offset)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(-1);

    let current_id = repl.repl_id();
    let current_offset = repl.offset();

    // Check if partial sync is possible
    if requested_id == current_id || requested_id == "?" {
        if requested_offset == -1 {
            // Full resync requested
            PsyncResponse::FullResync {
                repl_id: current_id,
                offset: current_offset,
            }
        } else if let Some(commands) = repl.backlog.get_from_offset(requested_offset) {
            // Partial sync possible
            PsyncResponse::Continue {
                commands,
                new_offset: current_offset,
            }
        } else {
            // Offset too old, need full resync
            PsyncResponse::FullResync {
                repl_id: current_id,
                offset: current_offset,
            }
        }
    } else {
        // Different replication ID, need full resync
        PsyncResponse::FullResync {
            repl_id: current_id,
            offset: current_offset,
        }
    }
}

/// Response to PSYNC command
pub enum PsyncResponse {
    /// Full resync needed - send FULLRESYNC and RDB
    FullResync { repl_id: String, offset: i64 },
    /// Partial sync possible - send CONTINUE and buffered commands
    Continue {
        commands: Vec<Bytes>,
        new_offset: i64,
    },
}

impl PsyncResponse {
    /// Convert to RESP format
    pub fn to_resp(&self) -> RespValue {
        match self {
            PsyncResponse::FullResync { repl_id, offset } => {
                RespValue::SimpleString(Bytes::from(format!("FULLRESYNC {} {}", repl_id, offset)))
            }
            PsyncResponse::Continue { new_offset, .. } => {
                RespValue::SimpleString(Bytes::from(format!("CONTINUE {}", new_offset)))
            }
        }
    }
}

/// Handle REPLCONF command from replica
pub fn handle_replconf(
    replica: &Arc<ConnectedReplica>,
    args: &[Bytes],
) -> Result<RespValue, String> {
    if args.is_empty() {
        return Err("ERR wrong number of arguments".to_string());
    }

    let mut i = 0;
    while i < args.len() {
        let opt = args[i].to_ascii_uppercase();
        match opt.as_slice() {
            b"LISTENING-PORT" => {
                // Replica's listening port - we don't need to track this
                i += 2;
            }
            b"CAPA" => {
                // Capability negotiation
                if i + 1 < args.len() {
                    let cap = String::from_utf8_lossy(&args[i + 1]).to_string();
                    replica.capabilities.write().push(cap);
                }
                i += 2;
            }
            b"ACK" => {
                // Replica offset acknowledgment
                if i + 1 < args.len()
                    && let Ok(offset) = std::str::from_utf8(&args[i + 1])
                        .ok()
                        .and_then(|s| s.parse::<i64>().ok())
                        .ok_or("invalid offset")
                    {
                        replica
                            .ack_offset
                            .store(offset, std::sync::atomic::Ordering::Relaxed);
                    }
                i += 2;
            }
            b"GETACK" => {
                // Master requesting ACK - return our offset
                // This is handled differently, replica should send REPLCONF ACK
                i += 2;
            }
            _ => {
                i += 2; // Skip unknown options
            }
        }
    }

    Ok(RespValue::ok())
}

/// Generate minimal empty RDB for initial sync
pub fn generate_empty_rdb() -> Bytes {
    // Minimal valid RDB file
    let mut rdb = Vec::new();

    // Magic number "REDIS"
    rdb.extend_from_slice(b"REDIS");
    // RDB version (0011 = 11)
    rdb.extend_from_slice(b"0011");

    // AUX field: redis-ver
    rdb.push(0xFA); // AUX
    rdb.push(0x09); // Length of "redis-ver"
    rdb.extend_from_slice(b"redis-ver");
    rdb.push(0x05); // Length of "7.0.0"
    rdb.extend_from_slice(b"7.0.0");

    // AUX field: redis-bits
    rdb.push(0xFA);
    rdb.push(0x0A);
    rdb.extend_from_slice(b"redis-bits");
    rdb.push(0xC0); // Special encoding for integer
    rdb.push(0x40); // 64

    // Select DB 0
    rdb.push(0xFE); // SELECTDB
    rdb.push(0x00); // DB 0

    // Resize DB (empty)
    rdb.push(0xFB); // RESIZEDB
    rdb.push(0x00); // Hash table size = 0
    rdb.push(0x00); // Expire hash table size = 0

    // End of file
    rdb.push(0xFF);

    // CRC64 checksum (8 bytes of zeros for now)
    rdb.extend_from_slice(&[0u8; 8]);

    Bytes::from(rdb)
}
