//! Sentinel Networking
//!
//! TCP listener and client handling for Sentinel mode.

use bytes::{Bytes, BytesMut};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use super::commands;
use super::config::SharedSentinelConfig;
use super::election::LeaderElection;
use super::events::SentinelEventPublisher;
use super::failover::SentinelFailover;
use super::state::SentinelState;
use crate::resp::{self, RespValue};

/// Maximum command buffer size
const MAX_BUFFER_SIZE: usize = 65536;

/// Sentinel network server
pub struct SentinelServer {
    state: Arc<SentinelState>,
    config: Arc<SharedSentinelConfig>,
    _events: Arc<SentinelEventPublisher>, // Reserved for future event publishing
    failover: Arc<SentinelFailover>,
    election: Arc<LeaderElection>,
}

impl SentinelServer {
    /// Create a new sentinel server
    pub fn new(
        state: Arc<SentinelState>,
        config: Arc<SharedSentinelConfig>,
        events: Arc<SentinelEventPublisher>,
        failover: Arc<SentinelFailover>,
        election: Arc<LeaderElection>,
    ) -> Self {
        Self {
            state,
            config,
            _events: events,
            failover,
            election,
        }
    }

    /// Start listening for connections
    pub async fn run(&self, listener: TcpListener) -> std::io::Result<()> {
        log::info!("Sentinel listening on {:?}", listener.local_addr()?);

        loop {
            match listener.accept().await {
                Ok((socket, addr)) => {
                    let state = self.state.clone();
                    let config = self.config.clone();
                    let failover = self.failover.clone();
                    let election = self.election.clone();

                    tokio::spawn(async move {
                        if let Err(e) =
                            handle_client(socket, state, config, failover, election).await
                        {
                            log::debug!("Client {} error: {}", addr, e);
                        }
                    });
                }
                Err(e) => {
                    log::error!("Accept error: {}", e);
                }
            }
        }
    }
}

/// Handle a single client connection
async fn handle_client(
    mut socket: TcpStream,
    state: Arc<SentinelState>,
    config: Arc<SharedSentinelConfig>,
    failover: Arc<SentinelFailover>,
    election: Arc<LeaderElection>,
) -> std::io::Result<()> {
    socket.set_nodelay(true)?;

    let mut buffer = BytesMut::with_capacity(4096);

    loop {
        // Read data
        let n = socket.read_buf(&mut buffer).await?;
        if n == 0 {
            return Ok(()); // Client disconnected
        }

        // Check buffer size limit
        if buffer.len() > MAX_BUFFER_SIZE {
            let error = RespValue::Error("ERR max buffer size exceeded".into());
            write_response(&mut socket, &error).await?;
            return Ok(());
        }

        // Try to parse commands
        loop {
            match resp::parse_command(&buffer) {
                Ok((cmd, consumed)) => {
                    let response = process_command(&cmd, &state, &config, &failover, &election);
                    write_response(&mut socket, &response).await?;

                    // Remove processed bytes
                    let _ = buffer.split_to(consumed);
                }
                Err(resp::ParseError::Incomplete) => {
                    // Need more data
                    break;
                }
                Err(e) => {
                    let error = RespValue::Error(format!("ERR {}", e).into());
                    write_response(&mut socket, &error).await?;
                    buffer.clear();
                    break;
                }
            }
        }
    }
}

/// Process a single command
fn process_command(
    args: &[Bytes],
    state: &Arc<SentinelState>,
    config: &Arc<SharedSentinelConfig>,
    failover: &Arc<SentinelFailover>,
    election: &Arc<LeaderElection>,
) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR empty command".into());
    }

    let command = String::from_utf8_lossy(&args[0]).to_uppercase();
    let cmd_args = &args[1..];

    match command.as_str() {
        "PING" => {
            if cmd_args.is_empty() {
                RespValue::SimpleString("PONG".into())
            } else {
                RespValue::BulkString(cmd_args[0].clone())
            }
        }
        "INFO" => handle_info(cmd_args, state, config),
        "SENTINEL" => commands::execute(cmd_args, state, config, Some(failover), Some(election)),
        "SUBSCRIBE" | "PSUBSCRIBE" => handle_subscribe(cmd_args),
        "UNSUBSCRIBE" | "PUNSUBSCRIBE" => handle_unsubscribe(cmd_args),
        "PUBLISH" => handle_publish(cmd_args),
        "CLIENT" => handle_client_cmd(cmd_args),
        "COMMAND" => handle_command_cmd(cmd_args),
        "AUTH" => handle_auth(cmd_args, config),
        "QUIT" => RespValue::SimpleString("OK".into()),
        "DEBUG" => handle_debug(cmd_args),
        "SHUTDOWN" => RespValue::Error("ERR not allowed in Sentinel mode".into()),
        _ => RespValue::Error(format!("ERR unknown command '{}', allowed: PING, INFO, SENTINEL, SUBSCRIBE, PSUBSCRIBE, AUTH, QUIT", command).into()),
    }
}

/// Handle INFO command
fn handle_info(
    args: &[Bytes],
    state: &Arc<SentinelState>,
    config: &Arc<SharedSentinelConfig>,
) -> RespValue {
    let section = if args.is_empty() {
        "default"
    } else {
        &String::from_utf8_lossy(&args[0]).to_lowercase()
    };

    let mut info = String::new();

    // Server section
    if section == "default" || section == "all" || section == "server" {
        info.push_str("# Server\r\n");
        info.push_str("redis_version:7.0.0\r\n");
        info.push_str("redis_mode:sentinel\r\n");
        info.push_str(&format!("sentinel_port:{}\r\n", config.read().port));
        info.push_str(&format!(
            "sentinel_id:{}\r\n",
            String::from_utf8_lossy(&state.myid)
        ));
        info.push_str("\r\n");
    }

    // Sentinel section
    if section == "default" || section == "all" || section == "sentinel" {
        info.push_str("# Sentinel\r\n");
        info.push_str(&format!("sentinel_masters:{}\r\n", state.master_count()));
        info.push_str(&format!(
            "sentinel_tilt:{}\r\n",
            if state.is_tilt() { 1 } else { 0 }
        ));
        info.push_str(&format!(
            "sentinel_tilt_since_seconds:{}\r\n",
            state.tilt_since_seconds()
        ));
        info.push_str(&format!(
            "sentinel_running_scripts:{}\r\n",
            state.running_scripts.load(Ordering::Relaxed)
        ));
        info.push_str(&format!(
            "sentinel_scripts_queue_length:{}\r\n",
            state.scripts_queue_length.load(Ordering::Relaxed)
        ));
        info.push_str(&format!(
            "sentinel_simulate_failure_flags:{}\r\n",
            state.simulate_failure_flags.load(Ordering::Relaxed)
        ));

        // List each master
        for (i, entry) in state.masters.iter().enumerate() {
            let master = entry.value();
            let ip = master.ip.read().clone();
            let port = master.port.load(Ordering::Relaxed);
            let status = if master.flags.is_odown() {
                "odown"
            } else if master.flags.is_sdown() {
                "sdown"
            } else {
                "ok"
            };

            info.push_str(&format!(
                "master{}:name={},status={},address={}:{},slaves={},sentinels={}\r\n",
                i,
                master.name,
                status,
                ip,
                port,
                master.num_slaves.load(Ordering::Relaxed),
                master.num_other_sentinels.load(Ordering::Relaxed) + 1 // +1 includes ourselves
            ));
        }

        info.push_str("\r\n");
    }

    RespValue::BulkString(Bytes::from(info))
}

/// Handle SUBSCRIBE/PSUBSCRIBE
fn handle_subscribe(args: &[Bytes]) -> RespValue {
    // Sentinel supports subscribing to event channels
    // For now, just acknowledge the subscription
    let mut responses = Vec::new();

    for (i, channel) in args.iter().enumerate() {
        responses.push(RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("subscribe")),
            RespValue::BulkString(channel.clone()),
            RespValue::Integer((i + 1) as i64),
        ]));
    }

    if responses.len() == 1 {
        responses.into_iter().next().unwrap()
    } else {
        RespValue::Array(responses)
    }
}

/// Handle UNSUBSCRIBE/PUNSUBSCRIBE
fn handle_unsubscribe(args: &[Bytes]) -> RespValue {
    let mut responses = Vec::new();

    for (_i, channel) in args.iter().enumerate() {
        responses.push(RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("unsubscribe")),
            RespValue::BulkString(channel.clone()),
            RespValue::Integer(0),
        ]));
    }

    if responses.is_empty() {
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("unsubscribe")),
            RespValue::Null,
            RespValue::Integer(0),
        ])
    } else if responses.len() == 1 {
        responses.into_iter().next().unwrap()
    } else {
        RespValue::Array(responses)
    }
}

/// Handle PUBLISH
fn handle_publish(_args: &[Bytes]) -> RespValue {
    // Sentinel doesn't support external PUBLISH
    RespValue::Integer(0)
}

/// Handle CLIENT command
fn handle_client_cmd(args: &[Bytes]) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'client' command".into());
    }

    let subcmd = String::from_utf8_lossy(&args[0]).to_uppercase();

    match subcmd.as_str() {
        "SETNAME" => RespValue::ok(),
        "GETNAME" => RespValue::Null,
        "LIST" => RespValue::BulkString(Bytes::from("")),
        "ID" => RespValue::Integer(1),
        _ => RespValue::Error(format!("ERR Unknown CLIENT subcommand '{}'", subcmd).into()),
    }
}

/// Handle COMMAND command
fn handle_command_cmd(args: &[Bytes]) -> RespValue {
    if args.is_empty() {
        // Return list of commands
        return RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("PING")),
            RespValue::BulkString(Bytes::from("INFO")),
            RespValue::BulkString(Bytes::from("SENTINEL")),
            RespValue::BulkString(Bytes::from("SUBSCRIBE")),
            RespValue::BulkString(Bytes::from("PSUBSCRIBE")),
            RespValue::BulkString(Bytes::from("AUTH")),
            RespValue::BulkString(Bytes::from("QUIT")),
        ]);
    }

    let subcmd = String::from_utf8_lossy(&args[0]).to_uppercase();
    match subcmd.as_str() {
        "COUNT" => RespValue::Integer(7),
        "DOCS" => RespValue::Array(vec![]),
        _ => RespValue::Array(vec![]),
    }
}

/// Handle AUTH command
fn handle_auth(args: &[Bytes], config: &Arc<SharedSentinelConfig>) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'auth' command".into());
    }

    let cfg = config.read();

    // Check if auth is required
    if cfg.requirepass.is_none() && cfg.sentinel_pass.is_none() {
        return RespValue::Error("ERR Client sent AUTH, but no password is set".into());
    }

    let password = String::from_utf8_lossy(&args[0]);

    let expected = cfg.sentinel_pass.as_ref().or(cfg.requirepass.as_ref());

    if let Some(expected_pass) = expected {
        if password == *expected_pass {
            return RespValue::ok();
        }
    }

    RespValue::Error("ERR invalid password".into())
}

/// Handle DEBUG command
fn handle_debug(args: &[Bytes]) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments".into());
    }

    let subcmd = String::from_utf8_lossy(&args[0]).to_uppercase();
    match subcmd.as_str() {
        "SLEEP" if args.len() >= 2 => {
            // Debug sleep (for testing failover)
            if let Ok(secs) = String::from_utf8_lossy(&args[1]).parse::<f64>() {
                std::thread::sleep(std::time::Duration::from_secs_f64(secs));
            }
            RespValue::ok()
        }
        _ => RespValue::Error("ERR Unknown DEBUG subcommand".into()),
    }
}

/// Write a RESP response to the socket
async fn write_response(socket: &mut TcpStream, response: &RespValue) -> std::io::Result<()> {
    let bytes = response.serialize();
    socket.write_all(&bytes).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pubsub::PubSub;

    #[test]
    fn test_process_command_ping() {
        let state = Arc::new(SentinelState::default());
        let config = Arc::new(SharedSentinelConfig::default());
        let pubsub = Arc::new(PubSub::new());
        let events = Arc::new(SentinelEventPublisher::new(pubsub));
        let election = Arc::new(LeaderElection::new(state.clone(), events.clone()));
        let failover = Arc::new(SentinelFailover::new(
            state.clone(),
            config.clone(),
            events.clone(),
            election.clone(),
        ));

        let response = process_command(
            &[Bytes::from("PING")],
            &state,
            &config,
            &failover,
            &election,
        );

        match response {
            RespValue::SimpleString(s) => assert_eq!(s, "PONG"),
            _ => panic!("Expected PONG"),
        }
    }
}
