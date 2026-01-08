use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::task::JoinSet;

use sockudo_kv::PubSub;
use sockudo_kv::client::ClientState;
use sockudo_kv::client_manager::ClientManager;
use sockudo_kv::cluster_state::ClusterState;
use sockudo_kv::commands::Dispatcher;
use sockudo_kv::commands::cluster as cluster_cmd;
use sockudo_kv::commands::connection::{self, ConnectionResult};
use sockudo_kv::commands::pubsub::{
    execute as pubsub_execute, execute_subscribe, is_allowed_in_pubsub_mode, is_subscribe_command,
};
use sockudo_kv::commands::transaction::{
    self, TransactionResult, handle_multi_queue, is_transaction_command,
};
use sockudo_kv::config::ServerConfig;
use sockudo_kv::protocol::{Command, Parser, RespValue};
use sockudo_kv::pubsub::PubSubMessage;
use sockudo_kv::server_state::ServerState;
use std::path::Path;

const READ_BUF_SIZE: usize = 64 * 1024;
const WRITE_BUF_SIZE: usize = 64 * 1024;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let config = sockudo_kv::cli::Cli::load_config().unwrap_or_else(|e| {
        eprintln!("Error loading config: {}", e);
        std::process::exit(1);
    });

    if config.daemonize {
        println!(
            "WARNING: daemonize yes is specified but not fully supported in this build. Running in foreground."
        );
    }

    if !config.pidfile.is_empty() {
        if let Err(e) = std::fs::write(&config.pidfile, std::process::id().to_string()) {
            eprintln!("Failed to write pidfile {}: {}", config.pidfile, e);
        }
    }

    println!(
        "Starting sockudo-kv with config: port={}, bound to {:?}",
        config.port, config.bind
    );

    // Bind to addresses
    let mut listeners = Vec::new();
    for bind_addr in &config.bind {
        let (addr_str, lenient) = if let Some(stripped) = bind_addr.strip_prefix('-') {
            (stripped, true)
        } else {
            (bind_addr.as_str(), false)
        };

        if addr_str == "*" {
            // Bind to all interfaces?
            // Redis treats * as "all interfaces" but TcpListener::bind("0.0.0.0") does that.
            // If user puts *, we probably want 0.0.0.0 (IPv4) or :: (IPv6) depending on system?
            // Actually * usually implies INADDR_ANY.
            let addr = format!("0.0.0.0:{}", config.port);
            match TcpListener::bind(&addr).await {
                Ok(l) => {
                    println!("Listening on {}", addr);
                    listeners.push(l);
                }
                Err(e) => {
                    // Try IPv6
                    let addr_v6 = format!(":::{}", config.port);
                    match TcpListener::bind(&addr_v6).await {
                        Ok(l) => {
                            println!("Listening on {}", addr_v6);
                            listeners.push(l);
                        }
                        Err(e2) => {
                            if !lenient {
                                eprintln!("Failed to bind to * (0.0.0.0 or ::): {} / {}", e, e2);
                                std::process::exit(1);
                            } else {
                                eprintln!("Warning: Failed to bind to *: {}", e);
                            }
                        }
                    }
                }
            }
            continue;
        }

        let addr = format!("{}:{}", addr_str, config.port);
        match TcpListener::bind(&addr).await {
            Ok(l) => {
                println!("Listening on {}", addr);
                listeners.push(l);
            }
            Err(e) => {
                if !lenient {
                    eprintln!("Failed to bind to {}: {}", addr, e);
                    std::process::exit(1);
                } else {
                    println!(
                        "Warning: Could not bind to {}, ignoring (- prefix used). Error: {}",
                        addr, e
                    );
                }
            }
        }
    }

    if listeners.is_empty() {
        eprintln!("Could not bind to any address.");
        std::process::exit(1);
    }

    // Global state - MultiStore with configured databases
    let multi_store = Arc::new(sockudo_kv::storage::MultiStore::with_capacity_and_count(
        100_000,
        config.databases,
    ));
    let pubsub = Arc::new(PubSub::new());
    let clients = if let Some(pass) = &config.requirepass {
        Arc::new(ClientManager::with_password(bytes::Bytes::from(
            pass.clone(),
        )))
    } else {
        Arc::new(ClientManager::new())
    };
    let server_state = Arc::new(ServerState::new());
    let cluster_state = Arc::new(ClusterState::new());
    let replication = Arc::new(sockudo_kv::ReplicationManager::new());

    // Set cluster port from config
    cluster_state
        .my_port
        .store(config.port as u64, std::sync::atomic::Ordering::Relaxed);

    // Apply configurations to server state
    server_state.set_maxmemory(config.maxmemory);
    server_state.set_maxmemory_policy(&format!("{:?}", config.maxmemory_policy));
    if let Some(pass) = &config.requirepass {
        server_state.set_default_user_password(pass);
    }

    // In a real implementation we would apply more config flags here,
    // e.g. persistence settings to replication manager, etc.

    // Load RDB if exists
    let rdb_path = Path::new(&config.dir).join(&config.dbfilename);
    if rdb_path.exists() {
        println!("Loading RDB from {:?}", rdb_path);
        match std::fs::read(&rdb_path) {
            Ok(data) => match sockudo_kv::replication::rdb::load_rdb(&data, &multi_store) {
                Ok(_) => println!("RDB loaded successfully"),
                Err(e) => eprintln!("Failed to load RDB: {}", e),
            },
            Err(e) => eprintln!("Failed to read RDB file: {}", e),
        }
    }

    // Handle replicaof
    if let Some((master_host, master_port)) = &config.replicaof {
        println!("Replicating from {}:{}", master_host, master_port);
        replication.set_replica_of(Some(master_host.clone()), Some(*master_port));

        let repl_clone = replication.clone();
        let store_clone = multi_store.clone();
        let host = master_host.clone();
        let port = *master_port;

        tokio::spawn(async move {
            loop {
                println!("Connecting to master {}:{}", host, port);
                if let Err(e) = sockudo_kv::replication::replica::connect_to_master(
                    repl_clone.clone(),
                    store_clone.clone(),
                    host.clone(),
                    port,
                )
                .await
                {
                    eprintln!("Replication error: {}", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            }
        });
    }

    let config = Arc::new(config);

    // Spawn listener tasks
    let mut tasks = JoinSet::new();

    for listener in listeners {
        let multi_store = Arc::clone(&multi_store);
        let pubsub = Arc::clone(&pubsub);
        let clients = Arc::clone(&clients);
        let server_state = Arc::clone(&server_state);
        let cluster_state = Arc::clone(&cluster_state);
        let replication = Arc::clone(&replication);
        let config = Arc::clone(&config);

        tasks.spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((socket, addr)) => {
                        let multi_store = Arc::clone(&multi_store);
                        let pubsub = Arc::clone(&pubsub);
                        let clients = Arc::clone(&clients);
                        let server_state = Arc::clone(&server_state);
                        let cluster_state = Arc::clone(&cluster_state);
                        let replication = Arc::clone(&replication);
                        let config = Arc::clone(&config);

                        tokio::spawn(async move {
                            let _ = handle_client(
                                socket,
                                addr,
                                multi_store,
                                pubsub,
                                clients,
                                server_state,
                                cluster_state,
                                replication,
                                config,
                            )
                            .await;
                        });
                    }
                    Err(e) => {
                        eprintln!("Accept error: {}", e);
                        // Backoff slightly to avoid spinning if there's a permanent error?
                        // If it's pure logic, maybe not.
                    }
                }
            }
        });
    }

    // Wait for all listeners (they shouldn't exit)
    // Also handle shutdown signal?
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
             println!("Shutdown signal received.");
        }
        _ = async { while tasks.join_next().await.is_some() {} } => {
             println!("All listeners stopped unexpectedly.");
        }
    }

    Ok(())
}

#[inline(always)]
async fn handle_client(
    mut socket: TcpStream,
    addr: std::net::SocketAddr,
    multi_store: Arc<sockudo_kv::storage::MultiStore>,
    pubsub: Arc<PubSub>,
    clients: Arc<ClientManager>,
    server_state: Arc<ServerState>,
    cluster_state: Arc<ClusterState>,
    replication: Arc<sockudo_kv::ReplicationManager>,
    config: Arc<ServerConfig>,
) -> std::io::Result<()> {
    socket.set_nodelay(true)?;

    // Register this client with PubSub
    let (sub_id, rx) = pubsub.register();

    // Register with client manager
    let require_auth = config.requirepass.is_some();
    let client = clients.register_with_auth(addr, sub_id, require_auth);

    let result = handle_client_inner(
        &mut socket,
        &multi_store,
        &pubsub,
        &clients,
        &client,
        &server_state,
        &cluster_state,
        &replication,
        &config,
        sub_id,
        rx,
    )
    .await;

    // Clean up
    pubsub.unregister(sub_id);
    clients.unregister(client.id);

    result
}

async fn handle_client_inner(
    socket: &mut TcpStream,
    multi_store: &Arc<sockudo_kv::storage::MultiStore>,
    pubsub: &Arc<PubSub>,
    clients: &Arc<ClientManager>,
    client: &Arc<ClientState>,
    server_state: &Arc<ServerState>,
    cluster_state: &Arc<ClusterState>,
    _replication: &Arc<sockudo_kv::ReplicationManager>,
    config: &Arc<ServerConfig>,
    sub_id: u64,
    mut rx: broadcast::Receiver<PubSubMessage>,
) -> std::io::Result<()> {
    let mut buf = BytesMut::with_capacity(READ_BUF_SIZE);
    let mut write_buf = Vec::with_capacity(WRITE_BUF_SIZE);
    let mut in_pubsub_mode = false;

    loop {
        tokio::select! {
            biased;

            // Handle incoming commands
            read_result = socket.read_buf(&mut buf) => {
                if read_result? == 0 {
                    return Ok(());
                }

                // Process ALL commands in buffer before writing
                loop {
                    match Parser::parse(&mut buf) {
                        Ok(Some(value)) => match Command::from_resp(value) {
                            Ok(cmd) => {
                                // Update last command timestamp
                                client.touch();

                                let cmd_name = cmd.name();

                                // Check authentication
                                if clients.requires_auth() && !client.is_authenticated() {
                                    let is_allowed = cmd.is_command(b"AUTH")
                                        || cmd.is_command(b"HELLO")
                                        || cmd.is_command(b"QUIT");

                                    if !is_allowed {
                                        write_buf.extend_from_slice(b"-NOAUTH Authentication required.\r\n");
                                        // continue to next command in buffer (but we are in a match, so we need to just stop processing this cmd)
                                        // Since we write invalid response, we must skip the rest of logic for this command.
                                        continue;
                                    }
                                }

                                // Check if we need to handle Pub/Sub commands
                                if is_subscribe_command(cmd_name) {
                                    // Handle subscription commands
                                    match execute_subscribe(pubsub, sub_id, cmd_name, &cmd.args) {
                                        Ok(responses) => {
                                            for response in responses {
                                                response.write_to(&mut write_buf);
                                            }
                                            in_pubsub_mode = pubsub.is_subscribed(sub_id);
                                            client.in_pubsub.store(in_pubsub_mode, std::sync::atomic::Ordering::Relaxed);
                                        }
                                        Err(e) => {
                                            write_buf.extend_from_slice(b"-");
                                            write_buf.extend_from_slice(e.to_string().as_bytes());
                                            write_buf.extend_from_slice(b"\r\n");
                                        }
                                    }
                                } else if cmd.is_command(b"PUBLISH") || cmd.is_command(b"PUBSUB") || cmd.is_command(b"SPUBLISH") {
                                    // Handle PUBLISH and PUBSUB commands
                                    match pubsub_execute(pubsub, cmd_name, &cmd.args) {
                                        Ok(response) => {
                                            response.write_to(&mut write_buf);
                                        }
                                        Err(e) => {
                                            write_buf.extend_from_slice(b"-");
                                            write_buf.extend_from_slice(e.to_string().as_bytes());
                                            write_buf.extend_from_slice(b"\r\n");
                                        }
                                    }
                                } else if in_pubsub_mode && !is_allowed_in_pubsub_mode(cmd_name) {
                                    // Error: only (P)SUBSCRIBE/(P)UNSUBSCRIBE/PING/QUIT allowed
                                    write_buf.extend_from_slice(
                                        b"-ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context\r\n"
                                    );
                                } else if is_connection_command(cmd_name) {
                                    // Handle connection commands
                                    match connection::execute(client, clients, cmd_name, &cmd.args) {
                                        Ok(ConnectionResult::Response(response)) => {
                                            if client.should_reply() {
                                                response.write_to(&mut write_buf);
                                            }
                                        }
                                        Ok(ConnectionResult::Quit(response)) => {
                                            response.write_to(&mut write_buf);
                                            socket.write_all(&write_buf).await?;
                                            return Ok(());
                                        }
                                        Ok(ConnectionResult::NoReply) => {
                                            // Don't write anything
                                        }
                                        Err(e) => {
                                            write_buf.extend_from_slice(b"-");
                                            write_buf.extend_from_slice(e.to_string().as_bytes());
                                            write_buf.extend_from_slice(b"\r\n");
                                        }
                                    }
                                } else if let Some(queued_response) = handle_multi_queue(client, cmd_name, &cmd.args) {
                                    // Command was queued in MULTI mode
                                    if client.should_reply() {
                                        queued_response.write_to(&mut write_buf);
                                    }
                                } else if is_transaction_command(cmd_name) {
                                    // Handle transaction commands (MULTI/EXEC/DISCARD/WATCH/UNWATCH)
                                    let store = multi_store.db(client.current_db());
                                    match transaction::execute(client, &store, cmd_name, &cmd.args) {
                                        Ok(TransactionResult::Response(response)) => {
                                            if client.should_reply() {
                                                response.write_to(&mut write_buf);
                                            }
                                        }
                                        Ok(TransactionResult::Queued) => {
                                            if client.should_reply() {
                                                write_buf.extend_from_slice(b"+QUEUED\r\n");
                                            }
                                        }
                                        Err(e) => {
                                            write_buf.extend_from_slice(b"-");
                                            write_buf.extend_from_slice(e.to_string().as_bytes());
                                            write_buf.extend_from_slice(b"\r\n");
                                        }
                                    }
                                } else {
                                    // Normal command processing
                                    if cmd.is_command(b"SWAPDB") {
                                        // SWAPDB needs access to MultiStore
                                        let response = handle_swapdb(multi_store, &cmd.args);
                                        if client.should_reply() {
                                            response.write_to(&mut write_buf);
                                        }
                                    } else if cmd.is_command(b"SAVE") {
                                        // Synchronous SAVE
                                        use sockudo_kv::replication::rdb::generate_rdb;
                                        let rdb_data = generate_rdb(multi_store);
                                        let rdb_path = Path::new(&config.dir).join(&config.dbfilename);
                                        match std::fs::write(&rdb_path, &rdb_data) {
                                            Ok(_) => {
                                                server_state.last_save_time.store(server_state.now_unix(), std::sync::atomic::Ordering::Relaxed);
                                                write_buf.extend_from_slice(b"+OK\r\n");
                                            }
                                            Err(e) => {
                                                write_buf.extend_from_slice(b"-ERR Failed to save: ");
                                                write_buf.extend_from_slice(e.to_string().as_bytes());
                                                write_buf.extend_from_slice(b"\r\n");
                                            }
                                        }
                                    } else if cmd.is_command(b"BGSAVE") {
                                        use sockudo_kv::replication::rdb::generate_rdb;
                                        let rdb_data = generate_rdb(multi_store);
                                        let rdb_path = Path::new(&config.dir).join(&config.dbfilename);
                                        match std::fs::write(&rdb_path, &rdb_data) {
                                            Ok(_) => {
                                                server_state.last_save_time.store(server_state.now_unix(), std::sync::atomic::Ordering::Relaxed);
                                                write_buf.extend_from_slice(b"+Background saving started\r\n");
                                            }
                                            Err(e) => {
                                                write_buf.extend_from_slice(b"-ERR Failed to save: ");
                                                write_buf.extend_from_slice(e.to_string().as_bytes());
                                                write_buf.extend_from_slice(b"\r\n");
                                            }
                                        }
                                    } else if cmd.is_command(b"SELECT") {
                                        // SELECT needs to validate against db count
                                        let response = handle_select(client, multi_store.db_count(), &cmd.args);
                                        if client.should_reply() {
                                            response.write_to(&mut write_buf);
                                        }
                                    } else if is_cluster_command(cmd_name) {
                                        // Handle cluster commands (CLUSTER, ASKING, READONLY, READWRITE)
                                        let store = multi_store.db(client.current_db());
                                        match cluster_cmd::execute(cluster_state, client, &store, cmd_name, &cmd.args) {
                                            Ok(response) => {
                                                if client.should_reply() {
                                                    response.write_to(&mut write_buf);
                                                }
                                            }
                                            Err(e) => {
                                                write_buf.extend_from_slice(b"-");
                                                write_buf.extend_from_slice(e.to_string().as_bytes());
                                                write_buf.extend_from_slice(b"\r\n");
                                            }
                                        }
                                    } else {
                                        let store = multi_store.db(client.current_db());
                                        let response = Dispatcher::execute(&store, server_state, cmd);
                                        if client.should_reply() {
                                            response.write_to(&mut write_buf);
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                write_buf.extend_from_slice(b"-");
                                write_buf.extend_from_slice(e.to_string().as_bytes());
                                write_buf.extend_from_slice(b"\r\n");
                            }
                        },
                        Ok(None) => break, // Need more data
                        Err(e) => {
                            write_buf.extend_from_slice(b"-");
                            write_buf.extend_from_slice(e.to_string().as_bytes());
                            write_buf.extend_from_slice(b"\r\n");
                            socket.write_all(&write_buf).await?;
                            return Ok(());
                        }
                    }
                }

                // Write responses
                if !write_buf.is_empty() {
                    socket.write_all(&write_buf).await?;
                    if write_buf.capacity() > WRITE_BUF_SIZE * 4 {
                        write_buf = Vec::with_capacity(WRITE_BUF_SIZE);
                    } else {
                        write_buf.clear();
                    }
                }
            }

            // Handle pub/sub messages (only when subscribed)
            msg_result = rx.recv(), if in_pubsub_mode => {
                match msg_result {
                    Ok(msg) => {
                        // Convert message to RESP and write
                        let response = pubsub_message_to_resp(msg);
                        response.write_to(&mut write_buf);
                        socket.write_all(&write_buf).await?;
                        write_buf.clear();
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        // Client is too slow, some messages were dropped
                        eprintln!("Client {} lagged, dropped {} messages", sub_id, n);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        return Ok(());
                    }
                }
            }
        }
    }
}

/// Check if command is a connection command
#[inline]
fn is_connection_command(cmd: &[u8]) -> bool {
    cmd.eq_ignore_ascii_case(b"PING")
        || cmd.eq_ignore_ascii_case(b"ECHO")
        || cmd.eq_ignore_ascii_case(b"QUIT")
        || cmd.eq_ignore_ascii_case(b"RESET")
        || cmd.eq_ignore_ascii_case(b"SELECT")
        || cmd.eq_ignore_ascii_case(b"AUTH")
        || cmd.eq_ignore_ascii_case(b"CLIENT")
        || cmd.eq_ignore_ascii_case(b"HELLO")
}

/// Check if command is a cluster command
#[inline]
fn is_cluster_command(cmd: &[u8]) -> bool {
    cmd.eq_ignore_ascii_case(b"CLUSTER")
        || cmd.eq_ignore_ascii_case(b"ASKING")
        || cmd.eq_ignore_ascii_case(b"READONLY")
        || cmd.eq_ignore_ascii_case(b"READWRITE")
}

/// Convert PubSubMessage to RESP format
fn pubsub_message_to_resp(msg: PubSubMessage) -> sockudo_kv::protocol::RespValue {
    use sockudo_kv::protocol::RespValue;

    match msg {
        PubSubMessage::Message { channel, message } => RespValue::array(vec![
            RespValue::bulk_string("message"),
            RespValue::bulk(channel),
            RespValue::bulk(message),
        ]),
        PubSubMessage::SMessage { channel, message } => RespValue::array(vec![
            RespValue::bulk_string("smessage"),
            RespValue::bulk(channel),
            RespValue::bulk(message),
        ]),
        PubSubMessage::PMessage {
            pattern,
            channel,
            message,
        } => RespValue::array(vec![
            RespValue::bulk_string("pmessage"),
            RespValue::bulk(pattern),
            RespValue::bulk(channel),
            RespValue::bulk(message),
        ]),
        PubSubMessage::Subscribe { channel, count } => RespValue::array(vec![
            RespValue::bulk_string("subscribe"),
            RespValue::bulk(channel),
            RespValue::integer(count as i64),
        ]),
        PubSubMessage::Unsubscribe { channel, count } => RespValue::array(vec![
            RespValue::bulk_string("unsubscribe"),
            RespValue::bulk(channel),
            RespValue::integer(count as i64),
        ]),
        PubSubMessage::PSubscribe { pattern, count } => RespValue::array(vec![
            RespValue::bulk_string("psubscribe"),
            RespValue::bulk(pattern),
            RespValue::integer(count as i64),
        ]),
        PubSubMessage::PUnsubscribe { pattern, count } => RespValue::array(vec![
            RespValue::bulk_string("punsubscribe"),
            RespValue::bulk(pattern),
            RespValue::integer(count as i64),
        ]),
        PubSubMessage::SSubscribe { channel, count } => RespValue::array(vec![
            RespValue::bulk_string("ssubscribe"),
            RespValue::bulk(channel),
            RespValue::integer(count as i64),
        ]),
        PubSubMessage::SUnsubscribe { channel, count } => RespValue::array(vec![
            RespValue::bulk_string("sunsubscribe"),
            RespValue::bulk(channel),
            RespValue::integer(count as i64),
        ]),
    }
}

/// Handle SWAPDB command - O(1) database swap
fn handle_swapdb(
    multi_store: &Arc<sockudo_kv::storage::MultiStore>,
    args: &[bytes::Bytes],
) -> RespValue {
    if args.len() != 2 {
        return RespValue::error("ERR wrong number of arguments for 'swapdb' command");
    }

    let db1 = match std::str::from_utf8(&args[0])
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
    {
        Some(v) => v,
        None => return RespValue::error("ERR invalid DB index"),
    };

    let db2 = match std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
    {
        Some(v) => v,
        None => return RespValue::error("ERR invalid DB index"),
    };

    if multi_store.swap_db(db1, db2) {
        RespValue::ok()
    } else {
        RespValue::error("ERR invalid DB index")
    }
}

/// Handle SELECT command - switch database with validation
fn handle_select(client: &Arc<ClientState>, db_count: usize, args: &[bytes::Bytes]) -> RespValue {
    if args.is_empty() {
        return RespValue::error("ERR wrong number of arguments for 'select' command");
    }

    let db_index = match std::str::from_utf8(&args[0])
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
    {
        Some(v) => v,
        None => return RespValue::error("ERR invalid DB index"),
    };

    if db_index >= db_count {
        return RespValue::error("ERR DB index is out of range");
    }

    client.select_db(db_index as u64);
    RespValue::ok()
}
