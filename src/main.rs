#![allow(clippy::too_many_arguments)] // Handler functions need many parameters

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use mimalloc::MiMalloc;

use socket2::{Domain, Protocol, Socket, Type};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::task::JoinSet;

#[cfg(unix)]
use tokio::net::UnixListener;

use sockudo_kv::PubSub;
use sockudo_kv::client::ClientState;
use sockudo_kv::client_manager::ClientManager;
use sockudo_kv::cluster::ClusterService;
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

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const READ_BUF_SIZE: usize = 64 * 1024;
const WRITE_BUF_SIZE: usize = 64 * 1024;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = sockudo_kv::cli::Cli::load_config().unwrap_or_else(|e| {
        eprintln!("Error loading config: {}", e);
        std::process::exit(1);
    });

    // Initialize logging from config (loglevel, logfile, syslog)
    if let Err(e) = sockudo_kv::logging::init_logging(&config) {
        eprintln!("Warning: Failed to initialize logging: {}", e);
        // Fall back to env_logger
        env_logger::init();
    }

    // Apply supervised mode (systemd/upstart notification)
    let supervised_mode: sockudo_kv::process_mgmt::SupervisedMode = config
        .supervised
        .parse()
        .unwrap_or(sockudo_kv::process_mgmt::SupervisedMode::No);
    supervised_mode.apply();

    // Set up crash handler if crash_log_enabled
    if config.crash_log_enabled {
        let memcheck = config.crash_memcheck_enabled;
        std::panic::set_hook(Box::new(move |panic_info| {
            eprintln!("\n=== SOCKUDO-KV CRASH REPORT ===");
            eprintln!("Time: {:?}", std::time::SystemTime::now());
            eprintln!("Panic: {}", panic_info);
            if let Some(location) = panic_info.location() {
                eprintln!(
                    "Location: {}:{}:{}",
                    location.file(),
                    location.line(),
                    location.column()
                );
            }
            // Backtrace
            eprintln!("\nBacktrace:");
            eprintln!("{:?}", std::backtrace::Backtrace::force_capture());

            if memcheck {
                eprintln!("\n--- Memory Statistics ---");
                // Try to get basic memory stats
                #[cfg(target_os = "linux")]
                if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
                    for line in status.lines() {
                        if line.starts_with("VmRSS")
                            || line.starts_with("VmPeak")
                            || line.starts_with("VmSize")
                            || line.starts_with("VmData")
                        {
                            eprintln!("  {}", line);
                        }
                    }
                }
                #[cfg(not(target_os = "linux"))]
                {
                    eprintln!("  (Memory stats only available on Linux)");
                }
            }
            eprintln!("=== END CRASH REPORT ===\n");
        }));
    }

    // Set process title if enabled
    if config.set_proc_title {
        let config_path = std::env::args().nth(1).unwrap_or_default();
        let title = sockudo_kv::process_mgmt::format_process_title(
            &config.proc_title_template,
            config.port,
            &config_path,
        );
        sockudo_kv::process_mgmt::set_process_title(&title);
    }

    // Show ASCII logo if always_show_logo is enabled
    if config.always_show_logo {
        println!(
            r#"
  _____         _           _         _  ____   __
 / ____|       | |         | |       | |/ /\ \ / /
| (___   ___  _| | ___   __| | ___   | ' /  \ V / 
 \___ \ / _ \/ __|| |/ / | | |/ _ \  |  <    > <  
 ____) | (_)|  (__| <| |_| | (_) |  | . \  / . \ 
|_____/ \___/\___|_|\_\__,_|\___/   |_|\_\/_/ \_\

sockudo-kv version 7.0.0 - Ready to accept connections
"#
        );
    }

    if config.daemonize {
        println!(
            "WARNING: daemonize yes is specified but not fully supported in this build. Running in foreground."
        );
    }

    if !config.pidfile.is_empty()
        && let Err(e) = std::fs::write(&config.pidfile, std::process::id().to_string())
    {
        eprintln!("Failed to write pidfile {}: {}", config.pidfile, e);
    }

    println!(
        "Starting sockudo-kv with config: port={}, bound to {:?}",
        config.port, config.bind
    );

    // Bind TCP listeners using socket2 for proper configuration
    let mut listeners = Vec::new();
    let tcp_backlog = config.tcp_backlog as i32;
    #[cfg(target_os = "linux")]
    let socket_mark = config.socket_mark_id;

    for bind_addr in &config.bind {
        let (addr_str, lenient) = if let Some(stripped) = bind_addr.strip_prefix('-') {
            (stripped, true)
        } else {
            (bind_addr.as_str(), false)
        };

        let addrs_to_try: Vec<String> = if addr_str == "*" {
            vec![
                format!("0.0.0.0:{}", config.port),
                format!("[::]:{}", config.port),
            ]
        } else {
            vec![format!("{}:{}", addr_str, config.port)]
        };

        let mut bound = false;
        for addr in addrs_to_try {
            match create_tcp_listener(
                &addr,
                tcp_backlog,
                #[cfg(target_os = "linux")]
                socket_mark,
            ) {
                Ok(listener) => {
                    println!("Listening on {}", addr);
                    listeners.push(listener);
                    bound = true;
                    break;
                }
                Err(e) => {
                    if !lenient {
                        eprintln!("Failed to bind to {}: {}", addr, e);
                    }
                }
            }
        }

        if !bound && !lenient && addr_str != "*" {
            eprintln!("Failed to bind to {}", addr_str);
            std::process::exit(1);
        }
    }

    if listeners.is_empty() {
        eprintln!("Could not bind to any address.");
        std::process::exit(1);
    }

    // Unix socket listener (cross-platform with cfg)
    #[cfg(unix)]
    let _unix_listener = if let Some(ref path) = config.unixsocket {
        match create_unix_listener(path, config.unixsocketperm) {
            Ok(listener) => {
                println!("Listening on Unix socket: {}", path);
                Some(listener)
            }
            Err(e) => {
                eprintln!("Failed to create Unix socket {}: {}", path, e);
                None
            }
        }
    } else {
        None
    };

    #[cfg(not(unix))]
    let _unix_listener: Option<()> = None;
    #[cfg(not(unix))]
    if config.unixsocket.is_some() {
        eprintln!("Warning: Unix sockets are not supported on this platform");
    }

    // TLS listener setup
    let tls_acceptor = if config.tls_port > 0 {
        match (&config.tls_cert_file, &config.tls_key_file) {
            (Some(cert_file), Some(key_file)) => {
                match sockudo_kv::tls::load_tls_config_with_options(
                    &sockudo_kv::tls::TlsServerOptions {
                        cert_path: cert_file,
                        key_path: key_file,
                        key_password: config.tls_key_file_pass.as_deref(),
                        ca_cert_file: config.tls_ca_cert_file.as_deref(),
                        ca_cert_dir: config.tls_ca_cert_dir.as_deref(),
                        auth_clients: &config.tls_auth_clients,
                        session_cache_size: if config.tls_session_caching {
                            config.tls_session_cache_size
                        } else {
                            0
                        },
                        session_cache_timeout: config.tls_session_cache_timeout,
                        prefer_server_ciphers: config.tls_prefer_server_ciphers,
                        protocols: config.tls_protocols.as_deref(),
                        ciphers: config.tls_ciphers.as_deref(),
                        ciphersuites: config.tls_ciphersuites.as_deref(),
                    },
                ) {
                    Ok(tls_config) => {
                        let acceptor = tokio_rustls::TlsAcceptor::from(tls_config);
                        // Create TLS listener
                        match create_tcp_listener(
                            &format!("0.0.0.0:{}", config.tls_port),
                            tcp_backlog,
                            #[cfg(target_os = "linux")]
                            socket_mark,
                        ) {
                            Ok(listener) => {
                                println!("TLS listening on 0.0.0.0:{}", config.tls_port);
                                Some((acceptor, listener))
                            }
                            Err(e) => {
                                eprintln!("Failed to bind TLS listener: {}", e);
                                None
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to load TLS config: {}", e);
                        None
                    }
                }
            }
            _ => {
                eprintln!("TLS port configured but no certificate/key files specified");
                None
            }
        }
    } else {
        None
    };

    // Suppress unused warning if TLS is not used yet
    let _ = &tls_acceptor;

    // Global state - MultiStore with configured databases
    // Note: Use with_db_count to avoid pre-allocating DashMap capacity
    let multi_store = Arc::new(sockudo_kv::storage::MultiStore::with_db_count(
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
    let replication = Arc::new(sockudo_kv::ReplicationManager::new());

    // Set cluster port from config
    server_state
        .cluster
        .my_port
        .store(config.port as u64, std::sync::atomic::Ordering::Relaxed);

    // Apply configurations to server state
    {
        let mut cfg = server_state.config.write();
        *cfg = config.clone();
    }

    server_state.set_maxmemory(config.maxmemory);
    server_state.set_maxmemory_policy(&format!("{:?}", config.maxmemory_policy));
    if let Some(pass) = &config.requirepass {
        server_state.set_default_user_password(pass);
    }

    // Apply slow log configuration
    server_state.slowlog_threshold_us.store(
        config.slowlog_log_slower_than as u64,
        std::sync::atomic::Ordering::Relaxed,
    );
    server_state.slowlog_max_len.store(
        config.slowlog_max_len as usize,
        std::sync::atomic::Ordering::Relaxed,
    );

    // Apply latency monitor configuration
    server_state.latency_threshold_ms.store(
        config.latency_monitor_threshold,
        std::sync::atomic::Ordering::Relaxed,
    );

    // Apply ACL log configuration
    server_state
        .acl_log_max_len
        .store(config.acllog_max_len, std::sync::atomic::Ordering::Relaxed);

    // Apply database count
    server_state
        .databases
        .store(config.databases, std::sync::atomic::Ordering::Relaxed);

    // Apply AOF enabled flag
    server_state
        .aof_enabled
        .store(config.appendonly, std::sync::atomic::Ordering::Relaxed);

    // Apply maxclients limit
    clients.set_maxclients(config.maxclients);

    // Initialize and start Cluster Service
    let (cluster_tls_acceptor, cluster_tls_client_config) = if config.tls_cluster {
        let acceptor = match (&config.tls_cert_file, &config.tls_key_file) {
            (Some(cert), Some(key)) => {
                match sockudo_kv::tls::load_tls_config_with_options(
                    &sockudo_kv::tls::TlsServerOptions {
                        cert_path: cert,
                        key_path: key,
                        key_password: config.tls_key_file_pass.as_deref(),
                        ca_cert_file: config.tls_ca_cert_file.as_deref(),
                        ca_cert_dir: config.tls_ca_cert_dir.as_deref(),
                        auth_clients: &config.tls_auth_clients,
                        session_cache_size: if config.tls_session_caching {
                            config.tls_session_cache_size
                        } else {
                            0
                        },
                        session_cache_timeout: config.tls_session_cache_timeout,
                        prefer_server_ciphers: config.tls_prefer_server_ciphers,
                        protocols: config.tls_protocols.as_deref(),
                        ciphers: config.tls_ciphers.as_deref(),
                        ciphersuites: config.tls_ciphersuites.as_deref(),
                    },
                ) {
                    Ok(cfg) => Some(tokio_rustls::TlsAcceptor::from(cfg)),
                    Err(e) => {
                        eprintln!("Failed to load TLS server config for cluster: {}", e);
                        None
                    }
                }
            }
            _ => None,
        };

        let client_config = match sockudo_kv::tls::load_client_tls_config_with_options(
            &sockudo_kv::tls::TlsClientOptions {
                ca_cert_file: config.tls_ca_cert_file.as_deref(),
                ca_cert_dir: config.tls_ca_cert_dir.as_deref(),
                client_cert_file: config
                    .tls_client_cert_file
                    .as_deref()
                    .or(config.tls_cert_file.as_deref()),
                client_key_file: config
                    .tls_client_key_file
                    .as_deref()
                    .or(config.tls_key_file.as_deref()),
                client_key_password: config.tls_client_key_file_pass.as_deref(),
                protocols: config.tls_protocols.as_deref(),
                ciphers: config.tls_ciphers.as_deref(),
                ciphersuites: config.tls_ciphersuites.as_deref(),
            },
        ) {
            Ok(cfg) => Some(cfg),
            Err(e) => {
                eprintln!("Failed to load TLS client config for cluster: {}", e);
                None
            }
        };

        (acceptor, client_config)
    } else {
        (None, None)
    };

    let cluster_service = Arc::new(ClusterService::new(
        server_state.clone(),
        cluster_tls_acceptor,
        cluster_tls_client_config,
    ));
    let cs = cluster_service.clone();
    tokio::spawn(async move {
        cs.start().await;
    });

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

        let tls_config = if config.tls_replication {
            match sockudo_kv::tls::load_client_tls_config(
                config.tls_ca_cert_file.as_deref(),
                config.tls_cert_file.as_deref(),
                config.tls_key_file.as_deref(),
            ) {
                Ok(c) => Some(c),
                Err(e) => {
                    eprintln!("Failed to load TLS client config for replication: {}", e);
                    None // Fallback to TCP or fail? Fail usually.
                }
            }
        } else {
            None
        };

        tokio::spawn(async move {
            loop {
                println!("Connecting to master {}:{}", host, port);
                if let Err(e) = sockudo_kv::replication::replica::connect_to_master(
                    repl_clone.clone(),
                    store_clone.clone(),
                    &host,
                    port,
                    tls_config.clone(),
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
        let replication = Arc::clone(&replication);
        let config = Arc::clone(&config);

        tasks.spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((socket, addr)) => {
                        // Check maxclients limit
                        if !clients.can_accept() {
                            // Send error response and close connection immediately
                            let _ = socket.try_write(b"-ERR max number of clients reached\r\n");
                            server_state
                                .rejected_connections
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            continue;
                        }

                        let multi_store = Arc::clone(&multi_store);
                        let pubsub = Arc::clone(&pubsub);
                        let clients = Arc::clone(&clients);
                        let server_state = Arc::clone(&server_state);
                        let cluster_state = Arc::clone(&server_state.cluster);
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

    // TLS Listener
    if let Some((acceptor, listener)) = tls_acceptor {
        let acceptor = Arc::new(acceptor);
        let multi_store = Arc::clone(&multi_store);
        let pubsub = Arc::clone(&pubsub);
        let clients = Arc::clone(&clients);
        let server_state = Arc::clone(&server_state);
        let replication = Arc::clone(&replication);
        let config = Arc::clone(&config);

        tasks.spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        let acceptor = acceptor.clone();
                        let multi_store = Arc::clone(&multi_store);
                        let pubsub = Arc::clone(&pubsub);
                        let clients = Arc::clone(&clients);
                        let server_state = Arc::clone(&server_state);
                        let cluster_state = Arc::clone(&server_state.cluster);
                        let replication = Arc::clone(&replication);
                        let config = Arc::clone(&config);

                        tokio::spawn(async move {
                            match acceptor.accept(stream).await {
                                Ok(tls_stream) => {
                                    let _ = handle_tls_client(
                                        tls_stream,
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
                                }
                                Err(e) => eprintln!("TLS accept error: {}", e),
                            }
                        });
                    }
                    Err(e) => eprintln!("TLS accept error: {}", e),
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
        _ = async { tasks.join_all().await } => {
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

    // Apply TCP keepalive from config
    if config.tcp_keepalive > 0 {
        let _ = apply_tcp_keepalive(&socket, config.tcp_keepalive);
    }

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

async fn handle_client_inner<S>(
    socket: &mut S,
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
) -> std::io::Result<()>
where
    S: AsyncReadExt + AsyncWriteExt + Unpin,
{
    let mut buf = BytesMut::with_capacity(READ_BUF_SIZE);
    let mut write_buf = Vec::with_capacity(WRITE_BUF_SIZE);
    let mut in_pubsub_mode = false;

    loop {
        tokio::select! {
            biased;

            // Handle incoming commands with optional timeout
            read_result = async {
                if config.timeout > 0 {
                    match tokio::time::timeout(Duration::from_secs(config.timeout), socket.read_buf(&mut buf)).await {
                        Ok(res) => res,
                        Err(_) => {
                            // Timeout -> treat as 0 bytes read (EOF-like) to trigger close
                            Ok(0)
                        }
                    }
                } else {
                    socket.read_buf(&mut buf).await
                }
            } => {
                if read_result? == 0 {
                    return Ok(());
                }

                // Process ALL commands in buffer before writing
                loop {
                    match Parser::parse(&mut buf) {
                        Ok(Some(value)) => match Command::from_resp(value) {
                            Ok(mut cmd) => {
                                // Update last command timestamp
                                client.touch();

                                // Apply command renaming from config
                                // rename_command maps original -> renamed (or empty to disable)
                                let cmd_name_str = String::from_utf8_lossy(cmd.name()).to_uppercase();
                                if let Some(renamed) = config.rename_command.get(&cmd_name_str) {
                                    if renamed.is_empty() {
                                        // Command is disabled
                                        write_buf.extend_from_slice(b"-ERR unknown command '");
                                        write_buf.extend_from_slice(cmd.name());
                                        write_buf.extend_from_slice(b"', with args beginning with: ");
                                        if !cmd.args.is_empty() {
                                            write_buf.extend_from_slice(&cmd.args[0]);
                                        }
                                        write_buf.extend_from_slice(b"\r\n");
                                        continue;
                                    }
                                    // Replace command name with renamed version
                                    cmd = Command {
                                        name: bytes::Bytes::from(renamed.as_bytes().to_vec()),
                                        args: cmd.args,
                                    };
                                }

                                let cmd_name = cmd.name();

                                // Protected Mode Check
                                let is_loopback = match client.addr.ip() {
                                    std::net::IpAddr::V4(ip) => ip.is_loopback(),
                                    std::net::IpAddr::V6(ip) => ip.is_loopback(),
                                };

                                if config.protected_mode && !is_loopback && config.requirepass.is_none() {
                                    // Allowed commands in protected mode
                                    let is_allowed = cmd.is_command(b"PING")
                                        || cmd.is_command(b"QUIT")
                                        || cmd.is_command(b"COMMAND")
                                        || cmd.is_command(b"AUTH")
                                        || cmd.is_command(b"HELLO");

                                    if !is_allowed {
                                        write_buf.extend_from_slice(b"-DENIED Redis is running in protected mode because protected mode is enabled and no password is set for the default user. In this mode connections are only accepted from the loopback interface. If you want to connect from external computers to Redis you may adopt one of the following solutions: 1) Just disable protected mode sending the command 'CONFIG SET protected-mode no' from the loopback interface by connecting to the 127.0.0.1 interface. 2) Alternatively you can just disable the protected mode by editing the Redis configuration file, and setting the protected mode option to 'no'. 3) To allow access from other hosts you can set a password in the configuration file setting the 'requirepass' option. 4) If you trust the network to be safe, you can bind Redis to all the interfaces by setting the 'bind' option to '*'.\r\n");
                                        // Skip processing this command but continue loop for next commands in buffer?
                                        // Redis closes connection usually? No, it sends error.
                                        continue;
                                    }
                                }

                                // Check authentication
                                if clients.requires_auth() && !client.is_authenticated() {
                                    let is_allowed = cmd.is_command(b"AUTH")
                                        || cmd.is_command(b"HELLO")
                                        || cmd.is_command(b"QUIT");

                                    if !is_allowed {
                                        write_buf.extend_from_slice(b"-NOAUTH Authentication required.\r\n");
                                        continue;
                                    }
                                }

                                // Check if we need to handle Pub/Sub commands
                                if is_subscribe_command(cmd_name) {
                                    // Handle subscription commands
                                    let user_name = client.user.read();
                                    let acl_user = server_state.get_acl_user(&user_name);
                                    drop(user_name);

                                    match execute_subscribe(pubsub, sub_id, cmd_name, &cmd.args, acl_user.as_ref()) {
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

async fn handle_tls_client(
    mut socket: tokio_rustls::server::TlsStream<TcpStream>,
    addr: std::net::SocketAddr,
    multi_store: Arc<sockudo_kv::storage::MultiStore>,
    pubsub: Arc<PubSub>,
    clients: Arc<ClientManager>,
    server_state: Arc<ServerState>,
    cluster_state: Arc<ClusterState>,
    replication: Arc<sockudo_kv::ReplicationManager>,
    config: Arc<ServerConfig>,
) -> std::io::Result<()> {
    let (stream, _conn) = socket.get_ref();
    stream.set_nodelay(true)?;
    if config.tcp_keepalive > 0 {
        let _ = apply_tcp_keepalive(stream, config.tcp_keepalive);
    }

    let (sub_id, rx) = pubsub.register();
    let require_auth = config.requirepass.is_some();

    // TODO: Implement tls_auth_clients_user mapping here if x509 parsing supported

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

    pubsub.unregister(sub_id);
    clients.unregister(client.id);
    result
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

/// Create a TCP listener with socket2 for proper configuration (backlog, socket mark)
fn create_tcp_listener(
    addr: &str,
    backlog: i32,
    #[cfg(target_os = "linux")] socket_mark: u32,
) -> std::io::Result<TcpListener> {
    use std::net::SocketAddr;

    let socket_addr: SocketAddr = addr
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

    let domain = if socket_addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };

    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;

    // Allow address reuse
    socket.set_reuse_address(true)?;

    // Set socket mark on Linux
    #[cfg(target_os = "linux")]
    if socket_mark != 0 {
        // SO_MARK requires CAP_NET_ADMIN
        let _ = socket.set_mark(socket_mark);
    }

    // Bind to address
    socket.bind(&socket_addr.into())?;

    // Listen with configured backlog
    socket.listen(backlog)?;

    // Set non-blocking for tokio
    socket.set_nonblocking(true)?;

    // Convert to tokio TcpListener
    let std_listener: std::net::TcpListener = socket.into();
    TcpListener::from_std(std_listener)
}

/// Apply TCP keepalive settings to a connected socket
fn apply_tcp_keepalive(socket: &TcpStream, keepalive_secs: u32) -> std::io::Result<()> {
    use socket2::SockRef;

    if keepalive_secs > 0 {
        let sock_ref = SockRef::from(socket);
        let keepalive =
            socket2::TcpKeepalive::new().with_time(Duration::from_secs(keepalive_secs as u64));

        #[cfg(any(target_os = "linux", target_os = "macos", target_os = "ios"))]
        let keepalive = keepalive
            .with_interval(Duration::from_secs(keepalive_secs as u64 / 3))
            .with_retries(3);

        sock_ref.set_tcp_keepalive(&keepalive)?;
    }
    Ok(())
}

/// Create a Unix socket listener (Unix only)
#[cfg(unix)]
fn create_unix_listener(path: &str, perm: Option<u32>) -> std::io::Result<UnixListener> {
    use std::os::unix::fs::PermissionsExt;

    // Remove existing socket file if it exists
    let _ = std::fs::remove_file(path);

    let listener = std::os::unix::net::UnixListener::bind(path)?;
    listener.set_nonblocking(true)?;

    // Set permissions if specified
    if let Some(mode) = perm {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))?;
    }

    UnixListener::from_std(listener)
}
