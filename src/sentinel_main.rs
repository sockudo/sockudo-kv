//! Sentinel Mode Startup
//!
//! Entry point and initialization for Redis Sentinel mode.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::task::JoinSet;

use crate::pubsub::PubSub;
use crate::sentinel::config::SharedSentinelConfig;
use crate::sentinel::config_parser::parse_sentinel_config;
use crate::sentinel::discovery::DiscoveryService;
use crate::sentinel::election::LeaderElection;
use crate::sentinel::events::SentinelEventPublisher;
use crate::sentinel::failover::SentinelFailover;
use crate::sentinel::monitor::SentinelMonitor;
use crate::sentinel::network::SentinelServer;
use crate::sentinel::state::SentinelState;
use crate::sentinel::tilt::TiltManager;

/// Start sentinel mode
pub async fn start_sentinel(config_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting Sockudo-KV in Sentinel mode...");

    // Parse sentinel configuration
    let sentinel_config = parse_sentinel_config(config_path)?;

    println!(
        "Sentinel configuration loaded: port={}, {} masters monitored",
        sentinel_config.port,
        sentinel_config.masters.len()
    );

    for (name, master_cfg) in &sentinel_config.masters {
        println!(
            "  Monitoring master '{}' at {}:{} (quorum={})",
            name, master_cfg.ip, master_cfg.port, master_cfg.quorum
        );
    }

    // Create shared configuration
    let config = Arc::new(SharedSentinelConfig::new(sentinel_config.clone()));

    // Create PubSub for sentinel events
    let pubsub = Arc::new(PubSub::new());

    // Initialize sentinel state with generated ID
    let state = Arc::new(SentinelState::new(sentinel_config.myid.clone()));

    // Initialize event publisher
    let events = Arc::new(SentinelEventPublisher::new(pubsub.clone()));

    // Initialize tilt manager
    let tilt = Arc::new(TiltManager::new(state.clone()));

    // Initialize leader election
    let election = Arc::new(LeaderElection::new(state.clone(), events.clone()));

    // Initialize failover manager
    let failover = Arc::new(SentinelFailover::new(
        state.clone(),
        config.clone(),
        events.clone(),
        election.clone(),
    ));

    // Initialize monitor (will be used in future for active monitoring)
    let _monitor = Arc::new(SentinelMonitor::new(
        state.clone(),
        config.clone(),
        events.clone(),
        tilt.clone(),
    ));

    // Initialize discovery
    let discovery = Arc::new(DiscoveryService::new(
        state.clone(),
        config.clone(),
        events.clone(),
    ));

    // Create sentinel server
    let server = Arc::new(SentinelServer::new(
        state.clone(),
        config.clone(),
        events.clone(),
        failover.clone(),
        election.clone(),
    ));

    // Bind TCP listener
    let bind_addr = format!(
        "{}:{}",
        sentinel_config
            .bind
            .get(0)
            .unwrap_or(&"0.0.0.0".to_string()),
        sentinel_config.port
    );
    let listener = TcpListener::bind(&bind_addr).await?;
    println!("Sentinel listening on {}", bind_addr);

    // Start background tasks
    let mut tasks = JoinSet::new();

    // Start discovery loop
    let discovery_clone = discovery.clone();
    tasks.spawn(async move {
        loop {
            discovery_clone.publish_hello_to_all().await;
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });

    // Start tilt detection
    let tilt_clone = tilt.clone();
    tasks.spawn(async move {
        loop {
            tilt_clone.check_tilt();
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });

    // Start network server (blocking)
    let server_task = tokio::spawn(async move {
        if let Err(e) = server.run(listener).await {
            eprintln!("Sentinel server error: {}", e);
        }
    });

    // Wait for shutdown signal or server termination
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            println!("Shutdown signal received.");
        }
        result = server_task => {
            match result {
                Ok(_) => println!("Sentinel server stopped."),
                Err(e) => eprintln!("Sentinel server error: {}", e),
            }
        }
        _ = async { tasks.join_all().await } => {
            println!("Background tasks stopped unexpectedly.");
        }
    }

    // Cleanup
    println!("Sentinel shutting down...");
    Ok(())
}
