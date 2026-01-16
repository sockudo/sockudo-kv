//! Sockudo-KV Sentinel Binary
//!
//! Redis Sentinel mode for high availability monitoring and automatic failover.

use std::path::Path;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Get config file from command line
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "sentinel.conf".to_string());

    println!("Sockudo-KV Sentinel starting...");
    println!("Configuration file: {}", config_path);

    // Start sentinel mode
    sockudo_kv::sentinel_main::start_sentinel(Path::new(&config_path)).await
}
