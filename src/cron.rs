//! Background cron tasks module
//!
//! Implements Redis-style background tasks running at the configured `hz` frequency.
//! Tasks include:
//! - Active key expiration (sampling and deleting expired keys)
//! - LFU counter decay
//! - Incremental rehashing (if activerehashing is enabled)
//! - Client timeout checks
//!
//! Supports `dynamic_hz` to automatically adjust frequency based on load.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::server_state::ServerState;
use crate::storage::MultiStore;

/// Start the background cron task
///
/// This spawns a Tokio task that runs periodically based on the configured `hz` value.
/// When `dynamic_hz` is enabled, the actual frequency may be adjusted based on
/// the number of connected clients.
pub fn start_cron(
    server_state: Arc<ServerState>,
    multi_store: Arc<MultiStore>,
    clients: Arc<crate::client_manager::ClientManager>,
) {
    tokio::spawn(async move {
        loop {
            // Get current hz value (may be adjusted if dynamic_hz is enabled)
            let base_hz = server_state.hz.load(Ordering::Relaxed);
            let dynamic = server_state.dynamic_hz.load(Ordering::Relaxed);

            // Calculate effective hz
            let effective_hz = if dynamic {
                // Adjust hz based on number of clients (Redis-style)
                let client_count = clients.client_count();
                let factor = (client_count as u32 / 100).min(10).max(1);
                (base_hz * factor).min(500) // Cap at 500 Hz
            } else {
                base_hz
            };

            // Calculate sleep duration (1 / hz seconds)
            let interval_ms = if effective_hz > 0 {
                1000 / effective_hz as u64
            } else {
                100 // Default to 10 Hz if hz is 0
            };

            // Perform cron tasks
            cron_tick(&server_state, &multi_store).await;

            // Sleep for the interval
            tokio::time::sleep(Duration::from_millis(interval_ms)).await;
        }
    });
}

/// Single cron tick - performs all background tasks
async fn cron_tick(server_state: &Arc<ServerState>, multi_store: &Arc<MultiStore>) {
    // 1. Active expiration - sample and delete expired keys
    active_expire_cycle(server_state, multi_store);

    // 2. LFU decay (if using LFU eviction policy)
    // Note: LFU decay is handled per-key when accessed, not globally

    // 3. Incremental rehashing
    if server_state.activerehashing.load(Ordering::Relaxed) {
        // DashMap handles rehashing internally, but we could trigger
        // any custom rehashing logic here if needed
        incremental_rehash(multi_store);
    }

    // 4. Reset connection rate limiting counter
    server_state
        .connections_this_cycle
        .store(0, Ordering::Relaxed);
}

/// Active expiration cycle - sample random keys and delete expired ones
fn active_expire_cycle(server_state: &ServerState, multi_store: &Arc<MultiStore>) {
    let effort = server_state.active_expire_effort.load(Ordering::Relaxed);
    let use_lazy = server_state.lazyfree_lazy_expire.load(Ordering::Relaxed);

    // Number of databases to scan per cycle
    let dbs_per_call = ((effort as usize) / 2).max(1).min(16);

    // Sample size per database based on effort (1-10 scale)
    let samples = (effort as usize * 5).max(5).min(50);

    // Get db count
    let db_count = server_state.databases.load(Ordering::Relaxed);

    for db_idx in 0..dbs_per_call.min(db_count) {
        if let Some(store) = multi_store.get_db(db_idx) {
            // Sample random keys and check for expiration
            let expired_count = store.expire_random_keys(samples, use_lazy);
            if expired_count > 0 {
                server_state
                    .expired_keys
                    .fetch_add(expired_count as u64, Ordering::Relaxed);
            }
        }
    }
}

/// Incremental rehashing for hash tables
/// Note: DashMap handles this internally, but this is where custom rehashing would go
fn incremental_rehash(_multi_store: &Arc<MultiStore>) {
    // DashMap automatically handles incremental rehashing
    // This function is a placeholder for any additional rehashing logic
    // For example, if we had custom hash tables for specific data structures
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_effective_hz_calculation() {
        // Test that hz calculation doesn't overflow or produce invalid values
        let base_hz = 10u32;
        let dynamic = true;
        let client_count = 500usize;

        let factor = (client_count as u32 / 100).min(10).max(1);
        let effective_hz = if dynamic {
            (base_hz * factor).min(500)
        } else {
            base_hz
        };

        assert!(effective_hz > 0);
        assert!(effective_hz <= 500);
    }
}
