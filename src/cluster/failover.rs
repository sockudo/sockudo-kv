//! Cluster Failover Manager
//!
//! Handles automatic failover decisions based on cluster configuration:
//! - cluster_replica_validity_factor: Determines if replica data is fresh enough
//! - cluster_replica_no_failover: Disables automatic failover
//! - cluster_migration_barrier: Minimum replicas before migration
//!
//! Failover flow:
//! 1. Master marked FAIL by majority
//! 2. Replicas check validity (disconnection time)
//! 3. Valid replica starts election
//! 4. Replica with highest priority/offset wins

use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use log::{debug, info};

use crate::cluster_state::{ClusterNode, ClusterState, NodeRole};
use crate::config::ServerConfig;
use parking_lot::RwLock;

/// Failover manager for cluster automatic failover
pub struct FailoverManager {
    cluster: Arc<ClusterState>,
    config: Arc<RwLock<ServerConfig>>,
}

/// Result of replica validity check
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaValidityStatus {
    /// Replica is valid and can failover
    Valid,
    /// Replica has been disconnected too long
    TooStale,
    /// Replica has NOFAILOVER flag set
    NoFailoverFlag,
    /// No master configured for this replica
    NoMaster,
}

impl FailoverManager {
    pub fn new(cluster: Arc<ClusterState>, config: Arc<RwLock<ServerConfig>>) -> Self {
        Self { cluster, config }
    }

    /// Check if a replica is valid for automatic failover
    ///
    /// A replica is valid if:
    /// 1. It has a master configured
    /// 2. It doesn't have NOFAILOVER flag
    /// 3. Its disconnection time is within the validity window:
    ///    `max_time = cluster_node_timeout * cluster_replica_validity_factor + repl_ping_replica_period`
    ///
    /// If cluster_replica_validity_factor is 0, the replica is always valid (availability over consistency)
    pub fn check_replica_validity(&self, replica: &ClusterNode) -> ReplicaValidityStatus {
        // Check if replica has NOFAILOVER flag
        if replica.flags.nofailover() {
            return ReplicaValidityStatus::NoFailoverFlag;
        }

        // Check if replica has a master
        let master_id = replica.master_id.read();
        if master_id.is_none() {
            return ReplicaValidityStatus::NoMaster;
        }
        let master_id = master_id.clone().unwrap();
        drop(master_id);

        let config = self.config.read();

        // If validity factor is 0, always consider valid
        if config.cluster_replica_validity_factor == 0 {
            return ReplicaValidityStatus::Valid;
        }

        // Calculate max allowed disconnection time
        let max_disconnect_ms = (config.cluster_node_timeout
            * config.cluster_replica_validity_factor as u64)
            + (config.repl_ping_replica_period * 1000);

        // Get last pong received from master
        let last_pong = replica.pong_recv.load(Ordering::Relaxed);
        let now = crate::storage::now_ms() as u64;

        let disconnect_time = now.saturating_sub(last_pong);

        if disconnect_time > max_disconnect_ms {
            debug!(
                "Replica {} is too stale: disconnected for {}ms (max: {}ms)",
                String::from_utf8_lossy(&replica.id),
                disconnect_time,
                max_disconnect_ms
            );
            return ReplicaValidityStatus::TooStale;
        }

        ReplicaValidityStatus::Valid
    }

    /// Check if automatic failover is enabled for a node
    pub fn is_failover_enabled(&self, node: &ClusterNode) -> bool {
        // Check node-level NOFAILOVER flag
        if node.flags.nofailover() {
            return false;
        }

        // Check global cluster_replica_no_failover config
        let config = self.config.read();
        !config.cluster_replica_no_failover
    }

    /// Check if master has enough replicas to allow migration
    /// Returns true if migration can proceed
    pub fn can_migrate_replica(&self, master_id: &Bytes) -> bool {
        let config = self.config.read();

        // If migration barrier is 0, always allow
        if config.cluster_migration_barrier == 0 {
            return true;
        }

        // Count replicas for this master
        let replicas = self.cluster.get_replicas(master_id);
        let replica_count = replicas.len() as u32;

        // Allow migration only if we have more replicas than the barrier
        replica_count > config.cluster_migration_barrier
    }

    /// Check if replica auto-migration is enabled
    pub fn is_replica_migration_enabled(&self) -> bool {
        self.config.read().cluster_allow_replica_migration
    }

    /// Find orphan masters (masters with no replicas that need coverage)
    /// For automatic replica migration
    pub fn find_orphan_masters(&self) -> Vec<Bytes> {
        let mut orphan_masters = Vec::new();

        // Check if this node has slots (is a master with data)
        if self.cluster.slots.count() > 0 {
            // Check our replicas
            let my_replicas = self.cluster.get_replicas(&self.cluster.my_id);
            if my_replicas.is_empty() {
                orphan_masters.push(self.cluster.my_id.clone());
            }
        }

        // Check other masters
        for node_entry in self.cluster.nodes.iter() {
            let node = node_entry.value();

            // Skip replicas
            if node.get_role() == NodeRole::Replica {
                continue;
            }

            // Skip failed nodes
            if node.flags.fail() {
                continue;
            }

            // Check if master has slots
            if node.slots.count() == 0 {
                continue;
            }

            // Check if master has no replicas
            let replicas = self.cluster.get_replicas(&node.id);
            if replicas.is_empty() {
                orphan_masters.push(node.id.clone());
            }
        }

        orphan_masters
    }

    /// Check if we should initiate failover for a failed master
    /// Returns the master ID if we should failover
    pub fn should_initiate_failover(&self) -> Option<Bytes> {
        // Only replicas can initiate failover
        // Check if we're a replica
        let my_master_id = {
            // For self node, we need to check if we have a master
            // If we have replicaof set in config, we're a replica
            let config = self.config.read();
            config.replicaof.clone()
        };

        let master_id = match my_master_id {
            Some((host, port)) => {
                // Find master by host:port
                self.cluster.find_node_by_ip_port(&host, port)?
            }
            None => return None, // We're not a replica
        };

        // Check if master is marked as FAIL
        let master = self.cluster.get_node(&master_id)?;
        if !master.flags.fail() {
            return None;
        }

        // Check if failover is enabled
        if self.config.read().cluster_replica_no_failover {
            info!("Failover disabled by cluster_replica_no_failover config");
            return None;
        }

        // Create a fake replica node for self to check validity
        // In real implementation, we'd track our own disconnection time
        // For now, we assume we're valid if we detected master failure recently

        info!(
            "Master {} is FAIL, initiating failover",
            String::from_utf8_lossy(&master_id)
        );
        Some(master_id)
    }

    /// Perform slot migration write pause check
    /// Returns true if writes should be paused during migration
    pub fn should_pause_writes_for_migration(&self, slot: u16) -> bool {
        // Check if slot is being migrated
        if self.cluster.migrating.contains_key(&slot) {
            let config = self.config.read();
            // Pause timeout > 0 means we should pause
            config.cluster_slot_migration_write_pause_timeout > 0
        } else {
            false
        }
    }

    /// Check if handoff should be delayed due to replication lag
    pub fn should_delay_handoff(&self, slot: u16, current_lag_bytes: u64) -> bool {
        let config = self.config.read();

        // If slot is not being migrated, no delay
        if !self.cluster.migrating.contains_key(&slot) {
            return false;
        }

        // Check if lag exceeds threshold
        current_lag_bytes > config.cluster_slot_migration_handoff_max_lag_bytes
    }
}

/// Calculate quorum needed for FAIL agreement
/// In Redis, this is (total masters / 2) + 1
pub fn calculate_fail_quorum(total_masters: usize) -> usize {
    (total_masters / 2) + 1
}

/// Check PFAIL reports and upgrade to FAIL if quorum reached
pub fn check_pfail_to_fail(cluster: &ClusterState, node: &ClusterNode) -> bool {
    if !node.flags.pfail() {
        return false;
    }

    // Count failure reports
    let failure_count = node.failure_reports.len();

    // Count total masters (including self)
    let total_masters = cluster
        .nodes
        .iter()
        .filter(|n| n.value().get_role() == NodeRole::Master)
        .count()
        + 1; // +1 for self if we're a master

    let quorum = calculate_fail_quorum(total_masters);

    if failure_count >= quorum {
        info!(
            "Node {} upgraded from PFAIL to FAIL (reports: {}, quorum: {})",
            String::from_utf8_lossy(&node.id),
            failure_count,
            quorum
        );
        node.flags.set_fail(true);
        node.flags.set_pfail(false);
        return true;
    }

    false
}
