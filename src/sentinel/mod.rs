//! Redis Sentinel Implementation - Dragonfly-style high performance
//!
//! High-availability monitoring and automatic failover for Redis masters.
//!
//! Key optimizations:
//! 1. Lock-free instance flags using atomics
//! 2. DashMap for concurrent instance access
//! 3. Atomic timestamps for health tracking
//! 4. Lock-free quorum checking
//!
//! Features:
//! - Master/replica monitoring with health checks
//! - Automatic failover on master failure
//! - Sentinel cluster consensus (quorum-based)
//! - Configuration provider for clients
//! - Pub/Sub event notifications

pub mod commands;
pub mod config;
pub mod config_parser;
pub mod discovery;
pub mod election;
pub mod events;
pub mod failover;
pub mod monitor;
pub mod network;
pub mod persistence;
pub mod scripts;
pub mod state;
pub mod tilt;

pub use config::{MasterConfig, SentinelConfig};
pub use config_parser::{is_sentinel_config, parse_sentinel_config};
pub use election::LeaderElection;
pub use events::SentinelEvent;
pub use failover::SentinelFailover;
pub use monitor::SentinelMonitor;
pub use state::{InstanceFlags, MasterInstance, ReplicaInstance, SentinelInstance, SentinelState};
