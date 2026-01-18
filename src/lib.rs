// Allow certain clippy lints that are intentional design decisions
#![allow(clippy::too_many_arguments)] // Redis-like APIs often need many parameters
#![allow(clippy::type_complexity)] // Complex types represent Redis data structures
#![allow(clippy::large_enum_variant)] // DataType enum variants have intentionally different sizes

pub mod blocking;
pub mod cli;
pub mod client;
pub mod client_manager;
pub mod cluster;
pub mod cluster_state;
pub mod commands;
pub mod config;
pub mod config_table;
pub mod cron;
pub mod error;
pub mod glob_trie;
pub mod kernel_tuning;
pub mod keyspace_notify;
pub mod logging;
pub mod lua_engine;
pub mod lzf;
pub mod pattern;
pub mod persistence;
pub mod process_mgmt;
pub mod protocol;
pub mod pubsub;
pub mod replication;
pub mod resp;
pub mod sentinel;
pub mod sentinel_main;
pub mod server_state;
pub mod storage;
pub mod tls;

pub use client::ClientState;
pub use client_manager::ClientManager;
pub use cluster_state::ClusterState;
pub use error::{Error, Result};
pub use lua_engine::LuaEngine;
pub use pubsub::PubSub;
pub use replication::ReplicationManager;
pub use server_state::ServerState;
pub use storage::Store;

#[cfg(test)]
use mimalloc::MiMalloc;

#[cfg(test)]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;
