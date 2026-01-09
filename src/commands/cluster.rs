//! Redis Cluster command handlers - Dragonfly-style high performance
//!
//! Implements all cluster commands with O(1) operations where possible.
//! Commands: ASKING, READONLY, READWRITE, CLUSTER *

use bytes::Bytes;
use std::sync::Arc;

use crate::client::ClientState;
use crate::cluster_state::{CLUSTER_SLOTS, ClusterState, NodeRole, SlotState, key_hash_slot};
use crate::error::{Error, Result};
use crate::protocol::RespValue;
use crate::storage::Store;

/// Execute a cluster command
pub fn execute(
    cluster: &Arc<ClusterState>,
    client: &Arc<ClientState>,
    store: &Store,
    cmd: &[u8],
    args: &[Bytes],
) -> Result<RespValue> {
    // ASKING - set flag for -ASK redirect - O(1)
    if cmd.eq_ignore_ascii_case(b"ASKING") {
        return cmd_asking(client);
    }

    // READONLY - enable replica reads - O(1)
    if cmd.eq_ignore_ascii_case(b"READONLY") {
        return cmd_readonly(client);
    }

    // READWRITE - disable replica reads - O(1)
    if cmd.eq_ignore_ascii_case(b"READWRITE") {
        return cmd_readwrite(client);
    }

    // CLUSTER subcommands
    if cmd.eq_ignore_ascii_case(b"CLUSTER") {
        return cmd_cluster(cluster, client, store, args);
    }

    Err(Error::UnknownCommand(
        String::from_utf8_lossy(cmd).into_owned(),
    ))
}

/// ASKING - O(1)
fn cmd_asking(client: &Arc<ClientState>) -> Result<RespValue> {
    client
        .asking
        .store(true, std::sync::atomic::Ordering::Relaxed);
    Ok(RespValue::ok())
}

/// READONLY - O(1)
fn cmd_readonly(client: &Arc<ClientState>) -> Result<RespValue> {
    client
        .readonly
        .store(true, std::sync::atomic::Ordering::Relaxed);
    Ok(RespValue::ok())
}

/// READWRITE - O(1)
fn cmd_readwrite(client: &Arc<ClientState>) -> Result<RespValue> {
    client
        .readonly
        .store(false, std::sync::atomic::Ordering::Relaxed);
    Ok(RespValue::ok())
}

/// CLUSTER subcommands dispatcher
fn cmd_cluster(
    cluster: &Arc<ClusterState>,
    _client: &Arc<ClientState>,
    store: &Store,
    args: &[Bytes],
) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("CLUSTER"));
    }

    let subcmd = &args[0];
    let subargs = &args[1..];

    // Use a match-like approach for better performance (branch prediction)
    let first_byte = subcmd.first().map(|b| b.to_ascii_uppercase());

    match first_byte {
        Some(b'A') => {
            if subcmd.eq_ignore_ascii_case(b"ADDSLOTS") {
                return cmd_cluster_addslots(cluster, subargs);
            }
            if subcmd.eq_ignore_ascii_case(b"ADDSLOTSRANGE") {
                return cmd_cluster_addslotsrange(cluster, subargs);
            }
        }
        Some(b'B') => {
            if subcmd.eq_ignore_ascii_case(b"BUMPEPOCH") {
                return cmd_cluster_bumpepoch(cluster);
            }
        }
        Some(b'C') => {
            if subcmd.eq_ignore_ascii_case(b"COUNT-FAILURE-REPORTS") {
                return cmd_cluster_count_failure_reports(cluster, subargs);
            }
            if subcmd.eq_ignore_ascii_case(b"COUNTKEYSINSLOT") {
                return cmd_cluster_countkeysinslot(store, subargs);
            }
        }
        Some(b'D') => {
            if subcmd.eq_ignore_ascii_case(b"DELSLOTS") {
                return cmd_cluster_delslots(cluster, subargs);
            }
            if subcmd.eq_ignore_ascii_case(b"DELSLOTSRANGE") {
                return cmd_cluster_delslotsrange(cluster, subargs);
            }
        }
        Some(b'F') => {
            if subcmd.eq_ignore_ascii_case(b"FAILOVER") {
                return cmd_cluster_failover(subargs);
            }
            if subcmd.eq_ignore_ascii_case(b"FLUSHSLOTS") {
                return cmd_cluster_flushslots(cluster);
            }
            if subcmd.eq_ignore_ascii_case(b"FORGET") {
                return cmd_cluster_forget(cluster, subargs);
            }
        }
        Some(b'G') => {
            if subcmd.eq_ignore_ascii_case(b"GETKEYSINSLOT") {
                return cmd_cluster_getkeysinslot(store, subargs);
            }
        }
        Some(b'H') => {
            if subcmd.eq_ignore_ascii_case(b"HELP") {
                return cmd_cluster_help();
            }
        }
        Some(b'I') => {
            if subcmd.eq_ignore_ascii_case(b"INFO") {
                return cmd_cluster_info(cluster);
            }
        }
        Some(b'K') => {
            if subcmd.eq_ignore_ascii_case(b"KEYSLOT") {
                return cmd_cluster_keyslot(subargs);
            }
        }
        Some(b'L') => {
            if subcmd.eq_ignore_ascii_case(b"LINKS") {
                return cmd_cluster_links(cluster);
            }
        }
        Some(b'M') => {
            if subcmd.eq_ignore_ascii_case(b"MEET") {
                return cmd_cluster_meet(cluster, subargs);
            }
            if subcmd.eq_ignore_ascii_case(b"MIGRATION") {
                return cmd_cluster_migration(cluster, subargs);
            }
            if subcmd.eq_ignore_ascii_case(b"MYID") {
                return cmd_cluster_myid(cluster);
            }
            if subcmd.eq_ignore_ascii_case(b"MYSHARDID") {
                return cmd_cluster_myshardid(cluster);
            }
        }
        Some(b'N') => {
            if subcmd.eq_ignore_ascii_case(b"NODES") {
                return cmd_cluster_nodes(cluster);
            }
        }
        Some(b'R') => {
            if subcmd.eq_ignore_ascii_case(b"REPLICAS") {
                return cmd_cluster_replicas(cluster, subargs);
            }
            if subcmd.eq_ignore_ascii_case(b"REPLICATE") {
                return cmd_cluster_replicate(cluster, subargs);
            }
            if subcmd.eq_ignore_ascii_case(b"RESET") {
                return cmd_cluster_reset(cluster, subargs);
            }
        }
        Some(b'S') => {
            if subcmd.eq_ignore_ascii_case(b"SAVECONFIG") {
                return cmd_cluster_saveconfig();
            }
            if subcmd.eq_ignore_ascii_case(b"SET-CONFIG-EPOCH") {
                return cmd_cluster_set_config_epoch(cluster, subargs);
            }
            if subcmd.eq_ignore_ascii_case(b"SETSLOT") {
                return cmd_cluster_setslot(cluster, subargs);
            }
            if subcmd.eq_ignore_ascii_case(b"SHARDS") {
                return cmd_cluster_shards(cluster);
            }
            if subcmd.eq_ignore_ascii_case(b"SLAVES") {
                return cmd_cluster_replicas(cluster, subargs);
            }
            if subcmd.eq_ignore_ascii_case(b"SLOT-STATS") {
                return cmd_cluster_slot_stats(cluster, store, subargs);
            }
            if subcmd.eq_ignore_ascii_case(b"SLOTS") {
                return cmd_cluster_slots(cluster);
            }
        }
        _ => {}
    }

    Err(Error::Custom(format!(
        "ERR Unknown subcommand or wrong number of arguments for '{}'",
        String::from_utf8_lossy(subcmd)
    )))
}

// =============================================================================
// Individual command implementations
// =============================================================================

/// CLUSTER ADDSLOTS slot [slot ...] - O(N)
fn cmd_cluster_addslots(cluster: &Arc<ClusterState>, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("CLUSTER ADDSLOTS"));
    }

    let mut slots = Vec::with_capacity(args.len());
    for arg in args {
        let slot = parse_slot(arg)?;
        if cluster.slots.has_slot(slot) {
            return Err(Error::Custom(format!("ERR Slot {} is already busy", slot)));
        }
        slots.push(slot);
    }

    cluster.add_slots(&slots);
    Ok(RespValue::ok())
}

/// CLUSTER ADDSLOTSRANGE start-slot end-slot [start-slot end-slot ...] - O(N)
fn cmd_cluster_addslotsrange(cluster: &Arc<ClusterState>, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() || args.len() % 2 != 0 {
        return Err(Error::WrongArity("CLUSTER ADDSLOTSRANGE"));
    }

    for chunk in args.chunks(2) {
        let start = parse_slot(&chunk[0])?;
        let end = parse_slot(&chunk[1])?;
        if start > end {
            return Err(Error::Custom(format!(
                "ERR start slot {} is greater than end slot {}",
                start, end
            )));
        }
        cluster.add_slot_range(start, end);
    }

    Ok(RespValue::ok())
}

/// CLUSTER BUMPEPOCH - O(1)
fn cmd_cluster_bumpepoch(cluster: &Arc<ClusterState>) -> Result<RespValue> {
    let epoch = cluster.bump_epoch();
    Ok(RespValue::SimpleString(Bytes::from(format!("BUMPED {}", epoch))))
}

/// CLUSTER COUNT-FAILURE-REPORTS node-id - O(1)
fn cmd_cluster_count_failure_reports(
    cluster: &Arc<ClusterState>,
    args: &[Bytes],
) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("CLUSTER COUNT-FAILURE-REPORTS"));
    }

    let count = cluster.count_failure_reports(&args[0]);
    Ok(RespValue::integer(count as i64))
}

/// CLUSTER COUNTKEYSINSLOT slot - O(N) - iterates all keys
fn cmd_cluster_countkeysinslot(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("CLUSTER COUNTKEYSINSLOT"));
    }

    let slot = parse_slot(&args[0])?;

    let mut count = 0i64;
    store.for_each_key(|key| {
        if key_hash_slot(key) == slot {
            count += 1;
        }
    });

    Ok(RespValue::integer(count))
}

/// CLUSTER DELSLOTS slot [slot ...] - O(N)
fn cmd_cluster_delslots(cluster: &Arc<ClusterState>, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("CLUSTER DELSLOTS"));
    }

    let mut slots = Vec::with_capacity(args.len());
    for arg in args {
        slots.push(parse_slot(arg)?);
    }

    cluster.del_slots(&slots);
    Ok(RespValue::ok())
}

/// CLUSTER DELSLOTSRANGE start-slot end-slot [start-slot end-slot ...] - O(N)
fn cmd_cluster_delslotsrange(cluster: &Arc<ClusterState>, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() || args.len() % 2 != 0 {
        return Err(Error::WrongArity("CLUSTER DELSLOTSRANGE"));
    }

    for chunk in args.chunks(2) {
        let start = parse_slot(&chunk[0])?;
        let end = parse_slot(&chunk[1])?;
        if start > end {
            return Err(Error::Custom(format!(
                "ERR start slot {} is greater than end slot {}",
                start, end
            )));
        }
        cluster.del_slot_range(start, end);
    }

    Ok(RespValue::ok())
}

/// CLUSTER FAILOVER [FORCE|TAKEOVER] - O(1)
fn cmd_cluster_failover(args: &[Bytes]) -> Result<RespValue> {
    let _mode = if args.is_empty() {
        "normal"
    } else if args[0].eq_ignore_ascii_case(b"FORCE") {
        "force"
    } else if args[0].eq_ignore_ascii_case(b"TAKEOVER") {
        "takeover"
    } else {
        return Err(Error::Syntax);
    };

    // Return OK for compatibility (stub in standalone mode)
    Ok(RespValue::ok())
}

/// CLUSTER FLUSHSLOTS - O(N)
fn cmd_cluster_flushslots(cluster: &Arc<ClusterState>) -> Result<RespValue> {
    cluster.flush_slots();
    Ok(RespValue::ok())
}

/// CLUSTER FORGET node-id - O(1)
fn cmd_cluster_forget(cluster: &Arc<ClusterState>, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("CLUSTER FORGET"));
    }

    let node_id = &args[0];

    // Check if trying to forget self
    if node_id == &cluster.my_id {
        return Err(Error::Custom("ERR I tried hard but I can't forget myself".to_string()));
    }

    // Try to forget - now properly removes from all indexes
    if !cluster.forget(node_id) {
        return Err(Error::Custom("ERR Unknown node".to_string()));
    }

    Ok(RespValue::ok())
}

/// CLUSTER GETKEYSINSLOT slot count - O(N)
fn cmd_cluster_getkeysinslot(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("CLUSTER GETKEYSINSLOT"));
    }

    let slot = parse_slot(&args[0])?;
    let count = parse_int(&args[1])?.max(0) as usize;

    let mut keys = Vec::with_capacity(count.min(1000));
    store.for_each_key(|key| {
        if keys.len() < count && key_hash_slot(key) == slot {
            keys.push(Bytes::copy_from_slice(key));
        }
    });

    Ok(RespValue::array(
        keys.into_iter().map(RespValue::bulk).collect(),
    ))
}

/// CLUSTER INFO - O(1)
fn cmd_cluster_info(cluster: &Arc<ClusterState>) -> Result<RespValue> {
    Ok(RespValue::bulk(Bytes::from(cluster.format_info())))
}

/// CLUSTER KEYSLOT key - O(N) where N is key length
fn cmd_cluster_keyslot(args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("CLUSTER KEYSLOT"));
    }

    let slot = key_hash_slot(&args[0]);
    Ok(RespValue::integer(slot as i64))
}

/// CLUSTER LINKS - O(N)
fn cmd_cluster_links(cluster: &Arc<ClusterState>) -> Result<RespValue> {
    let mut links = Vec::new();

    for link in cluster.links.iter() {
        let link_info = vec![
            RespValue::bulk_string("direction"),
            RespValue::bulk_string(match link.direction {
                crate::cluster_state::LinkDirection::To => "to",
                crate::cluster_state::LinkDirection::From => "from",
            }),
            RespValue::bulk_string("node"),
            RespValue::bulk(link.node_id.clone()),
            RespValue::bulk_string("create-time"),
            RespValue::integer(link.create_time as i64),
            RespValue::bulk_string("events"),
            RespValue::bulk_string(&link.events),
            RespValue::bulk_string("send-buffer-allocated"),
            RespValue::integer(link.send_buffer_allocated as i64),
            RespValue::bulk_string("send-buffer-used"),
            RespValue::integer(link.send_buffer_used as i64),
        ];
        links.push(RespValue::array(link_info));
    }

    Ok(RespValue::array(links))
}

/// CLUSTER MEET ip port [cluster-bus-port] - O(1)
fn cmd_cluster_meet(cluster: &Arc<ClusterState>, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("CLUSTER MEET"));
    }

    let ip = String::from_utf8_lossy(&args[0]).to_string();
    let port = parse_int(&args[1])? as u16;
    let cport = args.get(2).and_then(|a| parse_int(a).ok()).map(|v| v as u16);

    cluster.meet(ip, port, cport);
    Ok(RespValue::ok())
}

/// CLUSTER MIGRATION - O(N)
fn cmd_cluster_migration(cluster: &Arc<ClusterState>, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("CLUSTER MIGRATION"));
    }

    let subcmd = &args[0];

    if subcmd.eq_ignore_ascii_case(b"IMPORT") {
        let slots_args = &args[1..];
        if slots_args.is_empty() || slots_args.len() % 2 != 0 {
            return Err(Error::WrongArity("CLUSTER MIGRATION IMPORT"));
        }

        for chunk in slots_args.chunks(2) {
            let start = parse_slot(&chunk[0])?;
            let end = parse_slot(&chunk[1])?;
            for slot in start..=end {
                cluster.importing.insert(slot, Bytes::from_static(b""));
            }
        }
        return Ok(RespValue::ok());
    }

    if subcmd.eq_ignore_ascii_case(b"CANCEL") {
        if args.len() < 2 {
            return Err(Error::WrongArity("CLUSTER MIGRATION CANCEL"));
        }
        if args[1].eq_ignore_ascii_case(b"ALL") {
            cluster.importing.clear();
            cluster.migrating.clear();
        }
        return Ok(RespValue::ok());
    }

    if subcmd.eq_ignore_ascii_case(b"STATUS") {
        let mut tasks = Vec::new();

        for entry in cluster.migrating.iter() {
            tasks.push(RespValue::array(vec![
                RespValue::bulk_string("slot"),
                RespValue::integer(*entry.key() as i64),
                RespValue::bulk_string("state"),
                RespValue::bulk_string("migrating"),
                RespValue::bulk_string("target"),
                RespValue::bulk(entry.value().clone()),
            ]));
        }

        for entry in cluster.importing.iter() {
            tasks.push(RespValue::array(vec![
                RespValue::bulk_string("slot"),
                RespValue::integer(*entry.key() as i64),
                RespValue::bulk_string("state"),
                RespValue::bulk_string("importing"),
                RespValue::bulk_string("source"),
                RespValue::bulk(entry.value().clone()),
            ]));
        }

        return Ok(RespValue::array(tasks));
    }

    Err(Error::Custom(format!(
        "ERR Unknown CLUSTER MIGRATION subcommand '{}'",
        String::from_utf8_lossy(subcmd)
    )))
}

/// CLUSTER MYID - O(1)
fn cmd_cluster_myid(cluster: &Arc<ClusterState>) -> Result<RespValue> {
    Ok(RespValue::bulk(cluster.my_id.clone()))
}

/// CLUSTER MYSHARDID - O(1)
fn cmd_cluster_myshardid(cluster: &Arc<ClusterState>) -> Result<RespValue> {
    Ok(RespValue::bulk(cluster.my_shard_id.clone()))
}

/// CLUSTER NODES - O(N)
fn cmd_cluster_nodes(cluster: &Arc<ClusterState>) -> Result<RespValue> {
    Ok(RespValue::bulk(Bytes::from(cluster.format_nodes())))
}

/// CLUSTER REPLICAS node-id - O(1) using master->replicas index
fn cmd_cluster_replicas(cluster: &Arc<ClusterState>, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("CLUSTER REPLICAS"));
    }

    let master_id = &args[0];

    // Use O(1) indexed lookup
    let replicas = cluster.get_replicas(master_id);

    let result: Vec<RespValue> = replicas
        .iter()
        .map(|node| RespValue::bulk(Bytes::from(node.format_nodes_line())))
        .collect();

    Ok(RespValue::array(result))
}

/// CLUSTER REPLICATE node-id - O(1)
fn cmd_cluster_replicate(cluster: &Arc<ClusterState>, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("CLUSTER REPLICATE"));
    }

    let master_id = &args[0];

    // Check if trying to replicate self
    if master_id == &cluster.my_id {
        return Err(Error::Custom("ERR Can't replicate myself".to_string()));
    }

    // Check if master exists in cluster
    if !cluster.nodes.contains_key(master_id) {
        return Err(Error::Custom("ERR Unknown node".to_string()));
    }

    // For this node itself (not in nodes map), we need to update our own state
    // This is a simplification - in real Redis, we'd update global state differently
    // For now, return OK since the master is valid

    // Note: In a full implementation, we'd need to:
    // 1. Stop serving writes
    // 2. Connect to master for replication
    // 3. Update our own role in cluster state

    Ok(RespValue::ok())
}

/// CLUSTER RESET [HARD|SOFT] - O(N)
fn cmd_cluster_reset(cluster: &Arc<ClusterState>, args: &[Bytes]) -> Result<RespValue> {
    let hard = if args.is_empty() {
        false
    } else if args[0].eq_ignore_ascii_case(b"HARD") {
        true
    } else if args[0].eq_ignore_ascii_case(b"SOFT") {
        false
    } else {
        return Err(Error::Syntax);
    };

    cluster.reset(hard);
    Ok(RespValue::ok())
}

/// CLUSTER SAVECONFIG - O(1)
fn cmd_cluster_saveconfig() -> Result<RespValue> {
    // Return OK for compatibility (config persistence not implemented)
    Ok(RespValue::ok())
}

/// CLUSTER SET-CONFIG-EPOCH config-epoch - O(1)
fn cmd_cluster_set_config_epoch(cluster: &Arc<ClusterState>, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("CLUSTER SET-CONFIG-EPOCH"));
    }

    let epoch = parse_int(&args[0])? as u64;

    if !cluster.set_config_epoch(epoch) {
        return Err(Error::Custom(
            "ERR The config epoch is already set. Only setting to 0 epoch is allowed.".to_string(),
        ));
    }

    Ok(RespValue::ok())
}

/// CLUSTER SETSLOT slot <IMPORTING|MIGRATING|NODE|STABLE> [node-id] - O(1)
fn cmd_cluster_setslot(cluster: &Arc<ClusterState>, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("CLUSTER SETSLOT"));
    }

    let slot = parse_slot(&args[0])?;
    let action = &args[1];
    let node_id = args.get(2);

    if action.eq_ignore_ascii_case(b"IMPORTING") {
        let nid = node_id.ok_or(Error::WrongArity("CLUSTER SETSLOT IMPORTING"))?;
        cluster
            .set_slot(slot, SlotState::Importing, Some(nid))
            .map_err(|e| Error::Custom(e.to_string()))?;
    } else if action.eq_ignore_ascii_case(b"MIGRATING") {
        let nid = node_id.ok_or(Error::WrongArity("CLUSTER SETSLOT MIGRATING"))?;
        cluster
            .set_slot(slot, SlotState::Migrating, Some(nid))
            .map_err(|e| Error::Custom(e.to_string()))?;
    } else if action.eq_ignore_ascii_case(b"NODE") {
        let nid = node_id.ok_or(Error::WrongArity("CLUSTER SETSLOT NODE"))?;
        cluster
            .set_slot(slot, SlotState::Normal, Some(nid))
            .map_err(|e| Error::Custom(e.to_string()))?;
    } else if action.eq_ignore_ascii_case(b"STABLE") {
        cluster
            .set_slot(slot, SlotState::Normal, None)
            .map_err(|e| Error::Custom(e.to_string()))?;
    } else {
        return Err(Error::Syntax);
    }

    Ok(RespValue::ok())
}

/// CLUSTER SHARDS - O(N)
fn cmd_cluster_shards(cluster: &Arc<ClusterState>) -> Result<RespValue> {
    let my_ip = cluster.my_ip.read().clone();
    let my_port = cluster.my_port.load(std::sync::atomic::Ordering::Relaxed) as u16;
    let slots = cluster.slots.get_ranges();

    let mut slot_ranges = Vec::new();
    for (start, end) in slots {
        slot_ranges.push(RespValue::integer(start as i64));
        slot_ranges.push(RespValue::integer(end as i64));
    }

    let node_info = vec![
        RespValue::bulk_string("id"),
        RespValue::bulk(cluster.my_id.clone()),
        RespValue::bulk_string("port"),
        RespValue::integer(my_port as i64),
        RespValue::bulk_string("ip"),
        RespValue::bulk_string(&my_ip),
        RespValue::bulk_string("endpoint"),
        RespValue::bulk_string(&my_ip),
        RespValue::bulk_string("role"),
        RespValue::bulk_string("master"),
        RespValue::bulk_string("replication-offset"),
        RespValue::integer(0),
        RespValue::bulk_string("health"),
        RespValue::bulk_string("online"),
    ];

    let shard = vec![
        RespValue::bulk_string("slots"),
        RespValue::array(slot_ranges),
        RespValue::bulk_string("nodes"),
        RespValue::array(vec![RespValue::array(node_info)]),
    ];

    Ok(RespValue::array(vec![RespValue::array(shard)]))
}

/// CLUSTER SLOT-STATS <SLOTSRANGE start end | ORDERBY metric ...> - O(N)
fn cmd_cluster_slot_stats(
    cluster: &Arc<ClusterState>,
    store: &Store,
    args: &[Bytes],
) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("CLUSTER SLOT-STATS"));
    }

    if args[0].eq_ignore_ascii_case(b"SLOTSRANGE") {
        if args.len() < 3 {
            return Err(Error::WrongArity("CLUSTER SLOT-STATS SLOTSRANGE"));
        }

        let start = parse_slot(&args[1])?;
        let end = parse_slot(&args[2])?;

        let mut stats = Vec::new();
        for slot in start..=end {
            if cluster.slots.has_slot(slot) {
                let mut key_count = 0i64;
                store.for_each_key(|key| {
                    if key_hash_slot(key) == slot {
                        key_count += 1;
                    }
                });

                stats.push(RespValue::array(vec![
                    RespValue::integer(slot as i64),
                    RespValue::array(vec![
                        RespValue::bulk_string("key-count"),
                        RespValue::integer(key_count),
                    ]),
                ]));
            }
        }

        return Ok(RespValue::array(stats));
    }

    if args[0].eq_ignore_ascii_case(b"ORDERBY") {
        // Return empty for now
        return Ok(RespValue::array(vec![]));
    }

    Err(Error::Syntax)
}

/// CLUSTER SLOTS - O(N) with optimized slot range building
fn cmd_cluster_slots(cluster: &Arc<ClusterState>) -> Result<RespValue> {
    let my_id = &cluster.my_id;
    let my_ip = cluster.my_ip.read().clone();
    let my_port = cluster.my_port.load(std::sync::atomic::Ordering::Relaxed) as i64;

    let mut result = Vec::new();
    let mut start_slot: i64 = -1;
    let mut current_owner_idx: u32 = 0;

    // Use atomic slot owner table for faster iteration
    for slot in 0..CLUSTER_SLOTS {
        let owner_idx = cluster.get_slot_owner_index(slot as u16);

        if owner_idx != current_owner_idx {
            // Emit previous range if it had an owner
            if start_slot >= 0 && current_owner_idx != 0 {
                let (ip, port, id) = if current_owner_idx == 1 {
                    (my_ip.clone(), my_port, my_id.clone())
                } else if let Some(owner_id) = cluster.get_slot_owner(start_slot as u16) {
                    if let Some(node) = cluster.nodes.get(&owner_id) {
                        (node.ip.clone(), node.port as i64, owner_id)
                    } else {
                        ("".to_string(), 0, owner_id)
                    }
                } else {
                    ("".to_string(), 0, Bytes::new())
                };

                if !id.is_empty() {
                    let slot_info = vec![
                        RespValue::integer(start_slot),
                        RespValue::integer((slot - 1) as i64),
                        RespValue::array(vec![
                            RespValue::bulk_string(&ip),
                            RespValue::integer(port),
                            RespValue::bulk(id),
                        ]),
                    ];
                    result.push(RespValue::array(slot_info));
                }
            }
            start_slot = slot as i64;
            current_owner_idx = owner_idx;
        } else if start_slot < 0 && owner_idx != 0 {
            start_slot = slot as i64;
            current_owner_idx = owner_idx;
        }
    }

    // Emit final range
    if start_slot >= 0 && current_owner_idx != 0 {
        let (ip, port, id) = if current_owner_idx == 1 {
            (my_ip.clone(), my_port, my_id.clone())
        } else if let Some(owner_id) = cluster.get_slot_owner(start_slot as u16) {
            if let Some(node) = cluster.nodes.get(&owner_id) {
                (node.ip.clone(), node.port as i64, owner_id)
            } else {
                ("".to_string(), 0, owner_id)
            }
        } else {
            ("".to_string(), 0, Bytes::new())
        };

        if !id.is_empty() {
            let slot_info = vec![
                RespValue::integer(start_slot),
                RespValue::integer(16383),
                RespValue::array(vec![
                    RespValue::bulk_string(&ip),
                    RespValue::integer(port),
                    RespValue::bulk(id),
                ]),
            ];
            result.push(RespValue::array(slot_info));
        }
    }

    Ok(RespValue::array(result))
}

/// CLUSTER HELP
fn cmd_cluster_help() -> Result<RespValue> {
    let help = vec![
        "CLUSTER <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "ADDSLOTS <slot> [<slot> ...] -- Assign slots to current node.",
        "ADDSLOTSRANGE <start slot> <end slot> [<start slot> <end slot> ...] -- Assign slot ranges to current node.",
        "BUMPEPOCH -- Advance the cluster config epoch.",
        "COUNT-FAILURE-REPORTS <node-id> -- Return number of failure reports.",
        "COUNTKEYSINSLOT <slot> -- Return number of keys in slot.",
        "DELSLOTS <slot> [<slot> ...] -- Delete slots from current node.",
        "DELSLOTSRANGE <start slot> <end slot> [<start slot> <end slot> ...] -- Delete slot ranges from current node.",
        "FAILOVER [FORCE|TAKEOVER] -- Promote current replica to master.",
        "FLUSHSLOTS -- Delete current node's slot info.",
        "FORGET <node-id> -- Remove node from cluster.",
        "GETKEYSINSLOT <slot> <count> -- Return keys in slot.",
        "INFO -- Return cluster info.",
        "KEYSLOT <key> -- Return slot for key.",
        "LINKS -- Return info about cluster links.",
        "MEET <ip> <port> [<bus-port>] -- Connect to node.",
        "MIGRATION IMPORT|CANCEL|STATUS ... -- Manage slot migrations.",
        "MYID -- Return this node's ID.",
        "MYSHARDID -- Return this node's shard ID.",
        "NODES -- Return cluster nodes.",
        "REPLICAS <node-id> -- Return replicas of node.",
        "REPLICATE <node-id> -- Configure as replica of node.",
        "RESET [HARD|SOFT] -- Reset cluster state.",
        "SAVECONFIG -- Force save cluster config.",
        "SET-CONFIG-EPOCH <epoch> -- Set config epoch.",
        "SETSLOT <slot> <IMPORTING|MIGRATING|NODE|STABLE> [<node-id>] -- Set slot state.",
        "SHARDS -- Return shards info.",
        "SLAVES <node-id> -- Return replicas (deprecated, use REPLICAS).",
        "SLOT-STATS SLOTSRANGE <start> <end> | ORDERBY <metric> ... -- Return slot stats.",
        "SLOTS -- Return slot-to-node mapping.",
        "HELP -- Show this help.",
    ];

    Ok(RespValue::array(
        help.into_iter()
            .map(|s| RespValue::SimpleString(Bytes::from(s)))
            .collect(),
    ))
}

// =============================================================================
// Helper functions
// =============================================================================

/// Parse slot number from argument - O(1)
#[inline]
fn parse_slot(arg: &Bytes) -> Result<u16> {
    let s = std::str::from_utf8(arg).map_err(|_| Error::NotInteger)?;
    let slot: i64 = s.parse().map_err(|_| Error::NotInteger)?;
    if slot < 0 || slot >= CLUSTER_SLOTS as i64 {
        return Err(Error::Custom(format!(
            "ERR Invalid slot {} - must be between 0 and {}",
            slot,
            CLUSTER_SLOTS - 1
        )));
    }
    Ok(slot as u16)
}

/// Parse integer from argument - O(1)
#[inline]
fn parse_int(arg: &Bytes) -> Result<i64> {
    let s = std::str::from_utf8(arg).map_err(|_| Error::NotInteger)?;
    s.parse().map_err(|_| Error::NotInteger)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_slot() {
        assert_eq!(parse_slot(&Bytes::from("0")).unwrap(), 0);
        assert_eq!(parse_slot(&Bytes::from("16383")).unwrap(), 16383);
        assert!(parse_slot(&Bytes::from("16384")).is_err());
        assert!(parse_slot(&Bytes::from("-1")).is_err());
    }

    #[test]
    fn test_keyslot() {
        let args = vec![Bytes::from("mykey")];
        let result = cmd_cluster_keyslot(&args).unwrap();
        if let RespValue::Integer(slot) = result {
            assert!(slot >= 0 && slot < CLUSTER_SLOTS as i64);
        } else {
            panic!("Expected integer");
        }
    }
}
