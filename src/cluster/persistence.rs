//! Cluster State Persistence - nodes.conf file handling
//!
//! Saves and loads cluster state to/from the nodes.conf file.
//! Format is Redis-compatible:
//! ```
//! <node-id> <ip:port@cport> <flags> <master-id|-0> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot>...
//! vars currentEpoch <epoch> lastVoteEpoch 0
//! ```

use bytes::Bytes;
use log::info;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::sync::atomic::Ordering;

use crate::cluster_state::{CLUSTER_SLOTS, ClusterState, NodeRole};
use crate::config::ServerConfig;

/// Save cluster state to nodes.conf file
pub fn save_nodes_conf(config: &ServerConfig, cluster: &ClusterState) -> std::io::Result<()> {
    let path = Path::new(&config.dir).join(&config.cluster_config_file);

    // Write to temp file first, then atomically rename
    let temp_path = path.with_extension("conf.tmp");

    let mut file = File::create(&temp_path)?;

    // Write self node first
    let my_ip = cluster.my_ip.read().clone();
    let my_port = cluster.my_port.load(Ordering::Relaxed) as u16;
    let my_cport = if config.cluster_port > 0 {
        config.cluster_port
    } else {
        my_port + 10000
    };

    // Format: <id> <ip:port@cport,hostname> <flags> <master> <ping> <pong> <epoch> <link> [slots]
    let my_slots = cluster.slots.get_ranges();
    let slots_str = format_slot_ranges(&my_slots);

    writeln!(
        file,
        "{} {}:{}@{} myself,master - 0 {} {} connected{}",
        String::from_utf8_lossy(&cluster.my_id),
        my_ip,
        my_port,
        my_cport,
        cluster.config_epoch.load(Ordering::Relaxed),
        cluster.current_epoch.load(Ordering::Relaxed),
        if slots_str.is_empty() {
            String::new()
        } else {
            format!(" {}", slots_str)
        }
    )?;

    // Write other nodes
    for node_entry in cluster.nodes.iter() {
        let node = node_entry.value();
        let flags = node.flags.format();
        let master_id = node
            .master_id
            .read()
            .as_ref()
            .map(|id| String::from_utf8_lossy(id).to_string())
            .unwrap_or_else(|| "-".to_string());

        let link_state = if node.link_state.load(Ordering::Relaxed) == 1 {
            "connected"
        } else {
            "disconnected"
        };

        let node_slots = node.slots.get_ranges();
        let node_slots_str = format_slot_ranges(&node_slots);

        writeln!(
            file,
            "{} {}:{}@{} {} {} {} {} {} {}{}",
            String::from_utf8_lossy(&node.id),
            node.ip,
            node.port,
            node.cport,
            flags,
            master_id,
            node.ping_sent.load(Ordering::Relaxed),
            node.pong_recv.load(Ordering::Relaxed),
            node.config_epoch.load(Ordering::Relaxed),
            link_state,
            if node_slots_str.is_empty() {
                String::new()
            } else {
                format!(" {}", node_slots_str)
            }
        )?;
    }

    // Write vars line
    writeln!(
        file,
        "vars currentEpoch {} lastVoteEpoch 0",
        cluster.current_epoch.load(Ordering::Relaxed)
    )?;

    // Sync and rename
    file.sync_all()?;
    drop(file);

    fs::rename(&temp_path, &path)?;

    info!("Saved cluster state to {:?}", path);
    Ok(())
}

/// Load cluster state from nodes.conf file
pub fn load_nodes_conf(config: &ServerConfig, cluster: &ClusterState) -> std::io::Result<bool> {
    let path = Path::new(&config.dir).join(&config.cluster_config_file);

    if !path.exists() {
        info!("No cluster config file at {:?}, starting fresh", path);
        return Ok(false);
    }

    let file = File::open(&path)?;
    let reader = BufReader::new(file);

    let mut loaded_my_id = false;

    for line in reader.lines() {
        let line = line?;
        let line = line.trim();

        if line.is_empty() {
            continue;
        }

        // Parse vars line
        if line.starts_with("vars ") {
            parse_vars_line(line, cluster);
            continue;
        }

        // Parse node line
        if let Some((
            node_id,
            is_myself,
            ip,
            port,
            cport,
            flags_str,
            master_id,
            ping,
            pong,
            epoch,
            slots,
        )) = parse_node_line(line)
        {
            if is_myself {
                // This is our node - just load the ID and epoch
                // Note: in real Redis, it would validate the ID matches our current ID
                // For now we'll trust the file
                if !loaded_my_id {
                    // We keep our generated ID, but load the epoch
                    cluster.config_epoch.store(epoch, Ordering::Relaxed);
                    loaded_my_id = true;
                }

                // Add slots for self
                for (start, end) in &slots {
                    let _ = cluster.add_slot_range(*start, *end);
                }
            } else {
                // Add other node
                cluster.add_node_from_gossip(Bytes::from(node_id.clone()), ip.clone(), port, cport);

                // Update node state
                if let Some(node) = cluster.get_node(&Bytes::from(node_id.clone())) {
                    node.ping_sent.store(ping, Ordering::Relaxed);
                    node.pong_recv.store(pong, Ordering::Relaxed);
                    node.config_epoch.store(epoch, Ordering::Relaxed);

                    // Parse flags
                    if flags_str.contains("slave") {
                        node.set_role(NodeRole::Replica);
                        node.flags.set_slave(true);
                        if master_id != "-" {
                            *node.master_id.write() = Some(Bytes::from(master_id.clone()));
                        }
                    }
                    if flags_str.contains("fail?") {
                        node.flags.set_pfail(true);
                    }
                    if flags_str.contains("fail") && !flags_str.contains("fail?") {
                        node.flags.set_fail(true);
                    }
                    if flags_str.contains("nofailover") {
                        node.flags.set_nofailover(true);
                    }
                }
            }
        }
    }

    info!("Loaded cluster state from {:?}", path);
    Ok(true)
}

/// Format slot ranges for nodes.conf output
fn format_slot_ranges(ranges: &[(u16, u16)]) -> String {
    ranges
        .iter()
        .map(|(start, end)| {
            if start == end {
                format!("{}", start)
            } else {
                format!("{}-{}", start, end)
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Parse vars line
fn parse_vars_line(line: &str, cluster: &ClusterState) {
    let parts: Vec<&str> = line.split_whitespace().collect();
    for i in 0..parts.len() {
        if parts[i] == "currentEpoch"
            && i + 1 < parts.len()
            && let Ok(epoch) = parts[i + 1].parse::<u64>()
        {
            cluster.current_epoch.store(epoch, Ordering::Relaxed);
        }
    }
}

/// Parse a node line from nodes.conf
/// Returns: (node_id, is_myself, ip, port, cport, flags, master_id, ping, pong, epoch, slots)
fn parse_node_line(
    line: &str,
) -> Option<(
    String,
    bool,
    String,
    u16,
    u16,
    String,
    String,
    u64,
    u64,
    u64,
    Vec<(u16, u16)>,
)> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() < 9 {
        return None;
    }

    let node_id = parts[0].to_string();

    // Parse ip:port@cport (may include ,hostname)
    let addr = parts[1].split(',').next().unwrap_or(parts[1]);
    let (ip_port, cport_str) = addr.split_once('@').unwrap_or((addr, ""));
    let (ip, port_str) = ip_port.rsplit_once(':').unwrap_or((ip_port, "6379"));

    let port: u16 = port_str.parse().ok()?;
    let cport: u16 = if cport_str.is_empty() {
        port + 10000
    } else {
        cport_str.parse().ok()?
    };

    let flags_str = parts[2].to_string();
    let is_myself = flags_str.contains("myself");

    let master_id = parts[3].to_string();
    let ping: u64 = parts[4].parse().unwrap_or(0);
    let pong: u64 = parts[5].parse().unwrap_or(0);
    let epoch: u64 = parts[6].parse().unwrap_or(0);
    // parts[7] is link state - we don't need to parse it

    // Parse slots (parts 8+)
    let mut slots = Vec::new();
    for part in parts.iter().skip(8) {
        // Skip non-slot parts (like "connected", "disconnected")
        if part.starts_with('[')
            || part
                .chars()
                .next()
                .map(|c| !c.is_ascii_digit())
                .unwrap_or(true)
        {
            continue;
        }

        if let Some((start_str, end_str)) = part.split_once('-') {
            if let (Ok(start), Ok(end)) = (start_str.parse::<u16>(), end_str.parse::<u16>())
                && start <= end
                && end < CLUSTER_SLOTS as u16
            {
                slots.push((start, end));
            }
        } else if let Ok(slot) = part.parse::<u16>()
            && slot < CLUSTER_SLOTS as u16
        {
            slots.push((slot, slot));
        }
    }

    Some((
        node_id,
        is_myself,
        ip.to_string(),
        port,
        cport,
        flags_str,
        master_id,
        ping,
        pong,
        epoch,
        slots,
    ))
}
