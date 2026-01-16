//! Kernel Tuning Module
//!
//! Linux-specific kernel parameter tuning for Redis-compatible behavior.
//! - OOM score adjustment via /proc/self/oom_score_adj
//! - Transparent Huge Pages (THP) disabling

use crate::config::ServerConfig;

/// Apply kernel tuning settings based on configuration
/// This is a no-op on non-Linux platforms.
pub fn apply_kernel_tuning(config: &ServerConfig) {
    apply_oom_score_adj(config);
    apply_disable_thp(config);
}

/// Apply OOM score adjustment
///
/// Redis supports several modes:
/// - "no": Don't modify OOM score
/// - "yes" or "relative": Apply relative adjustments from oom_score_adj_values
/// - "absolute": Apply absolute OOM score value
#[cfg(target_os = "linux")]
fn apply_oom_score_adj(config: &ServerConfig) {
    use std::fs;
    use std::io::Write;

    let mode = config.oom_score_adj.to_lowercase();
    if mode == "no" {
        return;
    }

    let score = match mode.as_str() {
        "yes" | "relative" => {
            // Use the first value (master) from the triple (master, replica, bgsave)
            config.oom_score_adj_values.0
        }
        "absolute" => config.oom_score_adj_values.0,
        _ => {
            log::warn!("Unknown oom-score-adj mode: {}, ignoring", mode);
            return;
        }
    };

    // Clamp to valid range (-1000 to 1000)
    let score = score.clamp(-1000, 1000);

    // Write to /proc/self/oom_score_adj
    match fs::OpenOptions::new()
        .write(true)
        .open("/proc/self/oom_score_adj")
    {
        Ok(mut file) => {
            if let Err(e) = writeln!(file, "{}", score) {
                log::warn!("Failed to write OOM score adj: {}", e);
            } else {
                log::info!("Set OOM score adjustment to {}", score);
            }
        }
        Err(e) => {
            log::warn!("Failed to open /proc/self/oom_score_adj: {}", e);
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn apply_oom_score_adj(config: &ServerConfig) {
    if config.oom_score_adj != "no" {
        log::debug!(
            "OOM score adjustment is only supported on Linux (configured: {})",
            config.oom_score_adj
        );
    }
}

/// Disable Transparent Huge Pages (THP)
///
/// THP can cause latency spikes in Redis-like workloads.
/// This tries to disable THP for the current process via:
/// - /sys/kernel/mm/transparent_hugepage/enabled
/// - /sys/kernel/mm/transparent_hugepage/defrag
#[cfg(target_os = "linux")]
fn apply_disable_thp(config: &ServerConfig) {
    use std::fs;
    use std::io::Write;

    if !config.disable_thp {
        return;
    }

    // Try to disable THP globally (requires root)
    let thp_paths = [
        "/sys/kernel/mm/transparent_hugepage/enabled",
        "/sys/kernel/mm/transparent_hugepage/defrag",
    ];

    for path in thp_paths {
        match fs::OpenOptions::new().write(true).open(path) {
            Ok(mut file) => {
                if let Err(e) = file.write_all(b"never") {
                    // This is expected to fail without root
                    log::debug!("Could not write to {}: {} (may require root)", path, e);
                } else {
                    log::info!("Disabled THP via {}", path);
                }
            }
            Err(e) => {
                log::debug!("Could not open {}: {} (may require root)", path, e);
            }
        }
    }

    // Try process-specific THP control via prctl if available
    apply_thp_prctl();
}

#[cfg(target_os = "linux")]
fn apply_thp_prctl() {
    // PR_SET_THP_DISABLE = 41
    const PR_SET_THP_DISABLE: libc::c_int = 41;

    // Safety: prctl with PR_SET_THP_DISABLE only affects the current process
    let result = unsafe { libc::prctl(PR_SET_THP_DISABLE, 1, 0, 0, 0) };

    if result == 0 {
        log::info!("Disabled THP for this process via prctl");
    } else {
        log::debug!(
            "Could not disable THP via prctl (errno: {})",
            std::io::Error::last_os_error()
        );
    }
}

#[cfg(not(target_os = "linux"))]
fn apply_disable_thp(config: &ServerConfig) {
    if config.disable_thp {
        log::debug!("THP disabling is only supported on Linux");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_apply_kernel_tuning_does_not_panic() {
        let config = ServerConfig::default();
        // Should not panic on any platform
        apply_kernel_tuning(&config);
    }
}
