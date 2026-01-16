//! Sentinel Configuration Parser
//!
//! Parses sentinel.conf files with Redis Sentinel-specific directives.

use std::fs;
use std::path::Path;

use crate::sentinel::config::{MasterConfig, SentinelConfig};

/// Parse a sentinel configuration file
pub fn parse_sentinel_config(path: &Path) -> Result<SentinelConfig, String> {
    let content = fs::read_to_string(path)
        .map_err(|e| format!("Failed to read sentinel config file: {}", e))?;

    let mut config = SentinelConfig::default();

    for (line_num, line) in content.lines().enumerate() {
        let line = line.trim();

        // Skip empty lines and comments
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        // Parse the line
        if let Err(e) = parse_sentinel_line(line, &mut config) {
            eprintln!("Warning: line {}: {}", line_num + 1, e);
        }
    }

    Ok(config)
}

/// Parse a single configuration line
fn parse_sentinel_line(line: &str, config: &mut SentinelConfig) -> Result<(), String> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.is_empty() {
        return Ok(());
    }

    let directive = parts[0].to_lowercase();

    match directive.as_str() {
        "port" => {
            if parts.len() < 2 {
                return Err("port directive requires a value".to_string());
            }
            config.port = parts[1]
                .parse()
                .map_err(|_| "invalid port number".to_string())?;
        }
        "bind" => {
            if parts.len() < 2 {
                return Err("bind directive requires an address".to_string());
            }
            config.bind = parts[1..].iter().map(|s| s.to_string()).collect();
        }
        "protected-mode" => {
            if parts.len() < 2 {
                return Err("protected-mode requires a value".to_string());
            }
            config.protected_mode = parse_bool(parts[1])?;
        }
        "dir" => {
            if parts.len() < 2 {
                return Err("dir directive requires a path".to_string());
            }
            config.dir = parts[1].to_string();
        }
        "logfile" => {
            if parts.len() < 2 {
                return Err("logfile directive requires a path".to_string());
            }
            config.logfile = parts[1].to_string();
        }
        "loglevel" => {
            if parts.len() < 2 {
                return Err("loglevel directive requires a value".to_string());
            }
            config.loglevel = parts[1].to_string();
        }
        "daemonize" => {
            if parts.len() < 2 {
                return Err("daemonize requires a value".to_string());
            }
            config.daemonize = parse_bool(parts[1])?;
        }
        "pidfile" => {
            if parts.len() < 2 {
                return Err("pidfile directive requires a path".to_string());
            }
            config.pidfile = parts[1].to_string();
        }
        "sentinel" => {
            if parts.len() < 2 {
                return Err("sentinel directive requires a subcommand".to_string());
            }
            parse_sentinel_directive(&parts[1..], config)?;
        }
        "acllog-max-len" => {
            if parts.len() < 2 {
                return Err("acllog-max-len requires a value".to_string());
            }
            config.acllog_max_len = parts[1]
                .parse()
                .map_err(|_| "invalid acllog-max-len".to_string())?;
        }
        _ => {
            // Ignore unknown directives for compatibility
        }
    }

    Ok(())
}

/// Parse SENTINEL-specific directives
fn parse_sentinel_directive(parts: &[&str], config: &mut SentinelConfig) -> Result<(), String> {
    if parts.is_empty() {
        return Err("sentinel directive requires a subcommand".to_string());
    }

    let subcommand = parts[0].to_lowercase();

    match subcommand.as_str() {
        "monitor" => {
            // sentinel monitor <name> <ip> <port> <quorum>
            if parts.len() < 5 {
                return Err("sentinel monitor requires: name ip port quorum".to_string());
            }

            let name = parts[1].to_string();
            let ip = parts[2].to_string();
            let port: u16 = parts[3]
                .parse()
                .map_err(|_| "invalid port in sentinel monitor".to_string())?;
            let quorum: u32 = parts[4]
                .parse()
                .map_err(|_| "invalid quorum in sentinel monitor".to_string())?;

            let master = MasterConfig::new(name, ip, port, quorum);
            config.add_master(master);
        }
        "down-after-milliseconds" => {
            // sentinel down-after-milliseconds <name> <milliseconds>
            if parts.len() < 3 {
                return Err(
                    "sentinel down-after-milliseconds requires name and milliseconds".to_string(),
                );
            }

            let name = parts[1];
            let ms: u64 = parts[2]
                .parse()
                .map_err(|_| "invalid milliseconds".to_string())?;

            if let Some(master) = config.get_master_mut(name) {
                master.down_after_ms = ms;
            }
        }
        "parallel-syncs" => {
            // sentinel parallel-syncs <name> <count>
            if parts.len() < 3 {
                return Err("sentinel parallel-syncs requires name and count".to_string());
            }

            let name = parts[1];
            let count: u32 = parts[2]
                .parse()
                .map_err(|_| "invalid parallel-syncs count".to_string())?;

            if let Some(master) = config.get_master_mut(name) {
                master.parallel_syncs = count;
            }
        }
        "failover-timeout" => {
            // sentinel failover-timeout <name> <milliseconds>
            if parts.len() < 3 {
                return Err("sentinel failover-timeout requires name and milliseconds".to_string());
            }

            let name = parts[1];
            let ms: u64 = parts[2]
                .parse()
                .map_err(|_| "invalid failover-timeout".to_string())?;

            if let Some(master) = config.get_master_mut(name) {
                master.failover_timeout = ms;
            }
        }
        "auth-pass" => {
            // sentinel auth-pass <name> <password>
            if parts.len() < 3 {
                return Err("sentinel auth-pass requires name and password".to_string());
            }

            let name = parts[1];
            let password = parts[2].to_string();

            if let Some(master) = config.get_master_mut(name) {
                master.auth_pass = Some(password);
            }
        }
        "auth-user" => {
            // sentinel auth-user <name> <username>
            if parts.len() < 3 {
                return Err("sentinel auth-user requires name and username".to_string());
            }

            let name = parts[1];
            let username = parts[2].to_string();

            if let Some(master) = config.get_master_mut(name) {
                master.auth_user = Some(username);
            }
        }
        "announce-ip" => {
            // sentinel announce-ip <ip>
            if parts.len() < 2 {
                return Err("sentinel announce-ip requires an IP address".to_string());
            }
            config.announce_ip = Some(parts[1].to_string());
        }
        "announce-port" => {
            // sentinel announce-port <port>
            if parts.len() < 2 {
                return Err("sentinel announce-port requires a port number".to_string());
            }
            let port: u16 = parts[1]
                .parse()
                .map_err(|_| "invalid announce-port".to_string())?;
            config.announce_port = Some(port);
        }
        "resolve-hostnames" => {
            // sentinel resolve-hostnames yes|no
            if parts.len() < 2 {
                return Err("sentinel resolve-hostnames requires yes or no".to_string());
            }
            config.resolve_hostnames = parse_bool(parts[1])?;
        }
        "announce-hostnames" => {
            // sentinel announce-hostnames yes|no
            if parts.len() < 2 {
                return Err("sentinel announce-hostnames requires yes or no".to_string());
            }
            config.announce_hostnames = parse_bool(parts[1])?;
        }
        "deny-scripts-reconfig" => {
            // sentinel deny-scripts-reconfig yes|no
            if parts.len() < 2 {
                return Err("sentinel deny-scripts-reconfig requires yes or no".to_string());
            }
            config.deny_scripts_reconfig = parse_bool(parts[1])?;
        }
        "master-reboot-down-after-period" => {
            // sentinel master-reboot-down-after-period <name> <milliseconds>
            if parts.len() < 3 {
                return Err(
                    "sentinel master-reboot-down-after-period requires name and milliseconds"
                        .to_string(),
                );
            }

            let name = parts[1];
            let ms: u64 = parts[2]
                .parse()
                .map_err(|_| "invalid master-reboot-down-after-period".to_string())?;

            if let Some(master) = config.get_master_mut(name) {
                master.master_reboot_down_after_period = ms;
            }
        }
        "sentinel-user" => {
            // sentinel sentinel-user <username>
            if parts.len() < 2 {
                return Err("sentinel sentinel-user requires a username".to_string());
            }
            config.sentinel_user = Some(parts[1].to_string());
        }
        "sentinel-pass" => {
            // sentinel sentinel-pass <password>
            if parts.len() < 2 {
                return Err("sentinel sentinel-pass requires a password".to_string());
            }
            config.sentinel_pass = Some(parts[1].to_string());
        }
        _ => {
            // Ignore unknown sentinel directives for compatibility
        }
    }

    Ok(())
}

/// Parse boolean values (yes/no, true/false, 1/0)
fn parse_bool(s: &str) -> Result<bool, String> {
    match s.to_lowercase().as_str() {
        "yes" | "true" | "1" => Ok(true),
        "no" | "false" | "0" => Ok(false),
        _ => Err(format!("invalid boolean value: {}", s)),
    }
}

/// Detect if a config file is a sentinel config
pub fn is_sentinel_config(path: &Path) -> bool {
    // Check filename
    if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
        if filename.contains("sentinel") {
            return true;
        }
    }

    // Check file content for SENTINEL directives
    if let Ok(content) = fs::read_to_string(path) {
        for line in content.lines() {
            let line = line.trim();
            if line.starts_with("sentinel ") || line.starts_with("SENTINEL ") {
                return true;
            }
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_sentinel_monitor() {
        let mut config = SentinelConfig::default();
        parse_sentinel_directive(
            &["monitor", "mymaster", "127.0.0.1", "6379", "2"],
            &mut config,
        )
        .unwrap();

        let master = config.get_master("mymaster").unwrap();
        assert_eq!(master.ip, "127.0.0.1");
        assert_eq!(master.port, 6379);
        assert_eq!(master.quorum, 2);
    }

    #[test]
    fn test_parse_bool() {
        assert_eq!(parse_bool("yes").unwrap(), true);
        assert_eq!(parse_bool("no").unwrap(), false);
        assert_eq!(parse_bool("true").unwrap(), true);
        assert_eq!(parse_bool("false").unwrap(), false);
        assert_eq!(parse_bool("1").unwrap(), true);
        assert_eq!(parse_bool("0").unwrap(), false);
        assert!(parse_bool("maybe").is_err());
    }

    #[test]
    fn test_is_sentinel_config() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "port 26379").unwrap();
        writeln!(file, "sentinel monitor mymaster 127.0.0.1 6379 2").unwrap();
        file.flush().unwrap();

        assert!(is_sentinel_config(file.path()));
    }
}
