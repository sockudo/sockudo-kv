use crate::config::{MaxMemoryPolicy, ServerConfig};
use clap::Parser;

use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

#[derive(Parser, Debug)]
#[command(name = "sockudo-kv")]
#[command(author = "Sockudo Team")]
#[command(version = "0.2.0")]
#[command(about = "A high-performance Redis-compatible key-value store", long_about = None)]
pub struct Cli {
    /// Configuration file path
    #[arg(value_name = "CONFIG_FILE")]
    pub config_file: Option<PathBuf>,

    /// Server port (default: 6379)
    #[arg(long)]
    pub port: Option<u16>,

    /// Bind address (default: 127.0.0.1)
    #[arg(long)]
    pub bind: Option<String>,

    /// Number of databases (default: 16)
    #[arg(long)]
    pub databases: Option<usize>,

    /// Run as daemon
    #[arg(long)]
    pub daemonize: Option<bool>,

    /// Log level (debug, verbose, notice, warning)
    #[arg(long)]
    pub loglevel: Option<String>,

    /// Log file path
    #[arg(long)]
    pub logfile: Option<String>,

    /// Password required for access
    #[arg(long)]
    pub requirepass: Option<String>,

    /// Protected mode (yes/no)
    #[arg(long)]
    pub protected_mode: Option<String>,

    /// Max memory limit (e.g., 100mb, 1gb)
    #[arg(long)]
    pub maxmemory: Option<String>,

    /// Max memory policy
    #[arg(long)]
    pub maxmemory_policy: Option<String>,

    /// Append Only File (yes/no)
    #[arg(long)]
    pub appendonly: Option<String>,

    /// RDB filename
    #[arg(long)]
    pub dbfilename: Option<String>,

    /// Working directory
    #[arg(long)]
    pub dir: Option<String>,

    /// Replica of master (host port)
    #[arg(long, num_args = 2, value_names = ["HOST", "PORT"])]
    pub replicaof: Option<Vec<String>>,
}

impl Cli {
    pub fn load_config() -> Result<ServerConfig, String> {
        let cli = Cli::parse();
        let mut config = ServerConfig::default();

        // 1. Load from config file if provided
        if let Some(path) = &cli.config_file {
            parse_config_file(path, &mut config)?;
        }

        // 2. Override with CLI arguments
        if let Some(port) = cli.port {
            config.port = port;
        }
        if let Some(bind) = cli.bind {
            config.bind = bind;
        }
        if let Some(dbs) = cli.databases {
            config.databases = dbs;
        }
        if let Some(daemonize) = cli.daemonize {
            config.daemonize = daemonize;
        }
        if let Some(loglevel) = cli.loglevel {
            config.loglevel = loglevel;
        }
        if let Some(logfile) = cli.logfile {
            config.logfile = logfile;
        }
        if let Some(pass) = cli.requirepass {
            config.requirepass = Some(pass);
        }
        if let Some(pm) = cli.protected_mode {
            config.protected_mode = parse_bool(&pm).unwrap_or(true);
        }
        if let Some(mem) = cli.maxmemory {
            config.maxmemory =
                parse_memory(&mem).map_err(|e| format!("Invalid maxmemory: {}", e))?;
        }
        if let Some(pol) = cli.maxmemory_policy {
            config.maxmemory_policy = MaxMemoryPolicy::from_str(&pol)?;
        }
        if let Some(aof) = cli.appendonly {
            config.appendonly = parse_bool(&aof).unwrap_or(false);
        }
        if let Some(dbfile) = cli.dbfilename {
            config.dbfilename = dbfile;
        }
        if let Some(d) = cli.dir {
            config.dir = d;
        }
        if let Some(args) = cli.replicaof
            && args.len() == 2 {
                let port = args[1]
                    .parse::<u16>()
                    .map_err(|_| "Invalid replicaof port")?;
                config.replicaof = Some((args[0].clone(), port));
            }

        Ok(config)
    }
}

fn parse_config_file(path: &Path, config: &mut ServerConfig) -> Result<(), String> {
    let content =
        fs::read_to_string(path).map_err(|e| format!("Failed to read config file: {}", e))?;

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }

        match parts[0].to_lowercase().as_str() {
            "port" if parts.len() > 1 => {
                config.port = parts[1].parse().map_err(|_| "Invalid port")?;
            }
            "bind" if parts.len() > 1 => {
                // Redis supports multiple bind addresses, we just take all of them joined
                config.bind = parts[1..].join(" ");
            }
            "databases" if parts.len() > 1 => {
                config.databases = parts[1].parse().map_err(|_| "Invalid databases count")?;
            }
            "daemonize" if parts.len() > 1 => {
                config.daemonize = parse_bool(parts[1])?;
            }
            "loglevel" if parts.len() > 1 => {
                config.loglevel = parts[1].to_string();
            }
            "logfile" if parts.len() > 1 => {
                config.logfile = parts[1].trim_matches('"').to_string();
            }
            "requirepass" if parts.len() > 1 => {
                config.requirepass = Some(parts[1].trim_matches('"').to_string());
            }
            "protected-mode" if parts.len() > 1 => {
                config.protected_mode = parse_bool(parts[1])?;
            }
            "maxmemory" if parts.len() > 1 => {
                config.maxmemory = parse_memory(parts[1])?;
            }
            "maxmemory-policy" if parts.len() > 1 => {
                config.maxmemory_policy = MaxMemoryPolicy::from_str(parts[1])?;
            }
            "appendonly" if parts.len() > 1 => {
                config.appendonly = parse_bool(parts[1])?;
            }
            "dbfilename" if parts.len() > 1 => {
                config.dbfilename = parts[1].trim_matches('"').to_string();
            }
            "dir" if parts.len() > 1 => {
                config.dir = parts[1].trim_matches('"').to_string();
            }
            "replicaof" if parts.len() > 2 => {
                let port = parts[2].parse().map_err(|_| "Invalid replicaof port")?;
                config.replicaof = Some((parts[1].to_string(), port));
            }
            "save" => {
                // Handle "save 3600 1" or empty "save" to disable
                if parts.len() == 1 || (parts.len() == 2 && parts[1] == "\"\"") {
                    config.save_points.clear();
                } else if parts.len() >= 3 {
                    // We need to append to save points or clear default if this is the first one found?
                    // Redis behavior: if multiple save directives, they accumulate?
                    // Actually usually 'save' directive overwrites defaults.
                    // But for simplification, let's just clear defaults if we see any 'save' directive in file?
                    // A better approach: if we see 'save' directives, we should probably start with empty list.
                    // But implementing full stateful parser is complex.
                    // Let's assume we just parse simple lines.
                    // For now: single line "save 3600 1 300 100" is also valid in some redis versions?
                    // Redis conf example says: "save <seconds> <changes>"
                    // So we parse pairs.
                    if config.save_points == ServerConfig::default().save_points {
                        // Clear defaults on first valid save directive found in file
                        config.save_points.clear();
                    }
                    let sec = parts[1].parse().map_err(|_| "Invalid save seconds")?;
                    let changes = parts[2].parse().map_err(|_| "Invalid save changes")?;
                    config.save_points.push((sec, changes));
                }
            }
            "include" if parts.len() > 1 => {
                // Recursive include
                let include_path = PathBuf::from(parts[1].trim_matches('"'));
                if include_path.exists() {
                    parse_config_file(&include_path, config)?;
                }
            }
            _ => {
                // Ignore unknown directives
            }
        }
    }
    Ok(())
}

fn parse_bool(s: &str) -> Result<bool, String> {
    match s.to_lowercase().as_str() {
        "yes" | "true" | "1" => Ok(true),
        "no" | "false" | "0" => Ok(false),
        _ => Err(format!("Invalid boolean: {}", s)),
    }
}

fn parse_memory(s: &str) -> Result<u64, String> {
    let s = s.to_lowercase();
    let (num, unit) = if s.ends_with("gb") {
        (s.trim_end_matches("gb"), 1024 * 1024 * 1024)
    } else if s.ends_with("mb") {
        (s.trim_end_matches("mb"), 1024 * 1024)
    } else if s.ends_with("kb") {
        (s.trim_end_matches("kb"), 1024)
    } else if s.ends_with('g') {
        (s.trim_end_matches('g'), 1024 * 1024 * 1024)
    } else if s.ends_with('m') {
        (s.trim_end_matches('m'), 1024 * 1024)
    } else if s.ends_with('k') {
        (s.trim_end_matches('k'), 1024)
    } else if s.ends_with('b') {
        (s.trim_end_matches('b'), 1)
    } else {
        (s.as_str(), 1)
    };

    num.parse::<u64>()
        .map(|n| n * unit)
        .map_err(|_| format!("Invalid memory value: {}", s))
}
