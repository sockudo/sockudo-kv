//! Logging configuration module
//!
//! Implements Redis-compatible logging with support for:
//! - Log levels (debug, verbose, notice, warning, nothing)
//! - File-based logging
//! - Syslog (Unix only)
//! - Log sanitization for sensitive data

use crate::config::ServerConfig;
use log::{LevelFilter, Log, Metadata, Record};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::Mutex;

/// Redis-style log levels mapped to Rust log levels
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedisLogLevel {
    Debug,
    Verbose,
    Notice,
    Warning,
    Nothing,
}

impl RedisLogLevel {
    /// Parse Redis-style log level string
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "debug" => Self::Debug,
            "verbose" => Self::Verbose,
            "notice" => Self::Notice,
            "warning" => Self::Warning,
            "nothing" => Self::Nothing,
            _ => Self::Notice, // Default
        }
    }

    /// Convert to Rust log LevelFilter
    pub fn to_level_filter(self) -> LevelFilter {
        match self {
            Self::Debug => LevelFilter::Debug,
            Self::Verbose => LevelFilter::Info, // Redis "verbose" ≈ info
            Self::Notice => LevelFilter::Info,  // Redis "notice" ≈ info (default)
            Self::Warning => LevelFilter::Warn,
            Self::Nothing => LevelFilter::Off,
        }
    }
}

/// Syslog facility for Unix systems
#[cfg(unix)]
#[derive(Debug, Clone, Copy)]
pub enum SyslogFacility {
    User,
    Local0,
    Local1,
    Local2,
    Local3,
    Local4,
    Local5,
    Local6,
    Local7,
}

#[cfg(unix)]
impl SyslogFacility {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "user" => Self::User,
            "local0" => Self::Local0,
            "local1" => Self::Local1,
            "local2" => Self::Local2,
            "local3" => Self::Local3,
            "local4" => Self::Local4,
            "local5" => Self::Local5,
            "local6" => Self::Local6,
            "local7" => Self::Local7,
            _ => Self::Local0,
        }
    }

    /// Convert to syslog facility code (LOG_LOCAL0 = 16, etc.)
    pub fn to_code(self) -> i32 {
        match self {
            Self::User => 1 << 3,    // LOG_USER
            Self::Local0 => 16 << 3, // LOG_LOCAL0
            Self::Local1 => 17 << 3,
            Self::Local2 => 18 << 3,
            Self::Local3 => 19 << 3,
            Self::Local4 => 20 << 3,
            Self::Local5 => 21 << 3,
            Self::Local6 => 22 << 3,
            Self::Local7 => 23 << 3,
        }
    }
}

/// Combined logger that can write to file and/or stdout
pub struct SockudoLogger {
    level: LevelFilter,
    file: Option<Mutex<File>>,
    hide_user_data: bool,
    #[cfg(unix)]
    syslog_enabled: bool,
    #[cfg(unix)]
    syslog_ident: String,
}

impl SockudoLogger {
    /// Create a new logger from config
    pub fn new(config: &ServerConfig) -> Self {
        let level = RedisLogLevel::from_str(&config.loglevel).to_level_filter();

        // Open log file if specified
        let file = if !config.logfile.is_empty() {
            match OpenOptions::new()
                .create(true)
                .append(true)
                .open(&config.logfile)
            {
                Ok(f) => Some(Mutex::new(f)),
                Err(e) => {
                    eprintln!(
                        "Warning: Failed to open log file '{}': {}",
                        config.logfile, e
                    );
                    None
                }
            }
        } else {
            None
        };

        Self {
            level,
            file,
            hide_user_data: config.hide_user_data_from_log,
            #[cfg(unix)]
            syslog_enabled: config.syslog_enabled,
            #[cfg(unix)]
            syslog_ident: config.syslog_ident.clone(),
        }
    }

    /// Sanitize log message to hide sensitive data
    fn sanitize_message(&self, msg: &str) -> String {
        if !self.hide_user_data {
            return msg.to_string();
        }

        // Patterns to redact (passwords, auth tokens, keys)
        let mut result = msg.to_string();

        // Redact AUTH command arguments
        if result.to_uppercase().contains("AUTH") {
            // Replace password after AUTH
            let re_auth = regex_lite_replace(&result, r"(?i)AUTH\s+\S+", "AUTH [REDACTED]");
            result = re_auth;
        }

        // Redact CONFIG SET requirepass
        if result.to_uppercase().contains("REQUIREPASS") {
            let re_pass =
                regex_lite_replace(&result, r"(?i)requirepass\s+\S+", "requirepass [REDACTED]");
            result = re_pass;
        }

        result
    }

    /// Format log record as Redis-style output
    fn format_record(&self, record: &Record) -> String {
        let now = chrono_lite_now();
        let level_char = match record.level() {
            log::Level::Error => '!',
            log::Level::Warn => '#',
            log::Level::Info => '*',
            log::Level::Debug => '-',
            log::Level::Trace => '.',
        };

        let msg = self.sanitize_message(&record.args().to_string());
        format!("{}:{} {} {}\n", std::process::id(), level_char, now, msg)
    }

    #[cfg(unix)]
    fn write_syslog(&self, record: &Record) {
        if !self.syslog_enabled {
            return;
        }

        use std::ffi::CString;

        let priority = match record.level() {
            log::Level::Error => 3,                     // LOG_ERR
            log::Level::Warn => 4,                      // LOG_WARNING
            log::Level::Info => 6,                      // LOG_INFO
            log::Level::Debug | log::Level::Trace => 7, // LOG_DEBUG
        };

        let msg = self.sanitize_message(&record.args().to_string());
        if let Ok(c_msg) = CString::new(msg) {
            unsafe {
                // Use libc syslog
                let ident = CString::new(self.syslog_ident.as_str()).unwrap_or_default();
                libc::openlog(ident.as_ptr(), libc::LOG_PID, libc::LOG_LOCAL0);
                libc::syslog(priority, c_msg.as_ptr());
            }
        }
    }
}

impl Log for SockudoLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= self.level
    }

    fn log(&self, record: &Record) {
        if !self.enabled(record.metadata()) {
            return;
        }

        let formatted = self.format_record(record);

        // Write to file if configured
        if let Some(ref file) = self.file {
            if let Ok(mut f) = file.lock() {
                let _ = f.write_all(formatted.as_bytes());
            }
        } else {
            // Write to stderr (like Redis)
            eprint!("{}", formatted);
        }

        // Write to syslog on Unix
        #[cfg(unix)]
        self.write_syslog(record);
    }

    fn flush(&self) {
        if let Some(ref file) = self.file {
            if let Ok(mut f) = file.lock() {
                let _ = f.flush();
            }
        }
    }
}

/// Simple regex replacement without full regex crate
fn regex_lite_replace(input: &str, pattern: &str, _replacement: &str) -> String {
    // Simple pattern matching for our specific use cases
    if pattern.contains("AUTH") {
        // Find AUTH followed by space and word
        if let Some(idx) = input.to_uppercase().find("AUTH") {
            let before = &input[..idx];
            let after = &input[idx..];
            if let Some(space_idx) = after.find(' ') {
                let rest = &after[space_idx + 1..];
                if let Some(end_idx) = rest.find(|c: char| c.is_whitespace()) {
                    return format!("{}{} {} {}", before, "AUTH", "[REDACTED]", &rest[end_idx..]);
                } else {
                    return format!("{}{} {}", before, "AUTH", "[REDACTED]");
                }
            }
        }
    }
    if pattern.contains("requirepass") {
        if let Some(idx) = input.to_lowercase().find("requirepass") {
            let before = &input[..idx];
            let after = &input[idx..];
            if let Some(space_idx) = after.find(' ') {
                let rest = &after[space_idx + 1..];
                if let Some(end_idx) = rest.find(|c: char| c.is_whitespace()) {
                    return format!(
                        "{}{} {} {}",
                        before,
                        "requirepass",
                        "[REDACTED]",
                        &rest[end_idx..]
                    );
                } else {
                    return format!("{}{} {}", before, "requirepass", "[REDACTED]");
                }
            }
        }
    }
    input.to_string()
}

/// Simple timestamp without chrono crate
fn chrono_lite_now() -> String {
    use std::time::SystemTime;

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();

    let secs = now.as_secs();
    // Simple format: just seconds since epoch (could be improved)
    format!("{}", secs)
}

/// Initialize logging from server config
/// This replaces env_logger::init()
pub fn init_logging(config: &ServerConfig) -> Result<(), log::SetLoggerError> {
    let logger = Box::new(SockudoLogger::new(config));
    let level = RedisLogLevel::from_str(&config.loglevel).to_level_filter();

    log::set_boxed_logger(logger)?;
    log::set_max_level(level);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_level_parsing() {
        assert_eq!(
            RedisLogLevel::from_str("debug").to_level_filter(),
            LevelFilter::Debug
        );
        assert_eq!(
            RedisLogLevel::from_str("verbose").to_level_filter(),
            LevelFilter::Info
        );
        assert_eq!(
            RedisLogLevel::from_str("notice").to_level_filter(),
            LevelFilter::Info
        );
        assert_eq!(
            RedisLogLevel::from_str("warning").to_level_filter(),
            LevelFilter::Warn
        );
        assert_eq!(
            RedisLogLevel::from_str("nothing").to_level_filter(),
            LevelFilter::Off
        );
        // Unknown defaults to notice
        assert_eq!(
            RedisLogLevel::from_str("unknown").to_level_filter(),
            LevelFilter::Info
        );
    }

    #[test]
    fn test_sanitize_auth() {
        let logger = SockudoLogger {
            level: LevelFilter::Debug,
            file: None,
            hide_user_data: true,
            #[cfg(unix)]
            syslog_enabled: false,
            #[cfg(unix)]
            syslog_ident: String::new(),
        };

        let sanitized = logger.sanitize_message("User sent AUTH mysecretpassword");
        assert!(sanitized.contains("[REDACTED]"));
        assert!(!sanitized.contains("mysecretpassword"));
    }

    #[cfg(unix)]
    #[test]
    fn test_syslog_facility_parsing() {
        assert_eq!(SyslogFacility::from_str("local0").to_code(), 16 << 3);
        assert_eq!(SyslogFacility::from_str("user").to_code(), 1 << 3);
        assert_eq!(SyslogFacility::from_str("LOCAL7").to_code(), 23 << 3);
    }
}
