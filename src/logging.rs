//! Logging configuration module using tracing
//!
//! Implements Redis-compatible logging with support for:
//! - Log levels (debug, verbose, notice, warning, nothing)
//! - File-based logging (non-blocking)
//! - Log sanitization for sensitive data
//! - Custom formatting to match Redis style

use crate::config::ServerConfig;
use std::fmt;

use tracing::{Event, Subscriber};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::registry::LookupSpan;

/// Redis-style log levels mapped to Tracing levels
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

    /// Convert to Tracing Level (for filter)
    pub fn to_tracing_filter(self) -> String {
        match self {
            Self::Debug => "debug".to_string(),
            Self::Verbose => "info".to_string(), // Redis "verbose" ≈ info
            Self::Notice => "info".to_string(),  // Redis "notice" ≈ info (default)
            Self::Warning => "warn".to_string(),
            Self::Nothing => "off".to_string(),
        }
    }
}

/// Custom formatter to replicate Redis log output with sanitization
struct SockudoFormatter {
    hide_user_data: bool,
}

impl SockudoFormatter {
    fn new(hide_user_data: bool) -> Self {
        Self { hide_user_data }
    }

    /// Sanitize log message to hide sensitive data
    fn sanitize_message(&self, msg: &str) -> String {
        if !self.hide_user_data {
            return msg.to_string();
        }

        // Patterns to redact
        let mut result = msg.to_string();

        // Redact AUTH command args
        if result.to_uppercase().contains("AUTH") {
            result = regex_lite_replace(&result, "AUTH", true);
        }

        // Redact CONFIG SET requirepass
        if result.to_uppercase().contains("REQUIREPASS") {
            result = regex_lite_replace(&result, "requirepass", false);
        }

        result
    }
}

/// Helper to replace sensitive data without regex crate
/// Replaces the word AFTER the keyword with [REDACTED]
fn regex_lite_replace(input: &str, keyword: &str, case_insensitive_keyword: bool) -> String {
    // Find keyword
    let keyword_idx = if case_insensitive_keyword {
        input.to_uppercase().find(&keyword.to_uppercase())
    } else {
        input.find(keyword)
    };

    if let Some(idx) = keyword_idx {
        // We know 'after_keyword_start' starts with keyword.
        // We need to find the end of keyword, then space, then the value to redact.
        // But simply: find the keyword, skip it, find text after whitespace.

        // Logic:
        // 1. Split at keyword.
        // 2. Look at what follows.

        // Simpler approach for "AUTH password":
        // Find "AUTH " (case insensitive)
        // Redact next word.

        let keyword_len = keyword.len();
        if input.len() <= idx + keyword_len {
            return input.to_string(); // keyword is at end
        }

        let after_keyword = &input[idx + keyword_len..];

        // Skip whitespace
        if let Some(val_start_rel) = after_keyword.find(|c: char| !c.is_whitespace()) {
            let val_start = idx + keyword_len + val_start_rel;

            // Check if there is actual content
            let rest = &input[val_start..];

            // Find end of value (whitespace or end of string)
            let val_end_rel = rest.find(|c: char| c.is_whitespace()).unwrap_or(rest.len());

            let val_end = val_start + val_end_rel;

            let mut result = String::with_capacity(input.len());
            result.push_str(&input[..val_start]);
            result.push_str("[REDACTED]");
            result.push_str(&input[val_end..]);
            return result;
        }
    }
    input.to_string()
}

impl<S, N> FormatEvent<S, N> for SockudoFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        _ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let level_char = match *event.metadata().level() {
            tracing::Level::ERROR => '!',
            tracing::Level::WARN => '#',
            tracing::Level::INFO => '*',
            tracing::Level::DEBUG => '-',
            tracing::Level::TRACE => '.',
        };

        let pid = std::process::id();

        write!(writer, "{}:{} {} ", pid, level_char, now)?;

        // Extract message
        let mut visitor = MessageVisitor::new();
        event.record(&mut visitor);

        // Sanitize and write
        let msg = self.sanitize_message(&visitor.message);
        writeln!(writer, "{}", msg)
    }
}

struct MessageVisitor {
    message: String,
}

impl MessageVisitor {
    fn new() -> Self {
        Self {
            message: String::new(),
        }
    }
}

impl tracing::field::Visit for MessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            use std::fmt::Write;
            let _ = write!(self.message, "{:?}", value);
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message.push_str(value);
        }
    }
}

/// Initialize logging using tracing
pub fn init_logging(
    config: &ServerConfig,
) -> Result<Option<tracing_appender::non_blocking::WorkerGuard>, Box<dyn std::error::Error>> {
    // Bridge log crate to tracing
    tracing_log::LogTracer::init()?;

    let filter_str = RedisLogLevel::from_str(&config.loglevel).to_tracing_filter();
    let env_filter = EnvFilter::new(filter_str);

    let formatter = SockudoFormatter::new(config.hide_user_data_from_log);

    if !config.logfile.is_empty() {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&config.logfile)?;

        // Use non-blocking appender
        let (non_blocking, guard) = tracing_appender::non_blocking(file);

        let subscriber = tracing_subscriber::fmt::Subscriber::builder()
            .with_env_filter(env_filter)
            .event_format(formatter)
            .with_writer(non_blocking)
            .finish();

        tracing::subscriber::set_global_default(subscriber)?;
        Ok(Some(guard))
    } else {
        // Stdout
        let subscriber = tracing_subscriber::fmt::Subscriber::builder()
            .with_env_filter(env_filter)
            .event_format(formatter)
            .with_writer(std::io::stderr)
            .finish();

        tracing::subscriber::set_global_default(subscriber)?;
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_level_parsing() {
        assert_eq!(
            RedisLogLevel::from_str("debug").to_tracing_filter(),
            "debug"
        );
        assert_eq!(
            RedisLogLevel::from_str("verbose").to_tracing_filter(),
            "info"
        );
    }

    #[test]
    fn test_sanitize_auth() {
        let formatter = SockudoFormatter::new(true);
        let msg = "User sent AUTH mysecretpassword";
        let sanitized = formatter.sanitize_message(msg);
        assert!(sanitized.contains("[REDACTED]"));
        assert!(!sanitized.contains("mysecretpassword"));
    }

    #[test]
    fn test_sanitize_requirepass() {
        let formatter = SockudoFormatter::new(true);
        let msg = "config set requirepass supersecret";
        let sanitized = formatter.sanitize_message(msg);
        assert!(sanitized.contains("[REDACTED]"));
        assert!(!sanitized.contains("supersecret"));
    }
}
