//! Blocking commands implementation (BLPOP, BRPOP, BLMOVE, etc.)
//!
//! This module implements Redis blocking list operations. These commands block
//! the connection until data becomes available or a timeout is reached.

use std::time::Duration;

use crate::error::{Error, Result};

/// Parse timeout argument (supports both integer seconds and float seconds)
pub fn parse_timeout(arg: &[u8]) -> Result<Option<Duration>> {
    let s = std::str::from_utf8(arg).map_err(|_| Error::NotInteger)?;

    // Try parsing as float first (Redis 6.0+ supports float timeouts)
    if let Ok(secs) = s.parse::<f64>() {
        if secs < 0.0 {
            return Err(Error::Other("timeout is negative"));
        }
        if secs == 0.0 {
            return Ok(None); // 0 means block indefinitely
        }
        // Check for overflow - max timeout is about 2^53 seconds
        if secs > 9007199254740992.0 {
            return Err(Error::Other("timeout is out of range"));
        }
        return Ok(Some(Duration::from_secs_f64(secs)));
    }

    // Try parsing as integer
    let secs: i64 = s.parse().map_err(|_| Error::NotInteger)?;
    if secs < 0 {
        return Err(Error::Other("timeout is negative"));
    }
    if secs == 0 {
        return Ok(None);
    }
    Ok(Some(Duration::from_secs(secs as u64)))
}

/// Check if a command is a blocking list command
pub fn is_blocking_list_command(cmd: &[u8]) -> bool {
    cmd.eq_ignore_ascii_case(b"BLPOP")
        || cmd.eq_ignore_ascii_case(b"BRPOP")
        || cmd.eq_ignore_ascii_case(b"BLMOVE")
        || cmd.eq_ignore_ascii_case(b"BRPOPLPUSH")
        || cmd.eq_ignore_ascii_case(b"BLMPOP")
}
