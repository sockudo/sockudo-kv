use std::time::{SystemTime, UNIX_EPOCH};

/// Get current time in milliseconds since UNIX epoch
#[inline(always)]
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
