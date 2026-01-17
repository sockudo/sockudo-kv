use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Global flag to allow access to expired keys (for testing/debugging)
/// When true, is_expired() will always return false, allowing access to logically expired keys
static ALLOW_ACCESS_EXPIRED: AtomicBool = AtomicBool::new(false);

/// Set whether to allow access to expired keys
#[inline]
pub fn set_allow_access_expired(allow: bool) {
    ALLOW_ACCESS_EXPIRED.store(allow, Ordering::SeqCst);
}

/// Check if access to expired keys is allowed
#[inline(always)]
pub fn allow_access_expired() -> bool {
    ALLOW_ACCESS_EXPIRED.load(Ordering::Relaxed)
}

/// Get current time in milliseconds since UNIX epoch
#[inline(always)]
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
