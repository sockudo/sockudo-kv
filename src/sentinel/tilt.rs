//! Sentinel TILT Mode Protection
//!
//! TILT mode is a protection mechanism that detects abnormal system behavior
//! (time jumps, process freezes) and temporarily disables Sentinel actions.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use super::events::{SentinelEvent, SentinelEventPublisher};
use super::state::SentinelState;

/// TILT mode recovery period (30 seconds)
const TILT_EXIT_PERIOD_MS: u64 = 30_000;

/// Maximum acceptable time delta between ticks (2 seconds)
const TILT_TRIGGER_DELTA_MS: u64 = 2_000;

/// TILT mode manager
pub struct TiltManager {
    /// Reference to sentinel state
    state: Arc<SentinelState>,
    /// Last timer tick timestamp
    last_timer_tick: AtomicU64,
    /// Event publisher
    events: Option<Arc<SentinelEventPublisher>>,
}

impl TiltManager {
    /// Create a new TILT manager
    pub fn new(state: Arc<SentinelState>) -> Self {
        Self {
            state,
            last_timer_tick: AtomicU64::new(current_time_ms()),
            events: None,
        }
    }

    /// Create with event publisher
    pub fn with_events(state: Arc<SentinelState>, events: Arc<SentinelEventPublisher>) -> Self {
        Self {
            state,
            last_timer_tick: AtomicU64::new(current_time_ms()),
            events: Some(events),
        }
    }

    /// Check and update TILT mode
    ///
    /// Called on every timer tick (typically 100ms).
    /// Enters TILT if time delta is negative or > 2 seconds.
    /// Exits TILT after 30 seconds of normal operation.
    pub fn check_tilt(&self) {
        let now = current_time_ms();
        let last = self.last_timer_tick.swap(now, Ordering::SeqCst);

        // Calculate time delta
        let delta = if now >= last {
            now - last
        } else {
            // Time went backwards!
            u64::MAX
        };

        let was_tilt = self.state.is_tilt();

        // Check if we should enter TILT mode
        if delta > TILT_TRIGGER_DELTA_MS {
            if !was_tilt {
                log::warn!(
                    "Entering TILT mode: timer tick delta {} ms is too large",
                    delta
                );
                self.state.enter_tilt();

                if let Some(events) = &self.events {
                    events.publish(SentinelEvent::Tilt);
                }
            } else {
                // Already in TILT, reset the timer
                self.state.tilt_start_time.store(now, Ordering::Relaxed);
            }
            return;
        }

        // Check if we should exit TILT mode
        if was_tilt {
            let tilt_start = self.state.tilt_start_time.load(Ordering::Relaxed);
            let tilt_duration = now.saturating_sub(tilt_start);

            if tilt_duration >= TILT_EXIT_PERIOD_MS {
                log::info!(
                    "Exiting TILT mode after {} seconds of normal operation",
                    tilt_duration / 1000
                );
                self.state.exit_tilt();

                if let Some(events) = &self.events {
                    events.publish(SentinelEvent::TiltCleared);
                }
            }
        }
    }

    /// Check if actions are allowed (not in TILT mode)
    pub fn actions_allowed(&self) -> bool {
        !self.state.is_tilt()
    }

    /// Get TILT status info for INFO command
    pub fn info(&self) -> TiltInfo {
        TiltInfo {
            is_tilt: self.state.is_tilt(),
            tilt_since_seconds: self.state.tilt_since_seconds(),
        }
    }

    /// Force enter TILT mode (for testing)
    pub fn force_tilt(&self) {
        if !self.state.is_tilt() {
            log::warn!("Forcing TILT mode for testing");
            self.state.enter_tilt();

            if let Some(events) = &self.events {
                events.publish(SentinelEvent::Tilt);
            }
        }
    }

    /// Force exit TILT mode (for testing)
    pub fn force_exit_tilt(&self) {
        if self.state.is_tilt() {
            log::info!("Forcing exit from TILT mode");
            self.state.exit_tilt();

            if let Some(events) = &self.events {
                events.publish(SentinelEvent::TiltCleared);
            }
        }
    }
}

/// TILT status information
#[derive(Debug, Clone, Copy)]
pub struct TiltInfo {
    pub is_tilt: bool,
    pub tilt_since_seconds: i64,
}

impl TiltInfo {
    /// Format for INFO output
    pub fn format(&self) -> String {
        format!(
            "sentinel_tilt:{}\nsentinel_tilt_since_seconds:{}",
            if self.is_tilt { 1 } else { 0 },
            self.tilt_since_seconds
        )
    }
}

/// Get current time in milliseconds
#[inline]
fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tilt_manager() {
        let state = Arc::new(SentinelState::default());
        let manager = TiltManager::new(state.clone());

        // Initially not in TILT
        assert!(manager.actions_allowed());
        assert!(!state.is_tilt());

        // Normal tick shouldn't cause TILT
        manager.check_tilt();
        assert!(manager.actions_allowed());

        // Force TILT
        manager.force_tilt();
        assert!(!manager.actions_allowed());
        assert!(state.is_tilt());

        // Force exit
        manager.force_exit_tilt();
        assert!(manager.actions_allowed());
    }

    #[test]
    fn test_tilt_info() {
        let state = Arc::new(SentinelState::default());
        let manager = TiltManager::new(state.clone());

        let info = manager.info();
        assert!(!info.is_tilt);
        assert_eq!(info.tilt_since_seconds, -1);

        manager.force_tilt();
        let info = manager.info();
        assert!(info.is_tilt);
        assert!(info.tilt_since_seconds >= 0);
    }
}
