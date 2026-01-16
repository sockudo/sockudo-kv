//! Sentinel Script Execution
//!
//! Handles notification and client reconfiguration scripts.

use parking_lot::Mutex;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use super::state::SentinelState;

/// Script manager for executing notification and reconfiguration scripts
pub struct ScriptManager {
    state: Arc<SentinelState>,
}

impl ScriptManager {
    pub fn new(state: Arc<SentinelState>) -> Self {
        Self { state }
    }

    /// Queue a script for execution
    pub fn queue_script(&self, script: PendingScript) {
        // Update queue length in state
        let mut queue = self.state.scripts_queue.write();
        queue.push_back(script);
        self.state
            .scripts_queue_length
            .store(queue.len() as u32, Ordering::Relaxed);
    }
}

/// Maximum script execution time in milliseconds
const _SCRIPT_MAX_RUNTIME_MS: u64 = 60_000;

/// Maximum retry count for scripts
const SCRIPT_MAX_RETRY: u32 = 10;

/// Script execution result
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScriptResult {
    /// Script succeeded
    Success,
    /// Script failed, retry later
    RetryLater,
    /// Script failed, don't retry
    Failed,
    /// Script timed out
    Timeout,
}

/// Pending script in execution queue
#[derive(Debug)]
pub struct PendingScript {
    /// Script path
    pub path: PathBuf,
    /// Script arguments
    pub args: Vec<String>,
    /// Number of retry attempts
    pub retry_count: u32,
    /// Script type
    pub script_type: ScriptType,
    /// Master name this script is for
    pub master_name: String,
}

impl PendingScript {
    pub fn new(
        path: PathBuf,
        args: Vec<String>,
        script_type: ScriptType,
        master_name: String,
    ) -> Self {
        Self {
            path,
            args,
            retry_count: 0,
            script_type,
            master_name,
        }
    }

    pub fn retries(&self) -> u32 {
        self.retry_count
    }

    pub fn increment_retry(&mut self) -> u32 {
        self.retry_count += 1;
        self.retry_count
    }

    pub fn can_retry(&self) -> bool {
        self.retries() < SCRIPT_MAX_RETRY
    }
}

impl Clone for PendingScript {
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            args: self.args.clone(),
            retry_count: self.retry_count,
            script_type: self.script_type,
            master_name: self.master_name.clone(),
        }
    }
}

/// Script type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScriptType {
    /// notification-script: called on warning events
    Notification,
    /// client-reconfig-script: called on switch-master
    ClientReconfig,
}

impl ScriptType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Notification => "notification-script",
            Self::ClientReconfig => "client-reconfig-script",
        }
    }
}

/// Script executor
pub struct ScriptExecutor {
    /// Currently running scripts count
    running: AtomicU32,
    /// Pending scripts
    pending: Mutex<Vec<PendingScript>>,
}

impl ScriptExecutor {
    pub fn new() -> Self {
        Self {
            running: AtomicU32::new(0),
            pending: Mutex::new(Vec::new()),
        }
    }

    /// Queue a script for execution
    pub fn queue(&self, script: PendingScript) {
        self.pending.lock().push(script);
    }

    /// Get pending scripts count
    pub fn pending_count(&self) -> usize {
        self.pending.lock().len()
    }

    /// Get running scripts count
    pub fn running_count(&self) -> u32 {
        self.running.load(Ordering::Relaxed)
    }

    /// Execute a notification script
    pub fn run_notification_script(
        &self,
        path: &PathBuf,
        event_type: &str,
        event_description: &str,
    ) -> ScriptResult {
        self.execute_script(
            path,
            &[event_type.to_string(), event_description.to_string()],
        )
    }

    /// Execute a client reconfig script
    pub fn run_reconfig_script(
        &self,
        path: &PathBuf,
        master_name: &str,
        role: &str,
        state: &str,
        from_ip: &str,
        from_port: u16,
        to_ip: &str,
        to_port: u16,
    ) -> ScriptResult {
        let args = vec![
            master_name.to_string(),
            role.to_string(),
            state.to_string(),
            from_ip.to_string(),
            from_port.to_string(),
            to_ip.to_string(),
            to_port.to_string(),
        ];
        self.execute_script(path, &args)
    }

    /// Execute a script with arguments
    fn execute_script(&self, path: &PathBuf, args: &[String]) -> ScriptResult {
        self.running.fetch_add(1, Ordering::Relaxed);

        let result = self.run_script_inner(path, args);

        self.running.fetch_sub(1, Ordering::Relaxed);

        result
    }

    fn run_script_inner(&self, path: &PathBuf, args: &[String]) -> ScriptResult {
        let result = Command::new(path)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn();

        let mut child = match result {
            Ok(child) => child,
            Err(_) => return ScriptResult::Failed,
        };

        match child.wait() {
            Ok(status) => {
                if status.success() {
                    ScriptResult::Success
                } else {
                    match status.code() {
                        Some(1) => ScriptResult::RetryLater,
                        Some(_) | None => ScriptResult::Failed,
                    }
                }
            }
            Err(_) => ScriptResult::Failed,
        }
    }

    /// Process pending scripts
    pub fn process_pending(&self) -> Vec<(PendingScript, ScriptResult)> {
        let scripts: Vec<PendingScript> = {
            let mut pending = self.pending.lock();
            std::mem::take(&mut *pending)
        };

        let mut results = Vec::with_capacity(scripts.len());

        for mut script in scripts {
            let result = self.execute_script(&script.path, &script.args);

            // Re-queue if retry is needed and possible
            if result == ScriptResult::RetryLater && script.can_retry() {
                script.increment_retry();
                self.queue(script.clone());
            }

            results.push((script, result));
        }

        results
    }
}

impl Default for ScriptExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pending_script() {
        let mut script = PendingScript::new(
            PathBuf::from("/usr/bin/echo"),
            vec!["hello".to_string()],
            ScriptType::Notification,
            "mymaster".to_string(),
        );

        assert_eq!(script.retries(), 0);
        assert!(script.can_retry());

        for _ in 0..SCRIPT_MAX_RETRY {
            script.increment_retry();
        }

        assert!(!script.can_retry());
    }
}
