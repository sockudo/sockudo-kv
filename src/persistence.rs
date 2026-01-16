//! Persistence module - RDB snapshots and AOF
//!
//! Implements background save (BGSAVE) scheduling and AOF append

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use itertools::Itertools;

use crate::config::ServerConfig;
use crate::storage::MultiStore;

/// Save point configuration (seconds and changes threshold)
#[derive(Debug, Clone)]
pub struct SavePoint {
    pub seconds: u64,
    pub changes: u64,
}

impl SavePoint {
    /// Parse from config string "seconds changes"
    pub fn parse(config: &str) -> Option<Self> {
        if let Some((Ok(seconds), Ok(changes))) =
            config.split_whitespace().map(str::parse).collect_tuple()
        {
            Some(Self { seconds, changes })
        } else {
            None
        }
    }
}

/// AOF sync policy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AppendFsync {
    /// Sync every write
    Always,
    /// Sync every second (default)
    #[default]
    EverySecond,
    /// Let OS handle syncing
    No,
}

impl std::str::FromStr for AppendFsync {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.to_lowercase().as_str() {
            "always" => Self::Always,
            "everysec" => Self::EverySecond,
            "no" => Self::No,
            _ => Self::EverySecond,
        })
    }
}

/// Persistence manager for RDB and AOF
pub struct PersistenceManager {
    /// Save points from config
    save_points: Vec<SavePoint>,
    /// Number of changes since last save
    changes_since_save: AtomicU64,
    /// Last save timestamp
    last_save: std::sync::Mutex<Instant>,
    /// BGSAVE in progress flag
    bgsave_in_progress: AtomicBool,
    /// Last BGSAVE status (0 = ok, 1 = error)
    last_bgsave_status: AtomicU64,
    /// AOF enabled flag
    aof_enabled: AtomicBool,
    /// AOF sync policy
    aof_fsync: AppendFsync,
    /// RDB file path
    rdb_path: String,
    /// AOF file path
    aof_path: String,
}

impl PersistenceManager {
    /// Create from config
    pub fn from_config(config: &ServerConfig) -> Self {
        let rdb_path = std::path::Path::new(&config.dir)
            .join(&config.dbfilename)
            .to_string_lossy()
            .to_string();

        let aof_path = std::path::Path::new(&config.dir)
            .join(&config.appendfilename)
            .to_string_lossy()
            .to_string();

        Self {
            save_points: config
                .save_points
                .iter()
                .map(|&(seconds, changes)| SavePoint { seconds, changes })
                .collect(),
            changes_since_save: AtomicU64::new(0),
            last_save: std::sync::Mutex::new(Instant::now()),
            bgsave_in_progress: AtomicBool::new(false),
            last_bgsave_status: AtomicU64::new(0),
            aof_enabled: AtomicBool::new(config.appendonly),
            aof_fsync: config.appendfsync.parse().unwrap(),
            rdb_path,
            aof_path,
        }
    }

    /// Record a change (for save point tracking)
    #[inline]
    pub fn record_change(&self) {
        self.changes_since_save.fetch_add(1, Ordering::Relaxed);
    }

    /// Check if auto-save should be triggered
    pub fn should_save(&self) -> bool {
        if self.save_points.is_empty() {
            return false;
        }

        if self.bgsave_in_progress.load(Ordering::Relaxed) {
            return false;
        }

        let changes = self.changes_since_save.load(Ordering::Relaxed);
        let elapsed = self.last_save.lock().unwrap().elapsed().as_secs();

        for sp in &self.save_points {
            if elapsed >= sp.seconds && changes >= sp.changes {
                return true;
            }
        }

        false
    }

    /// Mark save as started
    pub fn start_bgsave(&self) {
        self.bgsave_in_progress.store(true, Ordering::Release);
    }

    /// Mark save as completed
    pub fn finish_bgsave(&self, success: bool) {
        self.bgsave_in_progress.store(false, Ordering::Release);
        self.last_bgsave_status
            .store(if success { 0 } else { 1 }, Ordering::Relaxed);
        if success {
            self.changes_since_save.store(0, Ordering::Relaxed);
            *self.last_save.lock().unwrap() = Instant::now();
        }
    }

    /// Check if BGSAVE is in progress
    #[inline]
    pub fn is_bgsave_in_progress(&self) -> bool {
        self.bgsave_in_progress.load(Ordering::Relaxed)
    }

    /// Get RDB file path
    pub fn rdb_path(&self) -> &str {
        &self.rdb_path
    }

    /// Get AOF file path
    pub fn aof_path(&self) -> &str {
        &self.aof_path
    }

    /// Check if AOF is enabled
    #[inline]
    pub fn is_aof_enabled(&self) -> bool {
        self.aof_enabled.load(Ordering::Relaxed)
    }

    /// Get AOF sync policy
    pub fn aof_sync_policy(&self) -> AppendFsync {
        self.aof_fsync
    }
}

/// Spawn background task for auto-save checking
pub fn spawn_persistence_task(
    persistence: Arc<PersistenceManager>,
    store: Arc<MultiStore>,
    config: Arc<crate::config::ServerConfig>,
    server_state: Arc<crate::server_state::ServerState>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));

        loop {
            interval.tick().await;

            if persistence.should_save() {
                let persistence = Arc::clone(&persistence);
                let store = Arc::clone(&store);
                let config = Arc::clone(&config);
                let server_state = Arc::clone(&server_state);

                tokio::spawn(async move {
                    persistence.start_bgsave();

                    // Generate RDB data using existing implementation
                    use crate::replication::rdb::{RdbConfig, generate_rdb_with_config};

                    let rdb_config = RdbConfig {
                        compression: config.rdbcompression,
                        checksum: config.rdbchecksum,
                    };

                    let rdb_data = generate_rdb_with_config(&store, rdb_config);
                    let rdb_path = std::path::Path::new(&config.dir).join(&config.dbfilename);

                    let success = match save_rdb_file(
                        &rdb_path,
                        &rdb_data,
                        config.rdb_save_incremental_fsync,
                    ) {
                        Ok(_) => {
                            server_state.last_save_time.store(
                                server_state.now_unix(),
                                std::sync::atomic::Ordering::Relaxed,
                            );
                            server_state
                                .last_bgsave_status
                                .store(false, std::sync::atomic::Ordering::Relaxed);
                            true
                        }
                        Err(e) => {
                            eprintln!("BGSAVE error: {}", e);
                            server_state
                                .last_bgsave_status
                                .store(true, std::sync::atomic::Ordering::Relaxed);
                            false
                        }
                    };

                    persistence.finish_bgsave(success);
                });
            }
        }
    })
}

/// Save RDB data to file with optional incremental fsync
fn save_rdb_file(
    path: &std::path::Path,
    data: &[u8],
    incremental_fsync: bool,
) -> std::io::Result<()> {
    use std::fs::OpenOptions;
    use std::io::Write;

    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;

    if !incremental_fsync {
        file.write_all(data)?;
        return Ok(());
    }

    // Incremental fsync every 4MB (like Redis)
    const CHUNK_SIZE: usize = 4 * 1024 * 1024;
    for chunk in data.chunks(CHUNK_SIZE) {
        file.write_all(chunk)?;
        file.sync_data()?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_save_point_parse() {
        let sp = SavePoint::parse("900 1").unwrap();
        assert_eq!(sp.seconds, 900);
        assert_eq!(sp.changes, 1);
    }

    #[test]
    fn test_appendfsync_parse() {
        assert_eq!(
            "always".parse::<AppendFsync>().unwrap(),
            AppendFsync::Always
        );
        assert_eq!(
            "everysec".parse::<AppendFsync>().unwrap(),
            AppendFsync::EverySecond
        );
        assert_eq!("no".parse::<AppendFsync>().unwrap(), AppendFsync::No);
    }
}
