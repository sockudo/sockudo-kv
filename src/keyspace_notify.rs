//! Keyspace Notifications
//!
//! Implements Redis keyspace notifications that publish events to:
//! - `__keyspace@<db>__:<key>` with the event name as message
//! - `__keyevent@<db>__:<event>` with the key name as message
//!
//! Controlled by `notify-keyspace-events` config setting.

use bytes::Bytes;
use std::sync::Arc;

use crate::PubSub;

/// Notification flags parsed from notify-keyspace-events config
#[derive(Debug, Clone, Copy, Default)]
pub struct NotifyFlags {
    /// K - Keyspace events (__keyspace@<db>__)
    pub keyspace: bool,
    /// E - Keyevent events (__keyevent@<db>__)
    pub keyevent: bool,
    /// g - Generic commands (DEL, EXPIRE, RENAME, ...)
    pub generic: bool,
    /// $ - String commands
    pub string: bool,
    /// l - List commands
    pub list: bool,
    /// s - Set commands
    pub set: bool,
    /// h - Hash commands
    pub hash: bool,
    /// z - Sorted set commands
    pub zset: bool,
    /// x - Expired events
    pub expired: bool,
    /// e - Evicted events
    pub evicted: bool,
    /// t - Stream commands
    pub stream: bool,
    /// m - Key miss events
    pub key_miss: bool,
    /// d - Module key type events
    pub module: bool,
    /// n - New key events
    pub new_key: bool,
}

impl NotifyFlags {
    /// Parse from config string (e.g., "KEg$lshzxe")
    /// A = alias for g$lshzxet
    pub fn parse(config: &str) -> Self {
        let mut flags = NotifyFlags::default();

        let mut has_a = false;
        for c in config.chars() {
            match c {
                'K' => flags.keyspace = true,
                'E' => flags.keyevent = true,
                'g' => flags.generic = true,
                '$' => flags.string = true,
                'l' => flags.list = true,
                's' => flags.set = true,
                'h' => flags.hash = true,
                'z' => flags.zset = true,
                'x' => flags.expired = true,
                'e' => flags.evicted = true,
                't' => flags.stream = true,
                'm' => flags.key_miss = true,
                'd' => flags.module = true,
                'n' => flags.new_key = true,
                'A' => has_a = true,
                _ => {}
            }
        }

        // A is alias for g$lshzxet (all standard events)
        if has_a {
            flags.generic = true;
            flags.string = true;
            flags.list = true;
            flags.set = true;
            flags.hash = true;
            flags.zset = true;
            flags.expired = true;
            flags.evicted = true;
            flags.stream = true;
        }

        flags
    }

    /// Check if any notifications are enabled
    #[inline]
    pub fn is_enabled(&self) -> bool {
        (self.keyspace || self.keyevent) && self.has_any_event()
    }

    /// Check if any event type is enabled
    #[inline]
    fn has_any_event(&self) -> bool {
        self.generic
            || self.string
            || self.list
            || self.set
            || self.hash
            || self.zset
            || self.expired
            || self.evicted
            || self.stream
            || self.key_miss
            || self.module
            || self.new_key
    }

    /// Check if notification should be sent for event type
    #[inline]
    pub fn should_notify(&self, event_type: EventType) -> bool {
        if !self.keyspace && !self.keyevent {
            return false;
        }

        match event_type {
            EventType::Generic => self.generic,
            EventType::String => self.string,
            EventType::List => self.list,
            EventType::Set => self.set,
            EventType::Hash => self.hash,
            EventType::SortedSet => self.zset,
            EventType::Expired => self.expired,
            EventType::Evicted => self.evicted,
            EventType::Stream => self.stream,
            EventType::KeyMiss => self.key_miss,
            EventType::NewKey => self.new_key,
        }
    }
}

/// Event types for keyspace notifications
#[derive(Debug, Clone, Copy)]
pub enum EventType {
    Generic,
    String,
    List,
    Set,
    Hash,
    SortedSet,
    Expired,
    Evicted,
    Stream,
    KeyMiss,
    NewKey,
}

/// Keyspace notification emitter
pub struct KeyspaceNotifier {
    flags: NotifyFlags,
    pubsub: Arc<PubSub>,
}

impl KeyspaceNotifier {
    /// Create new notifier with parsed flags
    pub fn new(config: &str, pubsub: Arc<PubSub>) -> Self {
        Self {
            flags: NotifyFlags::parse(config),
            pubsub,
        }
    }

    /// Check if notifications are enabled
    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.flags.is_enabled()
    }

    /// Notify about an event
    pub fn notify(&self, db: usize, event: &str, key: &[u8], event_type: EventType) {
        if !self.flags.should_notify(event_type) {
            return;
        }

        // __keyspace@<db>__:<key> with event as message
        if self.flags.keyspace {
            let channel_prefix = format!("__keyspace@{}__:", db);
            let channel_bytes = [channel_prefix.as_bytes(), key].concat();

            // Optimization: check if anyone is listening before formatting/allocating message
            if self.pubsub.has_subscribers_for_channel(&channel_bytes) {
                self.pubsub
                    .publish(&channel_bytes, Bytes::from(event.to_string()));
            }
        }

        // __keyevent@<db>__:<event> with key as message
        if self.flags.keyevent {
            let channel = format!("__keyevent@{}__:{}", db, event);
            // Optimization: check if anyone is listening
            if self.pubsub.has_subscribers_for_channel(channel.as_bytes()) {
                self.pubsub
                    .publish(channel.as_bytes(), Bytes::copy_from_slice(key));
            }
        }
    }

    // ==================== Common Event Helpers ====================

    /// Notify SET command
    #[inline]
    pub fn notify_set(&self, db: usize, key: &[u8]) {
        self.notify(db, "set", key, EventType::String);
    }

    /// Notify DEL command
    #[inline]
    pub fn notify_del(&self, db: usize, key: &[u8]) {
        self.notify(db, "del", key, EventType::Generic);
    }

    /// Notify EXPIRE/EXPIREAT
    #[inline]
    pub fn notify_expire(&self, db: usize, key: &[u8]) {
        self.notify(db, "expire", key, EventType::Generic);
    }

    /// Notify key expired
    #[inline]
    pub fn notify_expired(&self, db: usize, key: &[u8]) {
        self.notify(db, "expired", key, EventType::Expired);
    }

    /// Notify key evicted
    #[inline]
    pub fn notify_evicted(&self, db: usize, key: &[u8]) {
        self.notify(db, "evicted", key, EventType::Evicted);
    }

    /// Notify RENAME
    #[inline]
    pub fn notify_rename(&self, db: usize, old_key: &[u8], new_key: &[u8]) {
        self.notify(db, "rename_from", old_key, EventType::Generic);
        self.notify(db, "rename_to", new_key, EventType::Generic);
    }

    /// Notify LPUSH/RPUSH
    #[inline]
    pub fn notify_list_push(&self, db: usize, key: &[u8]) {
        self.notify(db, "lpush", key, EventType::List);
    }

    /// Notify SADD
    #[inline]
    pub fn notify_sadd(&self, db: usize, key: &[u8]) {
        self.notify(db, "sadd", key, EventType::Set);
    }

    /// Notify HSET
    #[inline]
    pub fn notify_hset(&self, db: usize, key: &[u8]) {
        self.notify(db, "hset", key, EventType::Hash);
    }

    /// Notify ZADD
    #[inline]
    pub fn notify_zadd(&self, db: usize, key: &[u8]) {
        self.notify(db, "zadd", key, EventType::SortedSet);
    }

    /// Notify XADD
    #[inline]
    pub fn notify_xadd(&self, db: usize, key: &[u8]) {
        self.notify(db, "xadd", key, EventType::Stream);
    }

    /// Notify new key created
    #[inline]
    pub fn notify_new_key(&self, db: usize, key: &[u8]) {
        self.notify(db, "new", key, EventType::NewKey);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_flags() {
        let flags = NotifyFlags::parse("KEg");
        assert!(flags.keyspace);
        assert!(flags.keyevent);
        assert!(flags.generic);
        assert!(!flags.string);
    }

    #[test]
    fn test_parse_all_alias() {
        let flags = NotifyFlags::parse("AKE");
        assert!(flags.keyspace);
        assert!(flags.keyevent);
        assert!(flags.generic);
        assert!(flags.string);
        assert!(flags.list);
        assert!(flags.set);
        assert!(flags.hash);
        assert!(flags.zset);
    }

    #[test]
    fn test_empty_config() {
        let flags = NotifyFlags::parse("");
        assert!(!flags.is_enabled());
    }

    #[test]
    fn test_should_notify() {
        let flags = NotifyFlags::parse("Kg");
        assert!(flags.should_notify(EventType::Generic));
        assert!(!flags.should_notify(EventType::String));
    }
}
