//! Pub/Sub infrastructure for real-time message passing
//!
//! Performance optimizations:
//! - DashMap for lock-free O(1) subscription management
//! - Tokio broadcast for O(1) message delivery
//! - Zero-copy Bytes throughout
//! - Pre-compiled pattern matching

use bytes::Bytes;
use dashmap::{DashMap, DashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::broadcast;

use crate::glob_trie::DashGlobTrie;

/// Message types for Pub/Sub
#[derive(Clone, Debug)]
pub enum PubSubMessage {
    /// Regular message: (channel, message)
    Message { channel: Bytes, message: Bytes },
    /// Sharded message: (channel, message) - uses "smessage" type
    SMessage { channel: Bytes, message: Bytes },
    /// Pattern message: (pattern, channel, message)
    PMessage {
        pattern: Bytes,
        channel: Bytes,
        message: Bytes,
    },
    /// Subscription confirmation: (channel, count)
    Subscribe { channel: Bytes, count: usize },
    /// Unsubscription confirmation: (channel, count)
    Unsubscribe { channel: Bytes, count: usize },
    /// Pattern subscription confirmation
    PSubscribe { pattern: Bytes, count: usize },
    /// Pattern unsubscription confirmation
    PUnsubscribe { pattern: Bytes, count: usize },
    /// Sharded subscription confirmation
    SSubscribe { channel: Bytes, count: usize },
    /// Sharded unsubscription confirmation
    SUnsubscribe { channel: Bytes, count: usize },
}

/// Subscriber state
pub struct Subscriber {
    /// Sender for pushing messages to this subscriber
    pub tx: broadcast::Sender<PubSubMessage>,
    /// Channels this subscriber is subscribed to (regular)
    pub channels: DashSet<Bytes>,
    /// Sharded channels this subscriber is subscribed to
    pub sharded_channels: DashSet<Bytes>,
    /// Patterns this subscriber is subscribed to
    pub patterns: DashSet<Bytes>,
}

impl Subscriber {
    pub fn new(tx: broadcast::Sender<PubSubMessage>) -> Self {
        Self {
            tx,
            channels: DashSet::new(),
            sharded_channels: DashSet::new(),
            patterns: DashSet::new(),
        }
    }

    #[inline]
    pub fn subscription_count(&self) -> usize {
        self.channels.len() + self.sharded_channels.len() + self.patterns.len()
    }
}

/// Global Pub/Sub manager
/// Lock-free and hyper-optimized for concurrent access
pub struct PubSub {
    /// channel -> set of subscriber IDs
    channels: DashMap<Bytes, DashSet<u64>>,
    /// sharded channel -> set of subscriber IDs
    sharded_channels: DashMap<Bytes, DashSet<u64>>,
    /// pattern -> set of subscriber IDs
    patterns: DashGlobTrie<DashSet<u64>>,
    /// subscriber ID -> Subscriber
    subscribers: DashMap<u64, Subscriber>,
    /// Next subscriber ID
    next_id: AtomicU64,
}

impl PubSub {
    pub fn new() -> Self {
        Self {
            channels: DashMap::new(),
            sharded_channels: DashMap::new(),
            patterns: DashGlobTrie::new(),
            subscribers: DashMap::new(),
            next_id: AtomicU64::new(1),
        }
    }

    /// Register a new subscriber, returns (id, receiver)
    pub fn register(&self) -> (u64, broadcast::Receiver<PubSubMessage>) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        // Dragonfly-style: smaller buffer, grows via backpressure
        let (tx, rx) = broadcast::channel(256);
        self.subscribers.insert(id, Subscriber::new(tx));
        (id, rx)
    }

    /// Unregister a subscriber and clean up all subscriptions
    pub fn unregister(&self, id: u64) {
        if let Some((_, sub)) = self.subscribers.remove(&id) {
            // Remove from all channels
            for channel in sub.channels.iter() {
                #[allow(clippy::explicit_auto_deref)]
                if let Some(subs) = self.channels.get(&*channel) {
                    subs.remove(&id);
                    if subs.is_empty() {
                        drop(subs);
                        self.channels.remove(&*channel);
                    }
                }
            }
            // Remove from all patterns
            for pattern in sub.patterns.iter() {
                #[allow(clippy::explicit_auto_deref)]
                if let Some(subs) = self.patterns.get_mut(&*pattern) {
                    subs.remove(&id);
                    if subs.is_empty() {
                        self.patterns.remove(&*pattern);
                    }
                }
            }
        }
    }

    /// Subscribe to channels
    /// Returns subscription counts for each channel
    pub fn subscribe(&self, id: u64, channels: &[Bytes]) -> Vec<usize> {
        let mut counts = Vec::with_capacity(channels.len());

        if let Some(sub) = self.subscribers.get(&id) {
            for channel in channels {
                // Add to subscriber's channels
                sub.channels.insert(channel.clone());

                // Add subscriber to channel's set
                self.channels.entry(channel.clone()).or_default().insert(id);

                counts.push(sub.subscription_count());
            }
        }

        counts
    }

    /// Unsubscribe from channels
    /// If channels is empty, unsubscribe from all
    pub fn unsubscribe(&self, id: u64, channels: &[Bytes]) -> Vec<(Bytes, usize)> {
        let mut results = Vec::new();

        if let Some(sub) = self.subscribers.get(&id) {
            let to_unsub: Vec<Bytes> = if channels.is_empty() {
                sub.channels.iter().map(|c| c.clone()).collect()
            } else {
                channels.to_vec()
            };

            for channel in to_unsub {
                // Remove from subscriber's channels
                sub.channels.remove(&channel);

                // Remove subscriber from channel's set
                if let Some(subs) = self.channels.get(&channel) {
                    subs.remove(&id);
                    if subs.is_empty() {
                        drop(subs);
                        self.channels.remove(&channel);
                    }
                }

                results.push((channel.clone(), sub.subscription_count()));
            }
        }

        results
    }

    /// Subscribe to patterns
    pub fn psubscribe(&self, id: u64, patterns: &[Bytes]) -> Vec<usize> {
        let mut counts = Vec::with_capacity(patterns.len());

        if let Some(sub) = self.subscribers.get(&id) {
            for pattern in patterns {
                // Add to subscriber's patterns
                sub.patterns.insert(pattern.clone());

                // Add subscriber to pattern's set
                self.patterns
                    .insert(pattern.clone(), DashSet::new)
                    .insert(id);

                counts.push(sub.subscription_count());
            }
        }

        counts
    }

    /// Unsubscribe from patterns
    pub fn punsubscribe(&self, id: u64, patterns: &[Bytes]) -> Vec<(Bytes, usize)> {
        let mut results = Vec::new();

        if let Some(sub) = self.subscribers.get(&id) {
            let to_unsub: Vec<Bytes> = if patterns.is_empty() {
                sub.patterns.iter().map(|p| p.clone()).collect()
            } else {
                patterns.to_vec()
            };

            for pattern in to_unsub {
                // Remove from subscriber's patterns
                sub.patterns.remove(&pattern);

                // Remove subscriber from pattern's set
                if let Some(subs) = self.patterns.get_mut(&pattern) {
                    subs.remove(&id);
                    if subs.is_empty() {
                        self.patterns.remove(&pattern);
                    }
                }

                results.push((pattern.clone(), sub.subscription_count()));
            }
        }

        results
    }

    /// Publish a message to a channel
    /// Returns number of clients that received the message
    pub fn publish(&self, channel: &[u8], message: Bytes) -> usize {
        let mut count = 0;
        let channel_bytes = Bytes::copy_from_slice(channel);

        // Deliver to exact channel subscribers
        if let Some(subs) = self.channels.get(channel) {
            for sub_id in subs.iter() {
                if let Some(sub) = self.subscribers.get(&*sub_id)
                    && sub
                        .tx
                        .send(PubSubMessage::Message {
                            channel: channel_bytes.clone(),
                            message: message.clone(),
                        })
                        .is_ok()
                {
                    count += 1;
                }
            }
        }

        // Deliver to pattern subscribers
        let matches = self.patterns.matches(channel);

        for (pat, subs) in matches {
            for sub_id in subs.iter() {
                if let Some(sub) = self.subscribers.get(&*sub_id)
                    && sub
                        .tx
                        .send(PubSubMessage::PMessage {
                            pattern: pat.clone(),
                            channel: channel_bytes.clone(),
                            message: message.clone(),
                        })
                        .is_ok()
                {
                    count += 1;
                }
            }
        }

        count
    }

    /// Check if there are any subscribers for a channel (or pattern matching it)
    /// This is an optimized check to avoid allocation in keyspace notifications
    pub fn has_subscribers_for_channel(&self, channel: &[u8]) -> bool {
        // Check exact channel match
        if let Some(subs) = self.channels.get(channel)
            && !subs.is_empty()
        {
            return true;
        }

        // Since we don't have an "any_match" API in GlobTrie yet, we rely on matches() check or existence
        // For keyspace events, meaningful patterns are typically `__key*` or `*`
        // If we have ANY patterns, we assume yes to be safe/simple, OR we check properly.
        // Let's check properly:
        !self.patterns.matches(channel).is_empty()
    }

    /// Subscribe to sharded channels
    /// Returns subscription counts for each channel
    pub fn ssubscribe(&self, id: u64, channels: &[Bytes]) -> Vec<usize> {
        let mut counts = Vec::with_capacity(channels.len());

        if let Some(sub) = self.subscribers.get(&id) {
            for channel in channels {
                // Add to subscriber's sharded channels
                sub.sharded_channels.insert(channel.clone());

                // Add subscriber to sharded channel's set
                self.sharded_channels
                    .entry(channel.clone())
                    .or_default()
                    .insert(id);

                counts.push(sub.subscription_count());
            }
        }

        counts
    }

    /// Unsubscribe from sharded channels
    /// If channels is empty, unsubscribe from all sharded channels
    pub fn sunsubscribe(&self, id: u64, channels: &[Bytes]) -> Vec<(Bytes, usize)> {
        let mut results = Vec::new();

        if let Some(sub) = self.subscribers.get(&id) {
            let to_unsub: Vec<Bytes> = if channels.is_empty() {
                sub.sharded_channels.iter().map(|c| c.clone()).collect()
            } else {
                channels.to_vec()
            };

            for channel in to_unsub {
                // Remove from subscriber's sharded channels
                sub.sharded_channels.remove(&channel);

                // Remove subscriber from sharded channel's set
                if let Some(subs) = self.sharded_channels.get(&channel) {
                    subs.remove(&id);
                    if subs.is_empty() {
                        drop(subs);
                        self.sharded_channels.remove(&channel);
                    }
                }

                results.push((channel.clone(), sub.subscription_count()));
            }
        }

        results
    }

    /// Publish a message to a sharded channel
    /// Returns number of clients that received the message (uses smessage type)
    pub fn spublish(&self, channel: &[u8], message: Bytes) -> usize {
        let mut count = 0;
        let channel_bytes = Bytes::copy_from_slice(channel);

        // Deliver to sharded channel subscribers (uses SMessage type)
        if let Some(subs) = self.sharded_channels.get(channel) {
            for sub_id in subs.iter() {
                if let Some(sub) = self.subscribers.get(&*sub_id)
                    && sub
                        .tx
                        .send(PubSubMessage::SMessage {
                            channel: channel_bytes.clone(),
                            message: message.clone(),
                        })
                        .is_ok()
                {
                    count += 1;
                }
            }
        }

        count
    }

    /// Get active channels matching pattern
    pub fn channels(&self, pattern: Option<&[u8]>) -> Vec<Bytes> {
        match pattern {
            Some(pat) => self
                .channels
                .iter()
                .filter(|e| !e.value().is_empty() && pattern_matches(pat, e.key()))
                .map(|e| e.key().clone())
                .collect(),
            None => self
                .channels
                .iter()
                .filter(|e| !e.value().is_empty())
                .map(|e| e.key().clone())
                .collect(),
        }
    }

    /// Get subscriber counts for channels
    pub fn numsub(&self, channels: &[Bytes]) -> Vec<(Bytes, usize)> {
        channels
            .iter()
            .map(|ch| {
                let count = self.channels.get(ch).map(|s| s.len()).unwrap_or(0);
                (ch.clone(), count)
            })
            .collect()
    }

    /// Get number of unique patterns
    pub fn numpat(&self) -> usize {
        self.patterns.len()
    }

    /// Check if subscriber is in pubsub mode
    pub fn is_subscribed(&self, id: u64) -> bool {
        self.subscribers
            .get(&id)
            .map(|s| s.subscription_count() > 0)
            .unwrap_or(false)
    }
}

impl Default for PubSub {
    fn default() -> Self {
        Self::new()
    }
}

// ==================== Pattern Matching ====================

/// Fast glob pattern matching
/// Supports: * (any sequence), ? (single char), [abc] (char class)
#[inline]
pub fn pattern_matches(pattern: &[u8], text: &[u8]) -> bool {
    let mut pi = 0;
    let mut ti = 0;
    let mut star_pi = None;
    let mut star_ti = None;

    while ti < text.len() {
        if pi < pattern.len() {
            match pattern[pi] {
                b'*' => {
                    star_pi = Some(pi);
                    star_ti = Some(ti);
                    pi += 1;
                    continue;
                }
                b'?' => {
                    pi += 1;
                    ti += 1;
                    continue;
                }
                b'[' => {
                    // Character class
                    let (matched, new_pi) = match_char_class(&pattern[pi..], text[ti]);
                    if matched {
                        pi += new_pi;
                        ti += 1;
                        continue;
                    }
                }
                c if c == text[ti] => {
                    pi += 1;
                    ti += 1;
                    continue;
                }
                _ => {}
            }
        }

        // Backtrack to star
        if let (Some(sp), Some(st)) = (star_pi, star_ti) {
            pi = sp + 1;
            star_ti = Some(st + 1);
            ti = st + 1;
        } else {
            return false;
        }
    }

    // Check remaining pattern
    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }

    pi == pattern.len()
}

/// Match character class [abc] or [a-z]
#[inline]
fn match_char_class(pattern: &[u8], c: u8) -> (bool, usize) {
    if pattern.is_empty() || pattern[0] != b'[' {
        return (false, 0);
    }

    let mut i = 1;
    let negate = if i < pattern.len() && pattern[i] == b'^' {
        i += 1;
        true
    } else {
        false
    };

    let mut matched = false;
    while i < pattern.len() && pattern[i] != b']' {
        if i + 2 < pattern.len() && pattern[i + 1] == b'-' {
            // Range like a-z
            if c >= pattern[i] && c <= pattern[i + 2] {
                matched = true;
            }
            i += 3;
        } else {
            if pattern[i] == c {
                matched = true;
            }
            i += 1;
        }
    }

    if i < pattern.len() && pattern[i] == b']' {
        i += 1;
    }

    (if negate { !matched } else { matched }, i)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pattern_matching() {
        assert!(pattern_matches(b"*", b"anything"));
        assert!(pattern_matches(b"hello*", b"hello world"));
        assert!(pattern_matches(b"*world", b"hello world"));
        assert!(pattern_matches(b"h?llo", b"hello"));
        assert!(pattern_matches(b"h[ae]llo", b"hello"));
        assert!(pattern_matches(b"h[ae]llo", b"hallo"));
        assert!(!pattern_matches(b"h[ae]llo", b"hillo"));
        assert!(pattern_matches(b"news.*", b"news.sports"));
        assert!(pattern_matches(b"user.[0-9]*", b"user.123"));
    }

    #[test]
    fn test_pubsub_publish_message_single_subscriber() {
        let pubsub = PubSub::new();
        let (id, mut rx) = pubsub.register();
        let counts = pubsub.subscribe(id, &[Bytes::from_static(b"chan")]);
        assert_eq!(counts, vec![1]);

        let sent = pubsub.publish(b"chan", Bytes::from_static(b"hello"));
        assert_eq!(sent, 1);

        let msg = rx.try_recv().unwrap();
        match msg {
            PubSubMessage::Message { channel, message } => {
                assert_eq!(channel, Bytes::from_static(b"chan"));
                assert_eq!(message, Bytes::from_static(b"hello"));
            }
            _ => panic!("unexpected message type"),
        }
    }

    #[test]
    fn test_pubsub_publish_message_multiple_subscribers() {
        let pubsub = PubSub::new();
        let (id1, mut rx1) = pubsub.register();
        let (id2, mut rx2) = pubsub.register();
        pubsub.subscribe(id1, &[Bytes::from_static(b"chan")]);
        pubsub.subscribe(id2, &[Bytes::from_static(b"chan")]);

        let sent = pubsub.publish(b"chan", Bytes::from_static(b"hi"));
        assert_eq!(sent, 2);

        for rx in [&mut rx1, &mut rx2] {
            let msg = rx.try_recv().unwrap();
            match msg {
                PubSubMessage::Message { channel, message } => {
                    assert_eq!(channel, Bytes::from_static(b"chan"));
                    assert_eq!(message, Bytes::from_static(b"hi"));
                }
                _ => panic!("unexpected message type"),
            }
        }
    }
}
