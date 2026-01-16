//! Sentinel Events and Pub/Sub Messages
//!
//! Redis Sentinel publishes events to Pub/Sub channels for monitoring.

use std::sync::Arc;

use crate::pubsub::PubSub;
use bytes::Bytes;

/// Sentinel event types (channel names match Redis Sentinel)
#[derive(Debug, Clone)]
pub enum SentinelEvent {
    // Master events
    /// Master reset
    ResetMaster {
        master_name: String,
        ip: String,
        port: u16,
    },
    /// New slave discovered
    Slave {
        master_name: String,
        slave_ip: String,
        slave_port: u16,
        master_ip: String,
        master_port: u16,
    },
    /// New sentinel discovered
    Sentinel {
        master_name: String,
        sentinel_ip: String,
        sentinel_port: u16,
        master_ip: String,
        master_port: u16,
    },
    /// Duplicate sentinel removed
    DupSentinel {
        master_name: String,
        sentinel_ip: String,
        sentinel_port: u16,
        master_ip: String,
        master_port: u16,
    },

    // Failure detection events
    /// Instance entered SDOWN state
    SDown {
        instance_type: String,
        name: String,
        ip: String,
        port: u16,
        master_name: Option<String>,
        master_ip: Option<String>,
        master_port: Option<u16>,
    },
    /// Instance exited SDOWN state
    SDownCleared {
        instance_type: String,
        name: String,
        ip: String,
        port: u16,
        master_name: Option<String>,
        master_ip: Option<String>,
        master_port: Option<u16>,
    },
    /// Master entered ODOWN state
    ODown {
        master_name: String,
        ip: String,
        port: u16,
        quorum: u32,
    },
    /// Master exited ODOWN state
    ODownCleared {
        master_name: String,
        ip: String,
        port: u16,
    },

    // Failover events
    /// New epoch started
    NewEpoch { epoch: u64 },
    /// Trying to start failover
    TryFailover {
        master_name: String,
        ip: String,
        port: u16,
    },
    /// Elected as leader for failover
    ElectedLeader {
        master_name: String,
        ip: String,
        port: u16,
    },
    /// Failover state: selecting slave
    FailoverStateSelectSlave {
        master_name: String,
        ip: String,
        port: u16,
    },
    /// No good slave for failover
    NoGoodSlave {
        master_name: String,
        ip: String,
        port: u16,
    },
    /// Selected slave for promotion
    SelectedSlave {
        slave_ip: String,
        slave_port: u16,
        master_name: String,
        master_ip: String,
        master_port: u16,
    },
    /// Sending SLAVEOF NO ONE
    FailoverStateSendSlaveofNoone {
        slave_ip: String,
        slave_port: u16,
        master_name: String,
        master_ip: String,
        master_port: u16,
    },
    /// Waiting for promotion confirmation
    FailoverStateWaitPromotion {
        slave_ip: String,
        slave_port: u16,
        master_name: String,
        master_ip: String,
        master_port: u16,
    },
    /// Reconfiguring slaves
    FailoverStateReconfSlaves {
        master_name: String,
        ip: String,
        port: u16,
    },
    /// Slave reconfiguration sent
    SlaveReconfSent {
        slave_ip: String,
        slave_port: u16,
        master_name: String,
        master_ip: String,
        master_port: u16,
    },
    /// Slave reconfiguration in progress
    SlaveReconfInprog {
        slave_ip: String,
        slave_port: u16,
        master_name: String,
        master_ip: String,
        master_port: u16,
    },
    /// Slave reconfiguration done
    SlaveReconfDone {
        slave_ip: String,
        slave_port: u16,
        master_name: String,
        master_ip: String,
        master_port: u16,
    },
    /// Failover ended (timeout)
    FailoverEndForTimeout {
        master_name: String,
        ip: String,
        port: u16,
    },
    /// Failover ended successfully
    FailoverEnd {
        master_name: String,
        ip: String,
        port: u16,
    },
    /// Failover aborted
    FailoverAborted {
        master_name: String,
        ip: String,
        port: u16,
    },
    /// Failover detected (external)
    FailoverDetected {
        master_name: String,
        ip: String,
        port: u16,
    },
    /// Master switched
    SwitchMaster {
        master_name: String,
        old_ip: String,
        old_port: u16,
        new_ip: String,
        new_port: u16,
    },

    // TILT mode events
    /// Entered TILT mode
    Tilt,
    /// Exited TILT mode
    TiltCleared,

    // Config events
    /// Configuration updated
    ConfigUpdateQueued,
    /// Convert to slave
    ConvertToSlave {
        instance_type: String,
        name: String,
        ip: String,
        port: u16,
        master_name: String,
        master_ip: String,
        master_port: u16,
    },
}

impl SentinelEvent {
    /// Get the channel name for this event (e.g., "+sdown", "-sdown")
    pub fn channel_name(&self) -> &'static str {
        match self {
            Self::ResetMaster { .. } => "+reset-master",
            Self::Slave { .. } => "+slave",
            Self::Sentinel { .. } => "+sentinel",
            Self::DupSentinel { .. } => "-dup-sentinel",
            Self::SDown { .. } => "+sdown",
            Self::SDownCleared { .. } => "-sdown",
            Self::ODown { .. } => "+odown",
            Self::ODownCleared { .. } => "-odown",
            Self::NewEpoch { .. } => "+new-epoch",
            Self::TryFailover { .. } => "+try-failover",
            Self::ElectedLeader { .. } => "+elected-leader",
            Self::FailoverStateSelectSlave { .. } => "+failover-state-select-slave",
            Self::NoGoodSlave { .. } => "-no-good-slave",
            Self::SelectedSlave { .. } => "+selected-slave",
            Self::FailoverStateSendSlaveofNoone { .. } => "+failover-state-send-slaveof-noone",
            Self::FailoverStateWaitPromotion { .. } => "+failover-state-wait-promotion",
            Self::FailoverStateReconfSlaves { .. } => "+failover-state-reconf-slaves",
            Self::SlaveReconfSent { .. } => "+slave-reconf-sent",
            Self::SlaveReconfInprog { .. } => "+slave-reconf-inprog",
            Self::SlaveReconfDone { .. } => "+slave-reconf-done",
            Self::FailoverEndForTimeout { .. } => "+failover-end-for-timeout",
            Self::FailoverEnd { .. } => "+failover-end",
            Self::FailoverAborted { .. } => "-failover-abort",
            Self::FailoverDetected { .. } => "+failover-detected",
            Self::SwitchMaster { .. } => "+switch-master",
            Self::Tilt => "+tilt",
            Self::TiltCleared => "-tilt",
            Self::ConfigUpdateQueued => "+config-update-queued",
            Self::ConvertToSlave { .. } => "+convert-to-slave",
        }
    }

    /// Format the event message
    pub fn format_message(&self) -> String {
        match self {
            Self::ResetMaster {
                master_name,
                ip,
                port,
            } => {
                format!("master {} {} {}", master_name, ip, port)
            }
            Self::Slave {
                master_name,
                slave_ip,
                slave_port,
                master_ip,
                master_port,
            } => {
                format!(
                    "slave {}:{} {}:{} @ {} {} {}",
                    slave_ip, slave_port, slave_ip, slave_port, master_name, master_ip, master_port
                )
            }
            Self::Sentinel {
                master_name,
                sentinel_ip,
                sentinel_port,
                master_ip,
                master_port,
            } => {
                format!(
                    "sentinel {}:{} {}:{} @ {} {} {}",
                    sentinel_ip,
                    sentinel_port,
                    sentinel_ip,
                    sentinel_port,
                    master_name,
                    master_ip,
                    master_port
                )
            }
            Self::DupSentinel {
                master_name,
                sentinel_ip,
                sentinel_port,
                master_ip,
                master_port,
            } => {
                format!(
                    "sentinel {}:{} {}:{} @ {} {} {}",
                    sentinel_ip,
                    sentinel_port,
                    sentinel_ip,
                    sentinel_port,
                    master_name,
                    master_ip,
                    master_port
                )
            }
            Self::SDown {
                instance_type,
                name,
                ip,
                port,
                master_name,
                master_ip,
                master_port,
            } => {
                if let (Some(mn), Some(mi), Some(mp)) = (master_name, master_ip, master_port) {
                    format!(
                        "{} {} {} {} @ {} {} {}",
                        instance_type, name, ip, port, mn, mi, mp
                    )
                } else {
                    format!("{} {} {} {}", instance_type, name, ip, port)
                }
            }
            Self::SDownCleared {
                instance_type,
                name,
                ip,
                port,
                master_name,
                master_ip,
                master_port,
            } => {
                if let (Some(mn), Some(mi), Some(mp)) = (master_name, master_ip, master_port) {
                    format!(
                        "{} {} {} {} @ {} {} {}",
                        instance_type, name, ip, port, mn, mi, mp
                    )
                } else {
                    format!("{} {} {} {}", instance_type, name, ip, port)
                }
            }
            Self::ODown {
                master_name,
                ip,
                port,
                quorum,
            } => {
                format!("master {} {} {} #quorum {}", master_name, ip, port, quorum)
            }
            Self::ODownCleared {
                master_name,
                ip,
                port,
            } => {
                format!("master {} {} {}", master_name, ip, port)
            }
            Self::NewEpoch { epoch } => {
                format!("{}", epoch)
            }
            Self::TryFailover {
                master_name,
                ip,
                port,
            } => {
                format!("master {} {} {}", master_name, ip, port)
            }
            Self::ElectedLeader {
                master_name,
                ip,
                port,
            } => {
                format!("master {} {} {}", master_name, ip, port)
            }
            Self::FailoverStateSelectSlave {
                master_name,
                ip,
                port,
            } => {
                format!("master {} {} {}", master_name, ip, port)
            }
            Self::NoGoodSlave {
                master_name,
                ip,
                port,
            } => {
                format!("master {} {} {}", master_name, ip, port)
            }
            Self::SelectedSlave {
                slave_ip,
                slave_port,
                master_name,
                master_ip,
                master_port,
            } => {
                format!(
                    "slave {}:{} {}:{} @ {} {} {}",
                    slave_ip, slave_port, slave_ip, slave_port, master_name, master_ip, master_port
                )
            }
            Self::FailoverStateSendSlaveofNoone {
                slave_ip,
                slave_port,
                master_name,
                master_ip,
                master_port,
            } => {
                format!(
                    "slave {}:{} {}:{} @ {} {} {}",
                    slave_ip, slave_port, slave_ip, slave_port, master_name, master_ip, master_port
                )
            }
            Self::FailoverStateWaitPromotion {
                slave_ip,
                slave_port,
                master_name,
                master_ip,
                master_port,
            } => {
                format!(
                    "slave {}:{} {}:{} @ {} {} {}",
                    slave_ip, slave_port, slave_ip, slave_port, master_name, master_ip, master_port
                )
            }
            Self::FailoverStateReconfSlaves {
                master_name,
                ip,
                port,
            } => {
                format!("master {} {} {}", master_name, ip, port)
            }
            Self::SlaveReconfSent {
                slave_ip,
                slave_port,
                master_name,
                master_ip,
                master_port,
            } => {
                format!(
                    "slave {}:{} {}:{} @ {} {} {}",
                    slave_ip, slave_port, slave_ip, slave_port, master_name, master_ip, master_port
                )
            }
            Self::SlaveReconfInprog {
                slave_ip,
                slave_port,
                master_name,
                master_ip,
                master_port,
            } => {
                format!(
                    "slave {}:{} {}:{} @ {} {} {}",
                    slave_ip, slave_port, slave_ip, slave_port, master_name, master_ip, master_port
                )
            }
            Self::SlaveReconfDone {
                slave_ip,
                slave_port,
                master_name,
                master_ip,
                master_port,
            } => {
                format!(
                    "slave {}:{} {}:{} @ {} {} {}",
                    slave_ip, slave_port, slave_ip, slave_port, master_name, master_ip, master_port
                )
            }
            Self::FailoverEndForTimeout {
                master_name,
                ip,
                port,
            } => {
                format!("master {} {} {}", master_name, ip, port)
            }
            Self::FailoverEnd {
                master_name,
                ip,
                port,
            } => {
                format!("master {} {} {}", master_name, ip, port)
            }
            Self::FailoverAborted {
                master_name,
                ip,
                port,
            } => {
                format!("master {} {} {}", master_name, ip, port)
            }
            Self::FailoverDetected {
                master_name,
                ip,
                port,
            } => {
                format!("master {} {} {}", master_name, ip, port)
            }
            Self::SwitchMaster {
                master_name,
                old_ip,
                old_port,
                new_ip,
                new_port,
            } => {
                format!(
                    "{} {} {} {} {}",
                    master_name, old_ip, old_port, new_ip, new_port
                )
            }
            Self::Tilt => "#tilt mode entered".to_string(),
            Self::TiltCleared => "#tilt mode exited".to_string(),
            Self::ConfigUpdateQueued => "".to_string(),
            Self::ConvertToSlave {
                instance_type,
                name,
                ip,
                port,
                master_name,
                master_ip,
                master_port,
            } => {
                format!(
                    "{} {} {} {} @ {} {} {}",
                    instance_type, name, ip, port, master_name, master_ip, master_port
                )
            }
        }
    }
}

/// Event publisher for Sentinel
pub struct SentinelEventPublisher {
    pubsub: Arc<PubSub>,
}

impl SentinelEventPublisher {
    pub fn new(pubsub: Arc<PubSub>) -> Self {
        Self { pubsub }
    }

    /// Publish a sentinel event to the appropriate channel
    pub fn publish(&self, event: SentinelEvent) -> usize {
        let channel = event.channel_name();
        let message = event.format_message();
        self.pubsub
            .publish(channel.as_bytes(), Bytes::from(message))
    }

    /// Publish SDOWN event
    pub fn publish_sdown(
        &self,
        instance_type: &str,
        name: &str,
        ip: &str,
        port: u16,
        master: Option<(&str, &str, u16)>,
    ) {
        let (master_name, master_ip, master_port) = master
            .map(|(n, i, p)| (Some(n.to_string()), Some(i.to_string()), Some(p)))
            .unwrap_or((None, None, None));

        self.publish(SentinelEvent::SDown {
            instance_type: instance_type.to_string(),
            name: name.to_string(),
            ip: ip.to_string(),
            port,
            master_name,
            master_ip,
            master_port,
        });
    }

    /// Publish SDOWN cleared event
    pub fn publish_sdown_cleared(
        &self,
        instance_type: &str,
        name: &str,
        ip: &str,
        port: u16,
        master: Option<(&str, &str, u16)>,
    ) {
        let (master_name, master_ip, master_port) = master
            .map(|(n, i, p)| (Some(n.to_string()), Some(i.to_string()), Some(p)))
            .unwrap_or((None, None, None));

        self.publish(SentinelEvent::SDownCleared {
            instance_type: instance_type.to_string(),
            name: name.to_string(),
            ip: ip.to_string(),
            port,
            master_name,
            master_ip,
            master_port,
        });
    }

    /// Publish ODOWN event
    pub fn publish_odown(&self, master_name: &str, ip: &str, port: u16, quorum: u32) {
        self.publish(SentinelEvent::ODown {
            master_name: master_name.to_string(),
            ip: ip.to_string(),
            port,
            quorum,
        });
    }

    /// Publish switch-master event
    pub fn publish_switch_master(
        &self,
        master_name: &str,
        old_ip: &str,
        old_port: u16,
        new_ip: &str,
        new_port: u16,
    ) {
        self.publish(SentinelEvent::SwitchMaster {
            master_name: master_name.to_string(),
            old_ip: old_ip.to_string(),
            old_port,
            new_ip: new_ip.to_string(),
            new_port,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_channel_names() {
        assert_eq!(SentinelEvent::Tilt.channel_name(), "+tilt");
        assert_eq!(SentinelEvent::TiltCleared.channel_name(), "-tilt");
        assert_eq!(
            SentinelEvent::SDown {
                instance_type: "master".to_string(),
                name: "mymaster".to_string(),
                ip: "127.0.0.1".to_string(),
                port: 6379,
                master_name: None,
                master_ip: None,
                master_port: None,
            }
            .channel_name(),
            "+sdown"
        );
    }

    #[test]
    fn test_switch_master_format() {
        let event = SentinelEvent::SwitchMaster {
            master_name: "mymaster".to_string(),
            old_ip: "127.0.0.1".to_string(),
            old_port: 6379,
            new_ip: "127.0.0.1".to_string(),
            new_port: 6380,
        };

        assert_eq!(event.channel_name(), "+switch-master");
        assert_eq!(
            event.format_message(),
            "mymaster 127.0.0.1 6379 127.0.0.1 6380"
        );
    }
}
