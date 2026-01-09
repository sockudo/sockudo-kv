//! Redis Cluster Wire Protocol structures
//!
//! Binary-compatible with Redis Cluster protocol.
//! Signature: "RCmb" (Redis Cluster message binary)

use bytes::{BufMut, Bytes, BytesMut};

pub const CLUSTER_PROTO_VER: u16 = 1;

// Message types
pub const CLUSTERMSG_TYPE_PING: u16 = 0;
pub const CLUSTERMSG_TYPE_PONG: u16 = 1;
pub const CLUSTERMSG_TYPE_MEET: u16 = 2;
pub const CLUSTERMSG_TYPE_FAIL: u16 = 3;
pub const CLUSTERMSG_TYPE_PUBLISH: u16 = 4;
pub const CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST: u16 = 5;
pub const CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK: u16 = 6;
pub const CLUSTERMSG_TYPE_UPDATE: u16 = 7;
pub const CLUSTERMSG_TYPE_MF_START: u16 = 8;
pub const CLUSTERMSG_TYPE_MODULE: u16 = 9;

// Flags
pub const CLUSTERMSG_FLAG_0: u16 = 0; // No flags

#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct ClusterMsgRcStart {
    pub sig: [u8; 4], // "RCmb"
    pub totlen: u32,  // Total length of message
    pub ver: u16,     // Protocol version
    pub port: u16,    // TCP port
    pub type_: u16,   // Message type
    pub count: u16,   // Number of gossip/extensions
    pub current_epoch: u64,
    pub config_epoch: u64,
    pub offset: u64,         // Replication offset
    pub sender: [u8; 40],    // Sender node ID
    pub myslots: [u8; 2048], // 16384 bits = 2048 bytes
    pub slaveof: [u8; 40],   // Master ID if slave
    pub myip: [u8; 46],      // Sender IP (char array)
    pub notused1: [u8; 32],  // Reserved
    pub pport: u16,          // Plaintext port (if TLS)
    pub cport: u16,          // Cluster bus port
    pub flags: u16,          // Node flags
    pub state: u8,           // Cluster state
    pub mflags: [u8; 3],     // Msg flags
}

// Ensure size matches standard Redis (which is complex due to padding, but we try to be close)
// Redis Cluster header is ~2200-2300 bytes.
// We'll use a safer approach: Parse/Serialize manually rather than relying on `repr(packed)` memory layout strictly across diverse archs,
// but `repr(packed)` helps with network buffer estimation.
//
// However, in Rust, accessing packed fields is unsafe.
// A better approach for the "Agentic" context is to implement `from_bytes` and `to_bytes`.

#[derive(Debug, Clone)]
pub struct ClusterMsg {
    pub sig: [u8; 4],
    pub totlen: u32,
    pub ver: u16,
    pub port: u16,
    pub type_: u16,
    pub count: u16,
    pub current_epoch: u64,
    pub config_epoch: u64,
    pub offset: u64,
    pub sender: Bytes, // 40 chars
    pub myslots: [u8; 2048],
    pub slaveof: Bytes, // 40 chars
    pub myip: [u8; 46],
    pub pport: u16,
    pub cport: u16,
    pub flags: u16,
    pub state: u8,
    // Payload depends on type
    pub data: ClusterMsgData,
}

#[derive(Debug, Clone)]
pub enum ClusterMsgData {
    Ping(Vec<ClusterMsgPing>),
    Pong(Vec<ClusterMsgPing>),
    Meet(Vec<ClusterMsgPing>),
    // Other types omitted for MVP
    Empty,
}

#[derive(Debug, Clone)]
pub struct ClusterMsgPing {
    // Gossip info
    pub nodename: Bytes, // 40 chars
    pub ping_sent: u32,
    pub pong_received: u32,
    pub ip: [u8; 46],
    pub port: u16,
    pub cport: u16,
    pub flags: u16,
    pub notused1: u32,
}

impl ClusterMsg {
    pub fn new(type_: u16) -> Self {
        Self {
            sig: *b"RCmb",
            totlen: 0, // Will be set on serialize
            ver: CLUSTER_PROTO_VER,
            port: 0,
            type_,
            count: 0,
            current_epoch: 0,
            config_epoch: 0,
            offset: 0,
            sender: Bytes::from_static(&[0u8; 40]),
            myslots: [0u8; 2048],
            slaveof: Bytes::from_static(&[0u8; 40]),
            myip: [0u8; 46],
            pport: 0,
            cport: 0,
            flags: 0,
            state: 0,
            data: ClusterMsgData::Empty,
        }
    }

    pub fn serialize(&self, buf: &mut BytesMut) {
        buf.put_slice(&self.sig);
        // TOTLEN placeholder (index 4)
        let len_idx = buf.len();
        buf.put_u32(0);

        buf.put_u16(self.ver);
        buf.put_u16(self.port);
        buf.put_u16(self.type_);
        buf.put_u16(self.count);
        buf.put_u64(self.current_epoch);
        buf.put_u64(self.config_epoch);
        buf.put_u64(self.offset);

        // Sender (40 bytes)
        let mut sender_padded = [0u8; 40];
        let len = self.sender.len().min(40);
        sender_padded[..len].copy_from_slice(&self.sender[..len]);
        buf.put_slice(&sender_padded);

        buf.put_slice(&self.myslots);

        // Slaveof (40 bytes)
        let mut slaveof_padded = [0u8; 40];
        let len = self.slaveof.len().min(40);
        slaveof_padded[..len].copy_from_slice(&self.slaveof[..len]);
        buf.put_slice(&slaveof_padded);

        buf.put_slice(&self.myip);

        // notused1 (32 bytes)
        buf.put_slice(&[0u8; 32]);

        buf.put_u16(self.pport);
        buf.put_u16(self.cport);
        buf.put_u16(self.flags);
        buf.put_u8(self.state);

        // mflags (3 bytes)
        buf.put_slice(&[0u8; 3]);

        // Payload
        match &self.data {
            ClusterMsgData::Ping(gossip)
            | ClusterMsgData::Pong(gossip)
            | ClusterMsgData::Meet(gossip) => {
                for g in gossip {
                    let mut node_padded = [0u8; 40];
                    let len = g.nodename.len().min(40);
                    node_padded[..len].copy_from_slice(&g.nodename[..len]);
                    buf.put_slice(&node_padded);

                    buf.put_u32(g.ping_sent);
                    buf.put_u32(g.pong_received);
                    buf.put_slice(&g.ip);
                    buf.put_u16(g.port);
                    buf.put_u16(g.cport);
                    buf.put_u16(g.flags);
                    buf.put_u32(0); // notused
                }
            }
            _ => {}
        }

        // Fix up totlen
        let total_len = buf.len() as u32;
        // Poke into the buffer to set totlen
        // This relies on BytesMut logic
        let bytes = &mut buf[len_idx..len_idx + 4];
        bytes.copy_from_slice(&total_len.to_be_bytes());
    }

    // NOTE: In a full impl, we'd have a `parse` method here.
    // For now we trust our own serialization for sending.
}

/// Calculate hashing slot for a key.
/// Redis Cluster uses CRC16(key) & 16383.
/// It also supports hash tags: {tag}.
pub fn key_slot(key: &[u8]) -> u16 {
    let mut key = key;

    // Check for hash tags
    // 1. Find the first occurrence of '{'
    if let Some(start) = key.iter().position(|&b| b == b'{') {
        // 2. Find the *first* occurrence of '}' *after* the '{'
        // Slice after start+1
        if let Some(end_offset) = key[start + 1..].iter().position(|&b| b == b'}') {
            let end = start + 1 + end_offset;
            // 3. If there is at least one character between them, scan only that substring
            if end > start + 1 {
                key = &key[start + 1..end];
            }
        }
    }

    crc16(key) & 16383
}

/// CRC16-CCITT implementation (XMODEM variant as used by Redis)
/// Polynomial: 0x1021
fn crc16(buf: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in buf {
        crc = ((crc << 8) ^ CRC16_TABLE[((crc >> 8) ^ (byte as u16)) as usize]) & 0xFFFF;
    }
    crc
}

// Precomputed table for CRC16 (XMODEM)
const CRC16_TABLE: [u16; 256] = [
    0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7, 0x8108, 0x9129, 0xa14a, 0xb16b,
    0xc18c, 0xd1ad, 0xe1ce, 0xf1ef, 0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
    0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de, 0x2462, 0x3443, 0x0420, 0x1401,
    0x64e6, 0x74c7, 0x44a4, 0x5485, 0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
    0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4, 0xb75b, 0xa77a, 0x9719, 0x8738,
    0xf7df, 0xe7fe, 0xd79d, 0xc7bc, 0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
    0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b, 0x5af5, 0x4ad4, 0x7ab7, 0x6a96,
    0x1a71, 0x0a50, 0x3a33, 0x2a12, 0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
    0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41, 0xedae, 0xfd8f, 0xcdec, 0xddcd,
    0xad2a, 0xbd0b, 0x8d68, 0x9d49, 0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
    0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78, 0x9188, 0x81a9, 0xb1ca, 0xa1eb,
    0xd10c, 0xc12d, 0xf14e, 0xe16f, 0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
    0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e, 0x02b1, 0x1290, 0x22f3, 0x32d2,
    0x4235, 0x5214, 0x6277, 0x7256, 0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
    0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405, 0xa7db, 0xb7fa, 0x8799, 0x97b8,
    0xe75f, 0xf77e, 0xc71d, 0xd73c, 0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
    0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab, 0x5844, 0x4865, 0x7806, 0x6827,
    0x18c0, 0x08e1, 0x3882, 0x28a3, 0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
    0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92, 0xfd2e, 0xed0f, 0xdd6c, 0xcd4d,
    0xbdaa, 0xad8b, 0x9de8, 0x8dc9, 0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
    0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8, 0x6e17, 0x7e36, 0x4e55, 0x5e74,
    0x2eb3, 0x3e92, 0x0ed1, 0x1ef0,
];
