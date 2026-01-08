use bytes::Bytes;

/// RESP3 protocol types - optimized for zero-copy where possible
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    /// Simple string: +OK\r\n
    SimpleString(Bytes),
    /// Error: -ERR message\r\n
    Error(Bytes),
    /// Integer: :1000\r\n
    Integer(i64),
    /// Bulk string: $5\r\nhello\r\n
    BulkString(Bytes),
    /// Null bulk string: $-1\r\n
    Null,
    /// Array: *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
    Array(Vec<RespValue>),
}

impl RespValue {
    pub const OK: RespValue = RespValue::SimpleString(Bytes::from_static(b"OK"));
    pub const PONG: RespValue = RespValue::SimpleString(Bytes::from_static(b"PONG"));
    pub const QUEUED: RespValue = RespValue::SimpleString(Bytes::from_static(b"QUEUED"));

    #[inline]
    pub fn ok() -> Self {
        Self::OK
    }

    #[inline]
    pub fn error(msg: &str) -> Self {
        Self::Error(Bytes::copy_from_slice(msg.as_bytes()))
    }

    #[inline]
    pub fn integer(n: i64) -> Self {
        Self::Integer(n)
    }

    #[inline]
    pub fn bulk(data: Bytes) -> Self {
        Self::BulkString(data)
    }

    #[inline]
    pub fn bulk_string(s: &str) -> Self {
        Self::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[inline]
    pub fn null() -> Self {
        Self::Null
    }

    #[inline]
    pub fn array(items: Vec<RespValue>) -> Self {
        Self::Array(items)
    }

    /// Serialize to RESP wire format
    pub fn serialize(&self) -> Bytes {
        let mut buf = Vec::with_capacity(64);
        self.write_to(&mut buf);
        Bytes::from(buf)
    }

    /// Write to buffer (avoids allocation if buffer exists)
    pub fn write_to(&self, buf: &mut Vec<u8>) {
        match self {
            RespValue::SimpleString(s) => {
                // Fast path for common static responses
                if s.as_ptr() == Self::OK.as_bytes().unwrap().as_ptr() {
                    buf.extend_from_slice(b"+OK\r\n");
                    return;
                }
                if s.as_ptr() == Self::PONG.as_bytes().unwrap().as_ptr() {
                    buf.extend_from_slice(b"+PONG\r\n");
                    return;
                }
                if s.as_ptr() == Self::QUEUED.as_bytes().unwrap().as_ptr() {
                    buf.extend_from_slice(b"+QUEUED\r\n");
                    return;
                }
                buf.push(b'+');
                buf.extend_from_slice(s);
                buf.extend_from_slice(b"\r\n");
            }
            RespValue::Error(e) => {
                buf.push(b'-');
                buf.extend_from_slice(e);
                buf.extend_from_slice(b"\r\n");
            }
            RespValue::Integer(n) => {
                buf.push(b':');
                buf.extend_from_slice(itoa::Buffer::new().format(*n).as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RespValue::BulkString(s) => {
                buf.push(b'$');
                buf.extend_from_slice(itoa::Buffer::new().format(s.len()).as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(s);
                buf.extend_from_slice(b"\r\n");
            }
            RespValue::Null => {
                buf.extend_from_slice(b"$-1\r\n");
            }
            RespValue::Array(items) => {
                buf.push(b'*');
                buf.extend_from_slice(itoa::Buffer::new().format(items.len()).as_bytes());
                buf.extend_from_slice(b"\r\n");
                for item in items {
                    item.write_to(buf);
                }
            }
        }
    }

    /// Try to interpret as bytes
    #[inline]
    pub fn as_bytes(&self) -> Option<&Bytes> {
        match self {
            RespValue::BulkString(b) | RespValue::SimpleString(b) => Some(b),
            _ => None,
        }
    }

    /// Try to interpret as integer
    #[inline]
    pub fn as_int(&self) -> Option<i64> {
        match self {
            RespValue::Integer(n) => Some(*n),
            RespValue::BulkString(b) | RespValue::SimpleString(b) => {
                std::str::from_utf8(b).ok()?.parse().ok()
            }
            _ => None,
        }
    }
}
