use bytes::Bytes;

/// RESP2/RESP3 protocol types - optimized for zero-copy where possible
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
    /// Null bulk string: $-1\r\n (RESP2) or _\r\n (RESP3)
    Null,
    /// Null array: *-1\r\n (RESP2) or _\r\n (RESP3)
    /// Used for commands that return arrays but the key doesn't exist
    NullArray,
    /// Array: *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
    Array(Vec<RespValue>),
    /// RESP3 Map: %2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$3\r\nbaz\r\n$4\r\nqux\r\n
    Map(Vec<(RespValue, RespValue)>),
    /// RESP3 Boolean: #t\r\n or #f\r\n (falls back to integer 1/0 in RESP2)
    Boolean(bool),
    /// RESP3 Double: ,3.14159\r\n (falls back to bulk string in RESP2)
    Double(f64),
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

    /// Create a null array response (for commands that return arrays but key doesn't exist)
    #[inline]
    pub fn null_array() -> Self {
        Self::NullArray
    }

    #[inline]
    pub fn array(items: Vec<RespValue>) -> Self {
        Self::Array(items)
    }

    /// Create a RESP3 map from key-value pairs
    #[inline]
    pub fn map(pairs: Vec<(RespValue, RespValue)>) -> Self {
        Self::Map(pairs)
    }

    /// Create a RESP3 boolean (falls back to integer in RESP2)
    #[inline]
    pub fn boolean(b: bool) -> Self {
        Self::Boolean(b)
    }

    /// Create a RESP3 double (falls back to bulk string in RESP2)
    #[inline]
    pub fn double(d: f64) -> Self {
        Self::Double(d)
    }

    /// Serialize to RESP wire format (RESP2)
    pub fn serialize(&self) -> Bytes {
        let mut buf = Vec::with_capacity(64);
        self.write_to(&mut buf);
        Bytes::from(buf)
    }

    /// Write to buffer in RESP2 format (avoids allocation if buffer exists)
    pub fn write_to(&self, buf: &mut Vec<u8>) {
        self.write_to_protocol(buf, 2);
    }

    /// Write to buffer in RESP3 format
    pub fn write_to_resp3(&self, buf: &mut Vec<u8>) {
        self.write_to_protocol(buf, 3);
    }

    /// Write to buffer with specified protocol version
    pub fn write_to_protocol(&self, buf: &mut Vec<u8>, protocol: u8) {
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
                if protocol >= 3 {
                    buf.extend_from_slice(b"_\r\n");
                } else {
                    buf.extend_from_slice(b"$-1\r\n");
                }
            }
            RespValue::NullArray => {
                if protocol >= 3 {
                    buf.extend_from_slice(b"_\r\n");
                } else {
                    buf.extend_from_slice(b"*-1\r\n");
                }
            }
            RespValue::Array(items) => {
                buf.push(b'*');
                buf.extend_from_slice(itoa::Buffer::new().format(items.len()).as_bytes());
                buf.extend_from_slice(b"\r\n");
                for item in items {
                    item.write_to_protocol(buf, protocol);
                }
            }
            RespValue::Map(pairs) => {
                if protocol >= 3 {
                    // RESP3 map format: %<count>\r\n<key1><value1>...<keyN><valueN>
                    buf.push(b'%');
                    buf.extend_from_slice(itoa::Buffer::new().format(pairs.len()).as_bytes());
                    buf.extend_from_slice(b"\r\n");
                    for (key, value) in pairs {
                        key.write_to_protocol(buf, protocol);
                        value.write_to_protocol(buf, protocol);
                    }
                } else {
                    // RESP2 fallback: convert to flat array [key1, value1, key2, value2, ...]
                    buf.push(b'*');
                    buf.extend_from_slice(itoa::Buffer::new().format(pairs.len() * 2).as_bytes());
                    buf.extend_from_slice(b"\r\n");
                    for (key, value) in pairs {
                        key.write_to_protocol(buf, protocol);
                        value.write_to_protocol(buf, protocol);
                    }
                }
            }
            RespValue::Boolean(b) => {
                if protocol >= 3 {
                    // RESP3 boolean format: #t\r\n or #f\r\n
                    if *b {
                        buf.extend_from_slice(b"#t\r\n");
                    } else {
                        buf.extend_from_slice(b"#f\r\n");
                    }
                } else {
                    // RESP2 fallback: integer 1 or 0
                    if *b {
                        buf.extend_from_slice(b":1\r\n");
                    } else {
                        buf.extend_from_slice(b":0\r\n");
                    }
                }
            }
            RespValue::Double(d) => {
                if protocol >= 3 {
                    // RESP3 double format: ,<floating-point-number>\r\n
                    buf.push(b',');
                    let s = d.to_string();
                    buf.extend_from_slice(s.as_bytes());
                    buf.extend_from_slice(b"\r\n");
                } else {
                    // RESP2 fallback: bulk string
                    let s = d.to_string();
                    buf.push(b'$');
                    buf.extend_from_slice(itoa::Buffer::new().format(s.len()).as_bytes());
                    buf.extend_from_slice(b"\r\n");
                    buf.extend_from_slice(s.as_bytes());
                    buf.extend_from_slice(b"\r\n");
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null_resp2() {
        let null = RespValue::Null;
        let mut buf = Vec::new();
        null.write_to_protocol(&mut buf, 2);
        assert_eq!(buf, b"$-1\r\n");
    }

    #[test]
    fn test_null_resp3() {
        let null = RespValue::Null;
        let mut buf = Vec::new();
        null.write_to_protocol(&mut buf, 3);
        assert_eq!(buf, b"_\r\n");
    }

    #[test]
    fn test_map_resp2_as_array() {
        let map = RespValue::map(vec![
            (
                RespValue::bulk_string("key1"),
                RespValue::bulk_string("val1"),
            ),
            (RespValue::bulk_string("key2"), RespValue::integer(42)),
        ]);
        let mut buf = Vec::new();
        map.write_to_protocol(&mut buf, 2);
        // In RESP2, map is serialized as flat array [key1, val1, key2, val2]
        let expected = b"*4\r\n$4\r\nkey1\r\n$4\r\nval1\r\n$4\r\nkey2\r\n:42\r\n";
        assert_eq!(buf, expected);
    }

    #[test]
    fn test_map_resp3_as_map() {
        let map = RespValue::map(vec![
            (
                RespValue::bulk_string("key1"),
                RespValue::bulk_string("val1"),
            ),
            (RespValue::bulk_string("key2"), RespValue::integer(42)),
        ]);
        let mut buf = Vec::new();
        map.write_to_protocol(&mut buf, 3);
        // In RESP3, map is serialized with % prefix
        let expected = b"%2\r\n$4\r\nkey1\r\n$4\r\nval1\r\n$4\r\nkey2\r\n:42\r\n";
        assert_eq!(buf, expected);
    }

    #[test]
    fn test_array_with_null_resp3() {
        let arr = RespValue::array(vec![
            RespValue::bulk_string("hello"),
            RespValue::Null,
            RespValue::integer(1),
        ]);
        let mut buf = Vec::new();
        arr.write_to_protocol(&mut buf, 3);
        let expected = b"*3\r\n$5\r\nhello\r\n_\r\n:1\r\n";
        assert_eq!(buf, expected);
    }
}
