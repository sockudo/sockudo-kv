use bytes::{Buf, Bytes, BytesMut};

use super::types::RespValue;
use crate::error::{Error, Result};

/// Zero-copy RESP parser
/// Parses RESP protocol with minimal allocations
pub struct Parser;

impl Parser {
    /// Parse a complete RESP value from buffer.
    /// Returns None if buffer doesn't contain a complete message.
    /// Advances buffer past the parsed data.
    #[inline]
    pub fn parse(buf: &mut BytesMut) -> Result<Option<RespValue>> {
        if buf.is_empty() {
            return Ok(None);
        }

        match Self::parse_value(buf) {
            Ok(Some((value, consumed))) => {
                buf.advance(consumed);
                Ok(Some(value))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Parse value, returning (value, bytes_consumed) or None if incomplete
    fn parse_value(buf: &[u8]) -> Result<Option<(RespValue, usize)>> {
        if buf.is_empty() {
            return Ok(None);
        }

        match buf[0] {
            b'+' => Self::parse_simple_string(buf),
            b'-' => Self::parse_error(buf),
            b':' => Self::parse_integer(buf),
            b'$' => Self::parse_bulk_string(buf),
            b'*' => Self::parse_array(buf),
            // Inline commands (telnet-style): PING\r\n or SET key value\r\n
            _ => Self::parse_inline(buf),
        }
    }

    /// Parse inline command (telnet-style)
    /// Format: COMMAND arg1 arg2 ...\r\n
    /// Supports quoted strings: SET key "hello world"
    fn parse_inline(buf: &[u8]) -> Result<Option<(RespValue, usize)>> {
        let crlf_pos = match Self::find_crlf(buf) {
            Some(pos) => pos,
            None => return Ok(None), // Need more data
        };

        // Empty line - need more data
        if crlf_pos == 0 {
            return Ok(Some((RespValue::Array(vec![]), 2)));
        }

        let line = &buf[..crlf_pos];
        let mut args: Vec<RespValue> = Vec::with_capacity(8);
        let mut i = 0;

        while i < line.len() {
            // Skip whitespace
            while i < line.len() && (line[i] == b' ' || line[i] == b'\t') {
                i += 1;
            }

            if i >= line.len() {
                break;
            }

            // Check for quoted string
            if line[i] == b'"' {
                i += 1; // Skip opening quote
                let start = i;
                let mut escaped = false;
                let mut unescaped = Vec::new();

                while i < line.len() {
                    if escaped {
                        match line[i] {
                            b'n' => unescaped.push(b'\n'),
                            b'r' => unescaped.push(b'\r'),
                            b't' => unescaped.push(b'\t'),
                            b'\\' => unescaped.push(b'\\'),
                            b'"' => unescaped.push(b'"'),
                            c => unescaped.push(c),
                        }
                        escaped = false;
                    } else if line[i] == b'\\' {
                        escaped = true;
                    } else if line[i] == b'"' {
                        break;
                    } else {
                        unescaped.push(line[i]);
                    }
                    i += 1;
                }

                // Skip closing quote if present
                if i < line.len() && line[i] == b'"' {
                    i += 1;
                }

                if unescaped.is_empty() && start < i {
                    // No escapes, use zero-copy
                    let end = if i > 0 && line.get(i - 1) == Some(&b'"') {
                        i - 1
                    } else {
                        i
                    };
                    args.push(RespValue::BulkString(Bytes::copy_from_slice(
                        &line[start..end.min(line.len())],
                    )));
                } else {
                    args.push(RespValue::BulkString(Bytes::from(unescaped)));
                }
            } else if line[i] == b'\'' {
                // Single-quoted string (no escape processing)
                i += 1;
                let start = i;
                while i < line.len() && line[i] != b'\'' {
                    i += 1;
                }
                args.push(RespValue::BulkString(Bytes::copy_from_slice(
                    &line[start..i],
                )));
                if i < line.len() && line[i] == b'\'' {
                    i += 1;
                }
            } else {
                // Unquoted token
                let start = i;
                while i < line.len() && line[i] != b' ' && line[i] != b'\t' {
                    i += 1;
                }
                args.push(RespValue::BulkString(Bytes::copy_from_slice(
                    &line[start..i],
                )));
            }
        }

        if args.is_empty() {
            return Ok(Some((RespValue::Array(vec![]), crlf_pos + 2)));
        }

        Ok(Some((RespValue::Array(args), crlf_pos + 2)))
    }

    /// Find \r\n in buffer, return position of \r
    #[inline]
    fn find_crlf(buf: &[u8]) -> Option<usize> {
        // Fast path: use memchr for speed
        memchr::memchr(b'\r', buf).and_then(|pos| {
            if pos + 1 < buf.len() && buf[pos + 1] == b'\n' {
                Some(pos)
            } else {
                None
            }
        })
    }

    fn parse_simple_string(buf: &[u8]) -> Result<Option<(RespValue, usize)>> {
        match Self::find_crlf(buf) {
            Some(pos) => {
                let s = Bytes::copy_from_slice(&buf[1..pos]);
                Ok(Some((RespValue::SimpleString(s), pos + 2)))
            }
            None => Ok(None),
        }
    }

    fn parse_error(buf: &[u8]) -> Result<Option<(RespValue, usize)>> {
        match Self::find_crlf(buf) {
            Some(pos) => {
                let s = Bytes::copy_from_slice(&buf[1..pos]);
                Ok(Some((RespValue::Error(s), pos + 2)))
            }
            None => Ok(None),
        }
    }

    fn parse_integer(buf: &[u8]) -> Result<Option<(RespValue, usize)>> {
        match Self::find_crlf(buf) {
            Some(pos) => {
                let s = std::str::from_utf8(&buf[1..pos])
                    .map_err(|_| Error::Protocol("invalid integer".into()))?;
                let n: i64 = s
                    .parse()
                    .map_err(|_| Error::Protocol("invalid integer".into()))?;
                Ok(Some((RespValue::Integer(n), pos + 2)))
            }
            None => Ok(None),
        }
    }

    fn parse_bulk_string(buf: &[u8]) -> Result<Option<(RespValue, usize)>> {
        let crlf_pos = match Self::find_crlf(buf) {
            Some(pos) => pos,
            None => return Ok(None),
        };

        let len_str = std::str::from_utf8(&buf[1..crlf_pos])
            .map_err(|_| Error::Protocol("invalid bulk length".into()))?;
        let len: i64 = len_str
            .parse()
            .map_err(|_| Error::Protocol("invalid bulk length".into()))?;

        if len == -1 {
            return Ok(Some((RespValue::Null, crlf_pos + 2)));
        }

        let len = len as usize;
        let data_start = crlf_pos + 2;
        let data_end = data_start + len;
        let total_len = data_end + 2; // +2 for trailing \r\n

        if buf.len() < total_len {
            return Ok(None);
        }

        let data = Bytes::copy_from_slice(&buf[data_start..data_end]);
        Ok(Some((RespValue::BulkString(data), total_len)))
    }

    fn parse_array(buf: &[u8]) -> Result<Option<(RespValue, usize)>> {
        let crlf_pos = match Self::find_crlf(buf) {
            Some(pos) => pos,
            None => return Ok(None),
        };

        let len_str = std::str::from_utf8(&buf[1..crlf_pos])
            .map_err(|_| Error::Protocol("invalid array length".into()))?;
        let len: i64 = len_str
            .parse()
            .map_err(|_| Error::Protocol("invalid array length".into()))?;

        if len == -1 {
            return Ok(Some((RespValue::Null, crlf_pos + 2)));
        }

        let len = len as usize;
        let mut items = Vec::with_capacity(len);
        let mut offset = crlf_pos + 2;

        for _ in 0..len {
            match Self::parse_value(&buf[offset..])? {
                Some((value, consumed)) => {
                    items.push(value);
                    offset += consumed;
                }
                None => return Ok(None),
            }
        }

        Ok(Some((RespValue::Array(items), offset)))
    }
}

/// Command extracted from RESP array
#[derive(Debug)]
pub struct Command {
    pub name: Bytes,
    pub args: Vec<Bytes>,
}

impl Command {
    /// Extract command from RESP value
    pub fn from_resp(value: RespValue) -> Result<Self> {
        match value {
            RespValue::Array(items) if !items.is_empty() => {
                let mut iter = items.into_iter();
                let name = match iter.next().unwrap() {
                    RespValue::BulkString(b) => b,
                    RespValue::SimpleString(b) => b,
                    _ => return Err(Error::Protocol("command name must be string".into())),
                };

                let args: Result<Vec<Bytes>> = iter
                    .map(|v| match v {
                        RespValue::BulkString(b) | RespValue::SimpleString(b) => Ok(b),
                        RespValue::Integer(n) => Ok(Bytes::from(n.to_string())),
                        _ => Err(Error::Protocol("invalid argument type".into())),
                    })
                    .collect();

                Ok(Command { name, args: args? })
            }
            _ => Err(Error::Protocol("expected array".into())),
        }
    }

    /// Get command name as slice (for case-insensitive matching)
    #[inline]
    pub fn name(&self) -> &[u8] {
        &self.name
    }

    /// Check if command name matches (case-insensitive)
    #[inline]
    pub fn is_command(&self, cmd: &[u8]) -> bool {
        self.name.len() == cmd.len()
            && self
                .name
                .iter()
                .zip(cmd)
                .all(|(a, b)| a.eq_ignore_ascii_case(b))
    }
}
