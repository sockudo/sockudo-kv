//! RESP Protocol utilities
//!
//! Re-exports and extensions for the RESP protocol.

pub use crate::protocol::RespValue;

use bytes::{Bytes, BytesMut};

/// Parse error types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    /// Need more data to complete parsing
    Incomplete,
    /// Invalid RESP format
    Invalid(String),
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Incomplete => write!(f, "incomplete data"),
            Self::Invalid(msg) => write!(f, "invalid format: {}", msg),
        }
    }
}

impl std::error::Error for ParseError {}

/// Parse a RESP command from a buffer
///
/// Returns (command_args, bytes_consumed) on success
pub fn parse_command(buffer: &BytesMut) -> Result<(Vec<Bytes>, usize), ParseError> {
    if buffer.is_empty() {
        return Err(ParseError::Incomplete);
    }

    let mut pos = 0;

    // Must start with *
    if buffer[0] != b'*' {
        // Try inline command
        return parse_inline_command(buffer);
    }

    // Parse array length
    pos += 1;
    let (array_len, len_bytes) = parse_integer(&buffer[pos..])?;
    pos += len_bytes;

    if array_len < 0 {
        return Err(ParseError::Invalid("negative array length".to_string()));
    }

    let array_len = array_len as usize;
    let mut args = Vec::with_capacity(array_len);

    // Parse each bulk string
    for _ in 0..array_len {
        if pos >= buffer.len() {
            return Err(ParseError::Incomplete);
        }

        if buffer[pos] != b'$' {
            return Err(ParseError::Invalid("expected bulk string".to_string()));
        }
        pos += 1;

        let (str_len, len_bytes) = parse_integer(&buffer[pos..])?;
        pos += len_bytes;

        if str_len < 0 {
            args.push(Bytes::new());
            continue;
        }

        let str_len = str_len as usize;
        if pos + str_len + 2 > buffer.len() {
            return Err(ParseError::Incomplete);
        }

        let data = Bytes::copy_from_slice(&buffer[pos..pos + str_len]);
        args.push(data);
        pos += str_len + 2; // +2 for \r\n
    }

    Ok((args, pos))
}

/// Parse an inline command (space-separated)
fn parse_inline_command(buffer: &BytesMut) -> Result<(Vec<Bytes>, usize), ParseError> {
    // Find end of line
    let newline_pos = buffer
        .iter()
        .position(|&b| b == b'\n')
        .ok_or(ParseError::Incomplete)?;

    let line_end = if newline_pos > 0 && buffer[newline_pos - 1] == b'\r' {
        newline_pos - 1
    } else {
        newline_pos
    };

    let line = &buffer[..line_end];

    // Split by whitespace
    let args: Vec<Bytes> = line
        .split(|&b| b == b' ' || b == b'\t')
        .filter(|s| !s.is_empty())
        .map(|s| Bytes::copy_from_slice(s))
        .collect();

    if args.is_empty() {
        return Err(ParseError::Invalid("empty command".to_string()));
    }

    Ok((args, newline_pos + 1))
}

/// Parse a RESP integer and return (value, bytes_consumed)
fn parse_integer(buffer: &[u8]) -> Result<(i64, usize), ParseError> {
    let newline_pos = buffer
        .iter()
        .position(|&b| b == b'\r' || b == b'\n')
        .ok_or(ParseError::Incomplete)?;

    let num_str = std::str::from_utf8(&buffer[..newline_pos])
        .map_err(|_| ParseError::Invalid("invalid utf8 in integer".to_string()))?;

    let value: i64 = num_str
        .parse()
        .map_err(|_| ParseError::Invalid("invalid integer".to_string()))?;

    // Skip \r\n
    let consumed = if newline_pos + 1 < buffer.len()
        && buffer[newline_pos] == b'\r'
        && buffer[newline_pos + 1] == b'\n'
    {
        newline_pos + 2
    } else {
        newline_pos + 1
    };

    Ok((value, consumed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_command() {
        let buf = BytesMut::from("*2\r\n$4\r\nPING\r\n$4\r\ntest\r\n");
        let (args, consumed) = parse_command(&buf).unwrap();

        assert_eq!(args.len(), 2);
        assert_eq!(args[0], "PING");
        assert_eq!(args[1], "test");
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn test_parse_inline_command() {
        let buf = BytesMut::from("PING\r\n");
        let (args, consumed) = parse_command(&buf).unwrap();

        assert_eq!(args.len(), 1);
        assert_eq!(args[0], "PING");
        assert_eq!(consumed, 6);
    }

    #[test]
    fn test_parse_incomplete() {
        let buf = BytesMut::from("*2\r\n$4\r\nPIN");
        let result = parse_command(&buf);
        assert_eq!(result, Err(ParseError::Incomplete));
    }
}
