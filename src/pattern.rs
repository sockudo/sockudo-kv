/// Glob pattern matching implementation ported from Redis's util.c stringmatchlen.
/// Optimized with SIMD via memchr for fast scanning.
///
/// Supports:
///     * - Matches any sequence of characters (including empty)
///     ? - Matches any single character
///     [abc] - Matches any character in the set
///     [a-z] - Matches any character in the range
///     [^...] - Negates the set
///     \x - Matches generic character x
///
/// Returns true if the string matches the pattern.
pub fn stringmatch(pattern: &str, text: &str, nocase: bool) -> bool {
    stringmatch_impl(pattern.as_bytes(), text.as_bytes(), nocase)
}

fn stringmatch_impl(pattern: &[u8], text: &[u8], nocase: bool) -> bool {
    let mut p_idx = 0;
    let mut t_idx = 0;
    let p_len = pattern.len();
    let t_len = text.len();

    while p_idx < p_len {
        let p_char = pattern[p_idx];

        match p_char {
            b'*' => {
                // Collapse multiple stars
                while p_idx + 1 < p_len && pattern[p_idx + 1] == b'*' {
                    p_idx += 1;
                }
                p_idx += 1;
                if p_idx == p_len {
                    return true; // trailing * matches everything
                }

                // SIMD optimization: if the next pattern character is a literal,
                // use memchr to find potential match positions quickly
                let next_char = pattern[p_idx];
                if !nocase
                    && next_char != b'*'
                    && next_char != b'?'
                    && next_char != b'['
                    && next_char != b'\\'
                {
                    // Fast path: scan for the next literal character using SIMD
                    let remaining_text = &text[t_idx..];
                    let mut search_pos = 0;

                    while let Some(found_offset) =
                        memchr::memchr(next_char, &remaining_text[search_pos..])
                    {
                        let actual_pos = t_idx + search_pos + found_offset;
                        if stringmatch_impl(&pattern[p_idx..], &text[actual_pos..], nocase) {
                            return true;
                        }
                        search_pos += found_offset + 1;
                    }
                    return false;
                }

                // Standard path for complex patterns or case-insensitive matching
                while t_idx <= t_len {
                    if stringmatch_impl(&pattern[p_idx..], &text[t_idx..], nocase) {
                        return true;
                    }
                    t_idx += 1;
                }
                return false;
            }
            b'?' => {
                if t_idx == t_len {
                    return false;
                }
                t_idx += 1;
                p_idx += 1;
            }
            b'[' => {
                if t_idx == t_len {
                    return false;
                }

                let text_char = no_case_helper(text[t_idx], nocase);
                p_idx += 1;

                let mut negate = false;
                if p_idx < p_len && pattern[p_idx] == b'^' {
                    negate = true;
                    p_idx += 1;
                }

                let mut matched = false;
                while p_idx < p_len && pattern[p_idx] != b']' {
                    if p_idx + 2 < p_len && pattern[p_idx + 1] == b'-' && pattern[p_idx + 2] != b']'
                    {
                        let start = no_case_helper(pattern[p_idx], nocase);
                        let end = no_case_helper(pattern[p_idx + 2], nocase);
                        if start <= text_char && text_char <= end {
                            matched = true;
                        }
                        p_idx += 3;
                    } else {
                        if no_case_helper(pattern[p_idx], nocase) == text_char {
                            matched = true;
                        }
                        p_idx += 1;
                    }
                }

                if p_idx < p_len && pattern[p_idx] == b']' {
                    p_idx += 1;
                }

                if negate {
                    matched = !matched;
                }

                if !matched {
                    return false;
                }

                t_idx += 1;
            }
            b'\\' => {
                if p_idx + 1 < p_len {
                    p_idx += 1;
                    if t_idx == t_len {
                        return false;
                    }
                    let p = no_case_helper(pattern[p_idx], nocase);
                    let c = no_case_helper(text[t_idx], nocase);

                    if p != c {
                        return false;
                    }
                    t_idx += 1;
                    p_idx += 1;
                } else {
                    // Trailing backslash - matches nothing effectively or error
                    return false;
                }
            }
            _ => {
                if t_idx == t_len {
                    return false;
                }
                let p = no_case_helper(p_char, nocase);
                let c = no_case_helper(text[t_idx], nocase);
                if p != c {
                    return false;
                }
                t_idx += 1;
                p_idx += 1;
            }
        }
    }
    t_idx == t_len
}

/// Convenience wrapper for CONFIG matching (case insensitive)
pub fn matches_pattern(pattern: &str, text: &str) -> bool {
    stringmatch(pattern, text, true)
}

/// Glob matching for binary data (case sensitive)
pub fn matches_glob(pattern: &[u8], text: &[u8]) -> bool {
    stringmatch_impl(pattern, text, false)
}

#[inline(always)]
pub fn no_case_helper(c: u8, nocase: bool) -> u8 {
    if nocase { c.to_ascii_lowercase() } else { c }
}

// ==================== SIMD-optimized String Search Utilities ====================

/// Fast substring search using memchr for the first character.
/// Returns the position of the first occurrence, or None.
#[inline]
pub fn find_substring(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    if needle.len() > haystack.len() {
        return None;
    }
    if needle.len() == 1 {
        return memchr::memchr(needle[0], haystack);
    }

    // Use memchr to find first byte, then verify the rest
    let first = needle[0];
    let rest = &needle[1..];
    let mut pos = 0;

    while let Some(idx) = memchr::memchr(first, &haystack[pos..]) {
        let start = pos + idx;
        let end = start + needle.len();
        if end > haystack.len() {
            return None;
        }
        if &haystack[start + 1..end] == rest {
            return Some(start);
        }
        pos = start + 1;
    }
    None
}

/// Fast reverse substring search using memrchr for the last character.
/// Returns the position of the last occurrence, or None.
#[inline]
pub fn rfind_substring(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(haystack.len());
    }
    if needle.len() > haystack.len() {
        return None;
    }
    if needle.len() == 1 {
        return memchr::memrchr(needle[0], haystack);
    }

    // Use memrchr to find last byte, then verify the prefix
    let last = needle[needle.len() - 1];
    let prefix = &needle[..needle.len() - 1];
    let mut end_pos = haystack.len();

    while let Some(idx) = memchr::memrchr(last, &haystack[..end_pos]) {
        if idx + 1 < needle.len() {
            return None;
        }
        let start = idx + 1 - needle.len();
        if &haystack[start..idx] == prefix {
            return Some(start);
        }
        end_pos = idx;
    }
    None
}

/// Count occurrences of a byte using SIMD-accelerated memchr iteration
#[inline]
pub fn count_byte(haystack: &[u8], byte: u8) -> usize {
    memchr::memchr_iter(byte, haystack).count()
}

/// Count occurrences of a substring
#[inline]
pub fn count_substring(haystack: &[u8], needle: &[u8]) -> usize {
    if needle.is_empty() {
        return haystack.len() + 1;
    }
    if needle.len() > haystack.len() {
        return 0;
    }
    if needle.len() == 1 {
        return count_byte(haystack, needle[0]);
    }

    let mut count = 0;
    let mut pos = 0;
    while let Some(idx) = find_substring(&haystack[pos..], needle) {
        count += 1;
        pos += idx + needle.len(); // Non-overlapping
    }
    count
}

/// Fast check if haystack contains needle
#[inline]
pub fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    find_substring(haystack, needle).is_some()
}

/// Fast check if haystack starts with prefix
#[inline]
pub fn starts_with(haystack: &[u8], prefix: &[u8]) -> bool {
    haystack.len() >= prefix.len() && &haystack[..prefix.len()] == prefix
}

/// Fast check if haystack ends with suffix
#[inline]
pub fn ends_with(haystack: &[u8], suffix: &[u8]) -> bool {
    haystack.len() >= suffix.len() && &haystack[haystack.len() - suffix.len()..] == suffix
}

/// Find all occurrences of a byte (SIMD-accelerated)
#[inline]
pub fn find_all_bytes(haystack: &[u8], byte: u8) -> Vec<usize> {
    memchr::memchr_iter(byte, haystack).collect()
}

/// Find the position of any byte from a set (useful for tokenization)
#[inline]
pub fn find_any_byte(haystack: &[u8], bytes: &[u8]) -> Option<usize> {
    match bytes.len() {
        0 => None,
        1 => memchr::memchr(bytes[0], haystack),
        2 => memchr::memchr2(bytes[0], bytes[1], haystack),
        3 => memchr::memchr3(bytes[0], bytes[1], bytes[2], haystack),
        _ => {
            // Fallback for more than 3 bytes
            for (i, &b) in haystack.iter().enumerate() {
                if bytes.contains(&b) {
                    return Some(i);
                }
            }
            None
        }
    }
}

/// Split bytes by delimiter using SIMD
#[inline]
pub fn split_by_byte(haystack: &[u8], delimiter: u8) -> Vec<&[u8]> {
    let mut result = Vec::new();
    let mut start = 0;

    for pos in memchr::memchr_iter(delimiter, haystack) {
        result.push(&haystack[start..pos]);
        start = pos + 1;
    }
    result.push(&haystack[start..]);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stringmatch() {
        assert!(stringmatch("*", "anything", false));
        assert!(stringmatch("?", "a", false));
        assert!(stringmatch("a*", "abc", false));
        assert!(stringmatch("*c", "abc", false));
        assert!(stringmatch("a*c", "abc", false));
        assert!(stringmatch("[a-c]", "b", false));
        assert!(stringmatch("[^a-c]", "d", false));
        assert!(!stringmatch("[^a-c]", "b", false));
        // Case sensitive
        assert!(stringmatch("A", "A", false));
        assert!(!stringmatch("A", "a", false));
        // Case insensitive
        assert!(stringmatch("A", "a", true));
        assert!(stringmatch("[A-C]", "b", true));
        // Escaping
        assert!(stringmatch("\\*", "*", false));
        assert!(stringmatch("a\\*b", "a*b", false));
    }

    #[test]
    fn test_find_substring() {
        assert_eq!(find_substring(b"hello world", b"world"), Some(6));
        assert_eq!(find_substring(b"hello world", b"hello"), Some(0));
        assert_eq!(find_substring(b"hello world", b"xyz"), None);
        assert_eq!(find_substring(b"hello world", b""), Some(0));
        assert_eq!(find_substring(b"aaa", b"aa"), Some(0));
    }

    #[test]
    fn test_rfind_substring() {
        assert_eq!(rfind_substring(b"hello hello", b"hello"), Some(6));
        assert_eq!(rfind_substring(b"aaaa", b"aa"), Some(2));
        assert_eq!(rfind_substring(b"abc", b"xyz"), None);
    }

    #[test]
    fn test_count_substring() {
        assert_eq!(count_substring(b"abcabc", b"abc"), 2);
        assert_eq!(count_substring(b"aaaa", b"aa"), 2); // Non-overlapping
        assert_eq!(count_byte(b"hello", b'l'), 2);
    }

    #[test]
    fn test_find_any_byte() {
        assert_eq!(find_any_byte(b"hello world", b" \t\n"), Some(5));
        assert_eq!(find_any_byte(b"hello", b" \t\n"), None);
    }

    #[test]
    fn test_split_by_byte() {
        let parts = split_by_byte(b"a,b,c", b',');
        assert_eq!(parts, vec![b"a".as_ref(), b"b".as_ref(), b"c".as_ref()]);
    }
}
