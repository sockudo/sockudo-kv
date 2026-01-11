/// Glob pattern matching implementation ported from Redis's util.c stringmatchlen.
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
                p_idx += 1;
                let mut not = false;
                if p_idx < p_len && pattern[p_idx] == b'^' {
                    not = true;
                    p_idx += 1;
                }
                let mut matched = false;
                while p_idx < p_len {
                    if pattern[p_idx] == b']' {
                        break;
                    }
                    if p_idx + 2 < p_len && pattern[p_idx + 1] == b'-' && pattern[p_idx + 2] != b']'
                    {
                        let start = pattern[p_idx];
                        let end = pattern[p_idx + 2];
                        let mut c = text[t_idx];
                        if nocase {
                            c = c.to_ascii_lowercase();
                        }
                        let mut start_c = start;
                        let mut end_c = end;
                        if nocase {
                            start_c = start.to_ascii_lowercase();
                            end_c = end.to_ascii_lowercase();
                        }
                        if c >= start_c && c <= end_c {
                            matched = true;
                        }
                        p_idx += 3;
                    } else {
                        let mut p = pattern[p_idx];
                        let mut c = text[t_idx];
                        if nocase {
                            p = p.to_ascii_lowercase();
                            c = c.to_ascii_lowercase();
                        }
                        if p == c {
                            matched = true;
                        }
                        p_idx += 1;
                    }
                }
                if not {
                    matched = !matched;
                }
                if !matched {
                    return false;
                }
                t_idx += 1;
                // Skip remaining set chars
                while p_idx < p_len && pattern[p_idx] != b']' {
                    p_idx += 1;
                }
                if p_idx < p_len {
                    p_idx += 1; // skip ']'
                }
            }
            b'\\' => {
                if p_idx + 1 < p_len {
                    p_idx += 1;
                    if t_idx == t_len {
                        return false;
                    }
                    let mut p = pattern[p_idx];
                    let mut c = text[t_idx];
                    if nocase {
                        p = p.to_ascii_lowercase();
                        c = c.to_ascii_lowercase();
                    }
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
                let mut p = p_char;
                let mut c = text[t_idx];
                if nocase {
                    p = p.to_ascii_lowercase();
                    c = c.to_ascii_lowercase();
                }
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
}
