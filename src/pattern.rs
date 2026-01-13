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

                let text_char = no_case_helper(text[t_idx], nocase);
                p_idx += 1;

                let mut negate = false;
                if p_idx < p_len && pattern[p_idx] == b'^' {
                    negate = true;
                    p_idx += 1;
                }

                let mut matched = false;
                while p_idx < p_len && pattern[p_idx] != b']' {
                    if p_idx + 2 < p_len && pattern[p_idx + 1] == b'-' && pattern[p_idx + 2] != b']' {
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
