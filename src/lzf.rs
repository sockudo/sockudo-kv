//! LZF compression implementation
//!
//! Pure Rust port of the LZF compression algorithm used by Redis.
//! Format:
//! - 000LLLLL <L+1 bytes>   : literal run, L+1 = 1..33 bytes
//! - LLLooooo oooooooo      : back reference, L+1 = 1..7 bytes, offset = o+1 (1..4096)
//! - 111ooooo LLLLLLLL oooo : back reference, L+8 bytes, offset = o+1 (1..4096)

const HLOG: usize = 14;
const HSIZE: usize = 1 << HLOG;
const MAX_LIT: usize = 32;
const MAX_OFF: usize = 4096;
const MAX_REF: usize = 264; // 8 + 256

/// Compress data using LZF algorithm
/// Returns None if compression doesn't reduce size
pub fn compress(input: &[u8]) -> Option<Vec<u8>> {
    if input.len() < 4 {
        return None;
    }

    let mut output = Vec::with_capacity(input.len());
    let mut htab: [usize; HSIZE] = [0; HSIZE];

    let mut ip = 0; // input pointer
    let mut lit = 0; // literal run length
    output.push(0); // placeholder for literal length

    // Hash function
    let hash = |p: usize| -> usize {
        let v = (input[p] as usize) << 16 | (input[p + 1] as usize) << 8 | (input[p + 2] as usize);
        ((v >> (24 - HLOG)).wrapping_sub(v)) as usize & (HSIZE - 1)
    };

    while ip < input.len().saturating_sub(2) {
        let hval = hash(ip);
        let ref_pos = htab[hval];
        htab[hval] = ip + 1; // Store 1-based to distinguish from 0

        // Check for match
        if ref_pos > 0 {
            let ref_idx = ref_pos - 1;
            let off = ip.wrapping_sub(ref_idx).wrapping_sub(1);

            if off < MAX_OFF
                && ref_idx < ip
                && ip + 2 < input.len()
                && ref_idx + 2 < input.len()
                && input[ref_idx] == input[ip]
                && input[ref_idx + 1] == input[ip + 1]
                && input[ref_idx + 2] == input[ip + 2]
            {
                // Found a match - calculate length
                let mut len = 3;
                let max_len = (input.len() - ip).min(MAX_REF);

                while len < max_len && input[ref_idx + len] == input[ip + len] {
                    len += 1;
                }

                // Write literal run if any
                if lit > 0 {
                    let lit_start = output.len() - lit - 1;
                    output[lit_start] = (lit - 1) as u8;
                    lit = 0;
                } else {
                    output.pop(); // Remove placeholder
                }

                len -= 2; // Encode as len-2

                // Write back reference
                if len < 7 {
                    output.push(((len << 5) | (off >> 8)) as u8);
                } else {
                    output.push((7 << 5 | (off >> 8)) as u8);
                    output.push((len - 7) as u8);
                }
                output.push(off as u8);

                ip += len + 2;
                output.push(0); // New literal placeholder
                continue;
            }
        }

        // No match - add literal
        lit += 1;
        output.push(input[ip]);
        ip += 1;

        if lit == MAX_LIT {
            let lit_start = output.len() - lit - 1;
            output[lit_start] = (lit - 1) as u8;
            lit = 0;
            output.push(0); // New placeholder
        }
    }

    // Handle remaining bytes
    while ip < input.len() {
        lit += 1;
        output.push(input[ip]);
        ip += 1;

        if lit == MAX_LIT {
            let lit_start = output.len() - lit - 1;
            output[lit_start] = (lit - 1) as u8;
            lit = 0;
            output.push(0); // New placeholder
        }
    }

    // Finalize last literal run
    if lit > 0 {
        let lit_start = output.len() - lit - 1;
        output[lit_start] = (lit - 1) as u8;
    } else {
        output.pop(); // Remove unused placeholder
    }

    // Only return if compression was beneficial
    if output.len() < input.len() {
        Some(output)
    } else {
        None
    }
}

/// Decompress LZF-compressed data
/// Returns the decompressed data or an error message
pub fn decompress(input: &[u8], out_len: usize) -> Result<Vec<u8>, &'static str> {
    let mut output = Vec::with_capacity(out_len);
    let mut ip = 0;

    while ip < input.len() {
        let ctrl = input[ip] as usize;
        ip += 1;

        if ctrl < 32 {
            // Literal run: ctrl+1 bytes
            let len = ctrl + 1;

            if ip + len > input.len() {
                return Err("Invalid LZF: truncated literal");
            }
            if output.len() + len > out_len {
                return Err("Invalid LZF: output overflow");
            }

            output.extend_from_slice(&input[ip..ip + len]);
            ip += len;
        } else {
            // Back reference
            let mut len = ctrl >> 5;

            if ip >= input.len() {
                return Err("Invalid LZF: truncated backref");
            }

            if len == 7 {
                len += input[ip] as usize;
                ip += 1;
                if ip >= input.len() {
                    return Err("Invalid LZF: truncated backref length");
                }
            }

            let off = ((ctrl & 0x1f) << 8) + (input[ip] as usize) + 1;
            ip += 1;

            if off > output.len() {
                return Err("Invalid LZF: backref offset too large");
            }

            len += 2;
            if output.len() + len > out_len {
                return Err("Invalid LZF: output overflow");
            }

            let ref_start = output.len() - off;
            for i in 0..len {
                let byte = output[ref_start + i];
                output.push(byte);
            }
        }
    }

    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lzf_roundtrip() {
        let data = b"Hello, World! Hello, World! Hello, World!";
        if let Some(compressed) = compress(data) {
            assert!(compressed.len() < data.len());
            let decompressed = decompress(&compressed, data.len()).unwrap();
            assert_eq!(&decompressed, data);
        }
    }

    #[test]
    fn test_lzf_incompressible() {
        let data: Vec<u8> = (0..100).collect();
        // Random data may not compress
        let _ = compress(&data);
    }

    #[test]
    fn test_lzf_small_input() {
        let data = b"ab";
        assert!(compress(data).is_none()); // Too small
    }

    #[test]
    fn test_lzf_repeated() {
        let data = vec![0xAA; 1000];
        if let Some(compressed) = compress(&data) {
            assert!(compressed.len() < data.len());
            let decompressed = decompress(&compressed, data.len()).unwrap();
            assert_eq!(decompressed, data);
        }
    }
}
