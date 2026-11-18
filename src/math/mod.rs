//! Deterministic integer math, stretch/squash tables, hashing, and CRC-32.

pub mod tables;

use tables::{STRETCH_TABLE, SQUASH_TABLE, SQUASH_RANGE};

/// Stretches a probability `p` in `[0, 4095]` into the log-odds domain `[-2048, 2047]`.
/// 2048 (50% probability) stretches to 0.
#[inline(always)]
pub fn stretch(p: u32) -> i32 {
    let idx = (p as usize).min(4095);
    STRETCH_TABLE[idx] as i32
}

/// Squashes a stretched activation sum `x` back to a probability in `[1, 4095]`.
/// x = 0 squashes to ~2048 (50%).
#[inline(always)]
pub fn squash(x: i32) -> u32 {
    let shifted = x + 2048;
    if shifted <= 0 {
        1
    } else if shifted >= (SQUASH_RANGE as i32) - 1 {
        4095
    } else {
        SQUASH_TABLE[shifted as usize] as u32
    }
}

/// Deterministic 64-bit mixer / state hash combiner.
/// Uses SplitMix64 / Wyhash constants for excellent diffusion with zero state.
#[inline(always)]
pub fn hash_combine(seed: u64, val: u64) -> u64 {
    let mut x = seed ^ val;
    x = x.wrapping_mul(0xbf58476d1ce4e5b9);
    x ^= x >> 31;
    x = x.wrapping_mul(0x94d049bb133111eb);
    x ^= x >> 32;
    x
}

/// Deterministic hash for an arbitrary byte slice.
pub fn hash_bytes(bytes: &[u8]) -> u64 {
    let mut h = 0xcbf29ce484222325u64; // FNV-1a 64-bit offset
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3); // FNV-1a prime
    }
    h
}

/// Computes standard IEEE 802.3 CRC-32 checksum deterministically without external crates.
pub fn crc32(data: &[u8]) -> u32 {
    static CRC_TABLE: [u32; 256] = {
        let mut table = [0u32; 256];
        let mut i = 0;
        while i < 256 {
            let mut c = i as u32;
            let mut j = 0;
            while j < 8 {
                if (c & 1) != 0 {
                    c = 0xEDB88320 ^ (c >> 1);
                } else {
                    c >>= 1;
                }
                j += 1;
            }
            table[i] = c;
            i += 1;
        }
        table
    };

    let mut crc = 0xFFFF_FFFFu32;
    for &b in data {
        let idx = ((crc ^ (b as u32)) & 0xFF) as usize;
        crc = CRC_TABLE[idx] ^ (crc >> 8);
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stretch_squash_symmetry() {
        // Test midpoint
        let mid_stretch = stretch(2048);
        assert_eq!(mid_stretch, 0, "50% probability should stretch to 0");
        let mid_squash = squash(0);
        assert!((mid_squash as i32 - 2048).abs() <= 20, "squash(0) should be near 2048, got {}", mid_squash);

        // Test monotonicity: higher p -> higher stretch
        let mut prev_s = stretch(1);
        for p in 2..4095 {
            let s = stretch(p);
            assert!(s >= prev_s, "Stretch not monotonic at {}: {} < {}", p, s, prev_s);
            prev_s = s;
        }

        // Test roundtrip consistency: squash(stretch(p)) should be close to p
        for p in [100, 500, 1000, 2048, 3000, 3900] {
            let s = stretch(p);
            let p_rec = squash(s);
            let diff = (p as i32 - p_rec as i32).abs();
            assert!(diff < 50, "Roundtrip diff too large for p={}: got {}", p, p_rec);
        }
    }

    #[test]
    fn test_crc32() {
        assert_eq!(crc32(b""), 0);
        assert_eq!(crc32(b"123456789"), 0xCBF43926);
    }

    #[test]
    fn test_hash_combine_deterministic() {
        let h1 = hash_combine(12345, 67890);
        let h2 = hash_combine(12345, 67890);
        assert_eq!(h1, h2);
        assert_ne!(h1, hash_combine(12345, 67891));
    }
}
