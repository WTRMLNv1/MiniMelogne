//! Adaptive bit counters and state representations for context models.
//!
//! Stores count of zeros and ones with exponential decay (halving) when counts
//! exceed capacity, allowing rapid adaptation to non-stationary data streams.

use crate::coder::SCALE;

/// Compact 2-byte adaptive counter tracking counts of 0 and 1 bits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BitCounter {
    pub c0: u8,
    pub c1: u8,
}

impl Default for BitCounter {
    #[inline(always)]
    fn default() -> Self {
        Self::new()
    }
}

impl BitCounter {
    pub const MAX_COUNT: u8 = 250;

    #[inline(always)]
    pub const fn new() -> Self {
        Self { c0: 0, c1: 0 }
    }

    /// Computes P(bit == 1) in fixed point [1, 4095].
    /// Uses Laplace-like smoothing: (c1 + 1) / (c0 + c1 + 2) * 4096.
    #[inline(always)]
    pub fn p1(&self) -> u32 {
        let n0 = self.c0 as u32;
        let n1 = self.c1 as u32;
        let total = n0 + n1 + 2;
        let p = ((n1 + 1) * SCALE) / total;
        p.clamp(1, SCALE - 1)
    }

    /// Updates counter with observed bit (0 or 1).
    #[inline(always)]
    pub fn update(&mut self, bit: u8) {
        if bit == 0 {
            if self.c0 < Self::MAX_COUNT {
                self.c0 += 1;
            } else {
                // Halve counts to decay old history
                self.c0 = 126;
                self.c1 >>= 1;
            }
        } else {
            if self.c1 < Self::MAX_COUNT {
                self.c1 += 1;
            } else {
                self.c1 = 126;
                self.c0 >>= 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter_prior() {
        let c = BitCounter::new();
        // Zero counts should give exactly 50% (2048)
        assert_eq!(c.p1(), 2048);
    }

    #[test]
    fn test_counter_adaptation() {
        let mut c = BitCounter::new();
        for _ in 0..10 {
            c.update(1);
        }
        // After ten 1s and zero 0s, P(1) = (10+1)/(0+10+2) * 4096 = 11/12 * 4096 ≈ 3754
        assert!(c.p1() > 3700);

        // Now observe 0s
        for _ in 0..40 {
            c.update(0);
        }
        // P(1) should swing strongly towards 0
        assert!(c.p1() < 1000);
    }

    #[test]
    fn test_counter_saturation() {
        let mut c = BitCounter::new();
        for _ in 0..1000 {
            c.update(0);
        }
        assert!(c.c0 <= BitCounter::MAX_COUNT);
        assert!(c.p1() >= 1);
    }
}
