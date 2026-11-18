//! Adaptive Probability Map (APM) / Secondary Symbol Estimation (SSE).
//!
//! Refines the mixer's output probability using piecewise linear interpolation
//! over an array of adaptive probability counters conditioned on context.

use crate::coder::SCALE;
use crate::models::state::BitCounter;

/// Number of discrete probability bins for interpolation.
pub const APM_BINS: usize = 33;

/// An Adaptive Probability Map table.
pub struct AdaptiveProbabilityMap {
    /// 2D table of adaptive counters: `table[context][bin]`
    table: Vec<[BitCounter; APM_BINS]>,
    num_contexts: usize,
}

impl AdaptiveProbabilityMap {
    pub fn new(num_contexts: usize) -> Self {
        let mut table = Vec::with_capacity(num_contexts);
        for _ in 0..num_contexts {
            let mut row = [BitCounter::new(); APM_BINS];
            // Initialize each bin with prior corresponding to its bin center
            for (bin, slot) in row.iter_mut().enumerate() {
                let bin_p = ((bin as u32 * (SCALE - 1)) / (APM_BINS as u32 - 1)).clamp(1, SCALE - 1);
                // Pre-seed counter to reflect initial probability with Laplace smoothing
                let target_num = ((bin_p * 32 + 2048) / SCALE).clamp(1, 31) as u8;
                slot.c1 = target_num - 1;
                slot.c0 = 30 - slot.c1;
            }
            table.push(row);
        }

        Self {
            table,
            num_contexts,
        }
    }

    /// Evaluates and refines probability `p` using linear interpolation between adjacent bins.
    #[inline(always)]
    pub fn evaluate(&self, context: usize, p: u32) -> u32 {
        let ctx = context % self.num_contexts;
        let p_clamped = p.clamp(1, SCALE - 1);

        // Find bin index in [0, APM_BINS - 2] and fraction
        let scaled = (p_clamped * (APM_BINS as u32 - 1)) as usize;
        let bin = (scaled / SCALE as usize).min(APM_BINS - 2);
        let frac = (scaled % SCALE as usize) as u32;

        let p_low = self.table[ctx][bin].p1();
        let p_high = self.table[ctx][bin + 1].p1();

        // Linear interpolation: p_low + (p_high - p_low) * frac / 4096
        let delta = (p_high as i64 - p_low as i64) * frac as i64;
        let interpolated = p_low as i64 + (delta >> 12);
        (interpolated as u32).clamp(1, SCALE - 1)
    }

    /// Updates APM counters with observed bit.
    #[inline(always)]
    pub fn update(&mut self, context: usize, p: u32, bit: u8) {
        let ctx = context % self.num_contexts;
        let p_clamped = p.clamp(1, SCALE - 1);

        let scaled = (p_clamped * (APM_BINS as u32 - 1)) as usize;
        let bin = (scaled / SCALE as usize).min(APM_BINS - 2);
        let frac = (scaled % SCALE as usize) as u32;

        // Weight updates to the two closest bins according to fractional distance
        if frac > 2048 {
            self.table[ctx][bin + 1].update(bit);
            if frac > 3500 {
                // Dominant bin
            } else {
                self.table[ctx][bin].update(bit);
            }
        } else {
            self.table[ctx][bin].update(bit);
            if frac > 500 {
                self.table[ctx][bin + 1].update(bit);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_apm_initial_calibration() {
        let apm = AdaptiveProbabilityMap::new(4);
        // At p = 2048 (50%), evaluate should return near 2048
        let out = apm.evaluate(0, 2048);
        assert!((out as i32 - 2048).abs() < 100, "APM 50% prior was {}", out);
    }

    #[test]
    fn test_apm_bias_correction() {
        let mut apm = AdaptiveProbabilityMap::new(1);
        // Suppose mixer consistently predicts 3000, but true bits are always 0
        for _ in 0..20 {
            apm.update(0, 3000, 0);
        }
        let refined = apm.evaluate(0, 3000);
        assert!(refined < 2000, "APM failed to correct optimistic mixer: refined = {}", refined);
    }
}
