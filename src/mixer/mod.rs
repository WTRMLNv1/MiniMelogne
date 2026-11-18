//! Logistic mixer with online stochastic gradient descent (SGD) and APM calibration.

pub mod apm;

use apm::AdaptiveProbabilityMap;
use crate::coder::SCALE;
use crate::math::{squash, stretch};

pub const DEFAULT_LEARNING_RATE: i32 = 8;
pub const WEIGHT_SCALE: i32 = 1024; // 1.0 in 10-bit fixed point

/// Multi-input online logistic mixer.
///
/// Combines stretched model predictions into a single probability using weights
/// learned dynamically through online stochastic gradient descent.
pub struct LogisticMixer {
    num_models: usize,
    num_contexts: usize,
    /// Weights indexed by `[context * num_models + model_idx]`
    weights: Vec<i32>,
    learning_rate: i32,
    apm: AdaptiveProbabilityMap,
    last_mixed_p: u32,
}

impl LogisticMixer {
    pub fn new(num_models: usize, num_contexts: usize) -> Self {
        let initial_weight = WEIGHT_SCALE / (num_models as i32).max(1);
        let weights = vec![initial_weight; num_models * num_contexts];
        let apm = AdaptiveProbabilityMap::new(num_contexts);

        Self {
            num_models,
            num_contexts,
            weights,
            learning_rate: DEFAULT_LEARNING_RATE,
            apm,
            last_mixed_p: 2048,
        }
    }

    /// Combines model probabilities `predictions` into a single calibrated probability.
    pub fn mix(&mut self, context_id: usize, predictions: &[u32]) -> u32 {
        let ctx = context_id % self.num_contexts;
        let base_idx = ctx * self.num_models;

        let mut dot: i64 = 0;
        for i in 0..self.num_models {
            let s = stretch(predictions[i]) as i64;
            let w = self.weights[base_idx + i] as i64;
            dot += s * w;
        }

        // Rescale dot product back from fixed-point weight scale
        let dot_rescaled = (dot / WEIGHT_SCALE as i64) as i32;
        let p_mixed = squash(dot_rescaled);
        self.last_mixed_p = p_mixed;

        // Pass through Secondary Symbol Estimation (APM) stage for calibration
        self.apm.evaluate(ctx, p_mixed)
    }

    /// Updates weights via stochastic gradient descent and updates APM.
    pub fn update(&mut self, context_id: usize, predictions: &[u32], actual_bit: u8) {
        let ctx = context_id % self.num_contexts;
        let base_idx = ctx * self.num_models;

        // Target: 4096 if 1, 0 if 0
        let target = if actual_bit == 1 { SCALE as i32 } else { 0 };
        let error = target - self.last_mixed_p as i32; // In [-4095, 4096]

        // Weight gradient update: w_i += lr * error * stretch(p_i)
        for i in 0..self.num_models {
            let s = stretch(predictions[i]);
            // (error * s) is in [-8M, 8M]. Scale down to keep updates stable.
            let delta = ((error as i64 * s as i64 * self.learning_rate as i64) >> 18) as i32;
            let w = &mut self.weights[base_idx + i];
            *w = (*w + delta).clamp(-16384, 16384);
        }

        // Update APM
        self.apm.update(ctx, self.last_mixed_p, actual_bit);
    }

    pub fn set_learning_rate(&mut self, lr: i32) {
        self.learning_rate = lr;
    }

    pub fn weights_for_context(&self, context_id: usize) -> &[i32] {
        let ctx = context_id % self.num_contexts;
        let base_idx = ctx * self.num_models;
        &self.weights[base_idx..base_idx + self.num_models]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mixer_online_learning() {
        let mut mixer = LogisticMixer::new(2, 1);

        // Model 0 always predicts bit = 1 accurately (p = 3800)
        // Model 1 is completely misleading / wrong (p = 200)
        let preds = [3800u32, 200u32];

        // Train on true bit = 1
        for _ in 0..50 {
            let _p = mixer.mix(0, &preds);
            mixer.update(0, &preds, 1);
        }

        let w = mixer.weights_for_context(0);
        // Model 0 should have gained massive weight, Model 1 should have negative or tiny weight
        assert!(w[0] > w[1], "Good model weight ({}) not greater than bad model weight ({})", w[0], w[1]);
        assert!(w[0] > 1000, "Good model weight didn't grow: {}", w[0]);

        // Combined prediction should now strongly favor 1
        let p_final = mixer.mix(0, &preds);
        assert!(p_final > 3500, "Mixer output was {}", p_final);
    }
}
