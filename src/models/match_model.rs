//! Match model: finds dictionary-like repetitions in past data.
//!
//! Maintains a rolling hash index of recent contexts. When a match of length >= 3
//! is detected, it predicts the exact next byte/bit from the previous occurrence with
//! confidence proportional to the match length.

use crate::coder::SCALE;
use crate::math::hash_combine;

pub struct MatchModel {
    /// Circular buffer storing recent bytes
    buffer: Vec<u8>,
    mask: usize,
    /// Direct-mapped hash table indexing 4-byte prefixes to buffer positions
    index: Vec<u32>,
    index_mask: usize,
    /// Current write position in buffer
    pos: usize,
    /// Active match position in buffer
    match_pos: usize,
    /// Active match length in bytes
    match_len: usize,
    /// Match confidence / streak counter
    expected_byte: u8,
}

impl MatchModel {
    pub fn new(log2_buf_size: usize, log2_index_size: usize) -> Self {
        let buf_size = 1usize << log2_buf_size;
        let index_size = 1usize << log2_index_size;
        Self {
            buffer: vec![0u8; buf_size],
            mask: buf_size - 1,
            index: vec![u32::MAX; index_size],
            index_mask: index_size - 1,
            pos: 0,
            match_pos: 0,
            match_len: 0,
            expected_byte: 0,
        }
    }

    #[inline(always)]
    fn hash_4bytes(&self, p: usize) -> usize {
        let b0 = self.buffer[p.wrapping_sub(4) & self.mask] as u64;
        let b1 = self.buffer[p.wrapping_sub(3) & self.mask] as u64;
        let b2 = self.buffer[p.wrapping_sub(2) & self.mask] as u64;
        let b3 = self.buffer[p.wrapping_sub(1) & self.mask] as u64;

        let mut h = hash_combine(b0, b1);
        h = hash_combine(h, b2);
        h = hash_combine(h, b3);
        (h as usize) & self.index_mask
    }

    /// Predicts P(bit == 1) based on the matched byte in history.
    pub fn predict(&self, bit_history: u8) -> u32 {
        if self.match_len < 3 {
            return 2048; // Neutral prior
        }

        // bit_history in [1, 255]: bits seen so far in this byte is (7 - leading_zeros)
        let bit_idx = 7 - bit_history.leading_zeros();
        let expected_bit = (self.expected_byte >> (7 - (bit_idx % 8))) & 1;

        // Confidence scales from ~85% (3480) for 3-byte match up to 98% (4014) for long matches
        let confidence = (3200 + (self.match_len.min(16) as u32 * 50)).min(SCALE - 50);

        if expected_bit == 1 {
            confidence
        } else {
            SCALE - confidence
        }
    }

    /// Updates match model on each bit.
    pub fn update_bit(&mut self, bit: u8, bit_history: u8) {
        if self.match_len >= 3 {
            let bit_idx = 7 - bit_history.leading_zeros();
            let expected_bit = (self.expected_byte >> (7 - (bit_idx % 8))) & 1;
            if bit != expected_bit {
                // Prediction failed, break match
                self.match_len = 0;
            }
        }
    }

    /// Advances byte position and finds match candidates for the next byte.
    pub fn advance_byte(&mut self, byte: u8) {
        self.buffer[self.pos] = byte;
        let cur_pos = self.pos;
        self.pos = (self.pos + 1) & self.mask;

        // If we had an ongoing match, check if it continues
        if self.match_len > 0 {
            let matched = self.buffer[self.match_pos];
            if matched == byte {
                self.match_len += 1;
                self.match_pos = (self.match_pos + 1) & self.mask;
                self.expected_byte = self.buffer[self.match_pos];
                return;
            } else {
                self.match_len = 0;
            }
        }

        // Look up 4-byte hash
        let h = self.hash_4bytes(self.pos);
        let prev_pos = self.index[h];
        self.index[h] = cur_pos as u32;

        if prev_pos != u32::MAX {
            let prev = prev_pos as usize;
            // Verify match of at least 4 bytes
            let mut match_ok = true;
            for i in 0..4 {
                let p1 = (cur_pos.wrapping_sub(i)) & self.mask;
                let p2 = (prev.wrapping_sub(i)) & self.mask;
                if self.buffer[p1] != self.buffer[p2] {
                    match_ok = false;
                    break;
                }
            }
            if match_ok {
                self.match_len = 4;
                self.match_pos = (prev + 1) & self.mask;
                self.expected_byte = self.buffer[self.match_pos];
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_match_model_repeat_prediction() {
        let mut mm = MatchModel::new(14, 12); // 16KB buffer
        let text = b"The quick brown fox jumps over the lazy dog. The quick brown fox ";

        for &b in &text[..45] {
            mm.advance_byte(b);
        }

        // Now repeat "The quick brown fox "
        for &b in &text[45..] {
            mm.advance_byte(b);
        }

        // After reading "The quick brown fox ", it should have found a long match
        assert!(mm.match_len >= 4);
        assert_eq!(mm.expected_byte, b'j'); // Next char after "fox " was 'j'umps!

        // Predict first bit of 'j' (ASCII 106 = 0b01101010, first bit is 0)
        let p = mm.predict(1);
        assert!(p < 1000, "Should strongly predict 0, got p = {}", p);
    }
}
