//! Word and identifier context model for source code.
//!
//! Tracks partial identifiers and previous words to predict repeated tokens,
//! type names, keywords, and identifier bigrams.

use crate::math::hash_combine;
use crate::models::state::BitCounter;

#[derive(Clone, Copy, Default)]
struct HashSlot {
    tag: u16,
    counter: BitCounter,
}

pub struct WordModel {
    table: Vec<HashSlot>,
    mask: usize,
    prev_word: u64,
    prev_word2: u64,
    current_word: u64,
}

impl WordModel {
    pub fn new(log2_size: usize) -> Self {
        let size = 1usize << log2_size;
        Self {
            table: vec![HashSlot::default(); size],
            mask: size - 1,
            prev_word: 0,
            prev_word2: 0,
            current_word: 0,
        }
    }

    #[inline(always)]
    fn get_hash_and_tag(&self, bit_history: u8) -> (usize, u16) {
        let mut h = self.prev_word.wrapping_mul(0x9e3779b97f4a7c15);
        h = hash_combine(h, self.current_word);
        h = hash_combine(h, bit_history as u64);

        let idx = (h as usize) & self.mask;
        let tag = ((h >> 32) as u16) ^ (h as u16);
        (idx, tag)
    }

    #[inline(always)]
    pub fn predict(&self, bit_history: u8) -> u32 {
        let (idx, tag) = self.get_hash_and_tag(bit_history);
        let slot = &self.table[idx];
        if slot.tag == tag {
            slot.counter.p1()
        } else {
            2048
        }
    }

    #[inline(always)]
    pub fn update(&mut self, bit_history: u8, bit: u8) {
        let (idx, tag) = self.get_hash_and_tag(bit_history);
        let slot = &mut self.table[idx];
        if slot.tag == tag {
            slot.counter.update(bit);
        } else if (slot.counter.c0 + slot.counter.c1) <= 2 {
            slot.tag = tag;
            slot.counter = BitCounter::new();
            slot.counter.update(bit);
        } else {
            slot.counter.c0 >>= 1;
            slot.counter.c1 >>= 1;
        }
    }

    pub fn advance_byte(&mut self, byte: u8) {
        let is_word_char = matches!(byte, b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_');
        if is_word_char {
            self.current_word = hash_combine(self.current_word, byte as u64);
        } else if self.current_word != 0 {
            self.prev_word2 = self.prev_word;
            self.prev_word = self.current_word;
            self.current_word = 0;
        }
    }
}
