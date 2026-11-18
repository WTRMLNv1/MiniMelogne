//! Sparse and stride context models.
//!
//! Captures periodic structures (such as fixed-width record tables, 16/32-bit words,
//! and 2D image rows) by conditioning predictions on bytes at offset `-stride`, `-2*stride`, etc.

use super::state::BitCounter;
use crate::math::hash_combine;

#[derive(Clone, Copy, Default)]
struct HashSlot {
    tag: u16,
    counter: BitCounter,
}

pub struct SparseModel {
    stride: usize,
    table: Vec<HashSlot>,
    mask: usize,
}

impl SparseModel {
    pub fn new(stride: usize, log2_size: usize) -> Self {
        let size = 1usize << log2_size;
        Self {
            stride,
            table: vec![HashSlot::default(); size],
            mask: size - 1,
        }
    }

    #[inline(always)]
    fn get_hash_and_tag(&self, context: &[u8], bit_history: u8) -> (usize, u16) {
        let len = context.len();
        let b1 = if len >= self.stride { context[len - self.stride] } else { 0 };
        let b2 = if len >= self.stride * 2 { context[len - self.stride * 2] } else { 0 };

        let mut h = (self.stride as u64).wrapping_mul(0x9e3779b97f4a7c15);
        h = hash_combine(h, b1 as u64);
        h = hash_combine(h, b2 as u64);
        h = hash_combine(h, bit_history as u64);

        let idx = (h as usize) & self.mask;
        let tag = ((h >> 32) as u16) ^ (h as u16);
        (idx, tag)
    }

    pub fn predict(&self, context: &[u8], bit_history: u8) -> u32 {
        if context.len() < self.stride {
            return 2048;
        }
        let (idx, tag) = self.get_hash_and_tag(context, bit_history);
        let slot = &self.table[idx];
        if slot.tag == tag {
            slot.counter.p1()
        } else {
            2048
        }
    }

    pub fn update(&mut self, context: &[u8], bit_history: u8, bit: u8) {
        if context.len() < self.stride {
            return;
        }
        let (idx, tag) = self.get_hash_and_tag(context, bit_history);
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

    pub fn stride(&self) -> usize {
        self.stride
    }
}
