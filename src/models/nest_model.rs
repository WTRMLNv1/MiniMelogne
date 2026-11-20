//! Code-structure nesting and indentation context model for source code.
//!
//! Tracks bracket nesting depths, innermost bracket type, column position,
//! and line indentation to capture structural syntax patterns in programming languages.

use crate::math::hash_combine;
use crate::models::state::BitCounter;

#[derive(Clone, Copy, Default)]
struct HashSlot {
    tag: u16,
    counter: BitCounter,
}

pub struct NestModel {
    table: Vec<HashSlot>,
    mask: usize,
    stack: Vec<u8>, // 1: '{', 2: '(', 3: '['
    brace_depth: u8,
    paren_depth: u8,
    bracket_depth: u8,
    current_col: u16,
    current_indent: u16,
    in_leading_ws: bool,
}

impl NestModel {
    pub fn new(log2_size: usize) -> Self {
        let size = 1usize << log2_size;
        Self {
            table: vec![HashSlot::default(); size],
            mask: size - 1,
            stack: Vec::with_capacity(64),
            brace_depth: 0,
            paren_depth: 0,
            bracket_depth: 0,
            current_col: 0,
            current_indent: 0,
            in_leading_ws: true,
        }
    }

    #[inline(always)]
    fn get_hash_and_tag(&self, context: &[u8], bit_history: u8) -> (usize, u16) {
        let order1_byte = context.last().copied().unwrap_or(0);
        let innermost = self.stack.last().copied().unwrap_or(0);

        // State packaging: brace depth, innermost bracket, indentation
        let state_val = (self.brace_depth as u64)
            | ((innermost as u64) << 5)
            | (((self.current_indent / 4).min(15) as u64) << 8);

        let mut h = hash_combine(state_val, order1_byte as u64);
        h = hash_combine(h, bit_history as u64);

        let idx = (h as usize) & self.mask;
        let tag = ((h >> 32) as u16) ^ (h as u16);
        (idx, tag)
    }

    /// Predicts P(bit == 1) in [1, 4095].
    #[inline(always)]
    pub fn predict(&self, context: &[u8], bit_history: u8) -> u32 {
        let (idx, tag) = self.get_hash_and_tag(context, bit_history);
        let slot = &self.table[idx];
        if slot.tag == tag {
            slot.counter.p1()
        } else {
            2048
        }
    }

    /// Updates model with observed bit.
    #[inline(always)]
    pub fn update(&mut self, context: &[u8], bit_history: u8, bit: u8) {
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

    /// Called at byte boundary to advance structural state.
    pub fn advance_byte(&mut self, byte: u8) {
        match byte {
            b'{' => {
                self.brace_depth = self.brace_depth.saturating_add(1).min(31);
                self.stack.push(1);
            }
            b'}' => {
                self.brace_depth = self.brace_depth.saturating_sub(1);
                if let Some(pos) = self.stack.iter().rposition(|&x| x == 1) {
                    self.stack.remove(pos);
                }
            }
            b'(' => {
                self.paren_depth = self.paren_depth.saturating_add(1).min(31);
                self.stack.push(2);
            }
            b')' => {
                self.paren_depth = self.paren_depth.saturating_sub(1);
                if let Some(pos) = self.stack.iter().rposition(|&x| x == 2) {
                    self.stack.remove(pos);
                }
            }
            b'[' => {
                self.bracket_depth = self.bracket_depth.saturating_add(1).min(31);
                self.stack.push(3);
            }
            b']' => {
                self.bracket_depth = self.bracket_depth.saturating_sub(1);
                if let Some(pos) = self.stack.iter().rposition(|&x| x == 3) {
                    self.stack.remove(pos);
                }
            }
            b'\n' => {
                self.current_col = 0;
                self.current_indent = 0;
                self.in_leading_ws = true;
                return;
            }
            b' ' if self.in_leading_ws => {
                self.current_indent = (self.current_indent + 1).min(63);
            }
            b'\t' if self.in_leading_ws => {
                self.current_indent = (self.current_indent + 4).min(63);
            }
            _ => {
                self.in_leading_ws = false;
            }
        }
        self.current_col = (self.current_col + 1).min(255);
    }
}
