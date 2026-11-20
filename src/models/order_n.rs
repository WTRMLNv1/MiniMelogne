//! Order-N context models mapping past byte history + current bit position to probabilities.

use super::state::{BitCounter, StateMap};
use crate::math::hash_combine;

/// Slot in an Order-N direct-mapped hash table.
#[derive(Clone, Copy, Default)]
struct HashSlot {
    tag: u16,
    counter: BitCounter,
}

#[derive(Clone, Copy, Default)]
struct StateSlot {
    tag: u16,
    state: u8,
}

/// An Order-N byte context model.
///
/// Hashes the preceding `order` bytes along with the current partial-byte bit history
/// to index a direct-mapped table of adaptive bit counters or bit-history states.
pub struct OrderNModel {
    order: usize,
    table: Vec<HashSlot>,
    state_table: Vec<StateSlot>,
    state_map: Option<StateMap>,
    mask: usize,
}

impl OrderNModel {
    /// Creates a new Order-N model with a table size of `2^log2_size` entries.
    pub fn new(order: usize, log2_size: usize) -> Self {
        let size = 1usize << log2_size;
        Self {
            order,
            table: vec![HashSlot::default(); size],
            state_table: Vec::new(),
            state_map: None,
            mask: size - 1,
        }
    }

    /// Creates a new Order-N model with bit-history StateMap.
    pub fn new_state_map(order: usize, log2_size: usize) -> Self {
        let size = 1usize << log2_size;
        Self {
            order,
            table: Vec::new(),
            state_table: vec![StateSlot::default(); size],
            state_map: Some(StateMap::new()),
            mask: size - 1,
        }
    }

    #[inline(always)]
    fn get_hash_and_tag(&self, context: &[u8], bit_history: u8) -> (usize, u16) {
        let start = if context.len() >= self.order {
            context.len() - self.order
        } else {
            0
        };
        let slice = &context[start..];

        let mut h = self.order as u64;
        for &b in slice {
            h = hash_combine(h, b as u64);
        }
        h = hash_combine(h, bit_history as u64);

        let idx = (h as usize) & self.mask;
        let tag = ((h >> 32) as u16) ^ (h as u16);
        (idx, tag)
    }

    /// Predicts P(bit == 1) in [1, 4095].
    #[inline(always)]
    pub fn predict(&self, context: &[u8], bit_history: u8) -> u32 {
        if context.len() < self.order {
            return 2048; // Not enough context bytes yet
        }
        let (idx, tag) = self.get_hash_and_tag(context, bit_history);
        if let Some(ref sm) = self.state_map {
            let slot = &self.state_table[idx];
            if slot.tag == tag {
                sm.p1(slot.state)
            } else {
                2048
            }
        } else {
            let slot = &self.table[idx];
            if slot.tag == tag {
                slot.counter.p1()
            } else {
                2048 // Collision or uninitialized slot: neutral prior
            }
        }
    }

    /// Updates the model with the observed bit.
    #[inline(always)]
    pub fn update(&mut self, context: &[u8], bit_history: u8, bit: u8) {
        if context.len() < self.order {
            return;
        }
        let (idx, tag) = self.get_hash_and_tag(context, bit_history);
        if let Some(ref mut sm) = self.state_map {
            let slot = &mut self.state_table[idx];
            if slot.tag == tag {
                sm.update(slot.state, bit);
                slot.state = StateMap::next_state(slot.state, bit);
            } else if slot.state <= 2 {
                slot.tag = tag;
                slot.state = StateMap::next_state(0, bit);
            } else {
                slot.state = StateMap::age_state(slot.state);
            }
        } else {
            let slot = &mut self.table[idx];
            if slot.tag == tag {
                slot.counter.update(bit);
            } else if (slot.counter.c0 + slot.counter.c1) <= 2 {
                // Replace weak slot with new tag
                slot.tag = tag;
                slot.counter = BitCounter::new();
                slot.counter.update(bit);
            } else {
                // Aging existing entry on collision
                slot.counter.c0 >>= 1;
                slot.counter.c1 >>= 1;
            }
        }
    }

    pub fn order(&self) -> usize {
        self.order
    }
}

/// Order-0 model: conditioned only on the current partial byte bit history.
/// 256 counters, completely collision-free.
pub struct Order0Model {
    counters: [BitCounter; 256],
}

impl Default for Order0Model {
    fn default() -> Self {
        Self::new()
    }
}

impl Order0Model {
    pub fn new() -> Self {
        Self {
            counters: [BitCounter::new(); 256],
        }
    }

    #[inline(always)]
    pub fn predict(&self, bit_history: u8) -> u32 {
        self.counters[bit_history as usize].p1()
    }

    #[inline(always)]
    pub fn update(&mut self, bit_history: u8, bit: u8) {
        self.counters[bit_history as usize].update(bit);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_order0_learning() {
        let mut m = Order0Model::new();
        // At root bit_history = 1
        assert_eq!(m.predict(1), 2048);
        for _ in 0..10 {
            m.update(1, 1);
        }
        assert!(m.predict(1) > 3700);
    }

    #[test]
    fn test_order_n_learning() {
        let mut m = OrderNModel::new(2, 12);
        let ctx = b"hello";
        let bit_hist = 1;

        assert_eq!(m.predict(ctx, bit_hist), 2048);
        for _ in 0..5 {
            m.update(ctx, bit_hist, 0);
        }
        assert!(m.predict(ctx, bit_hist) < 1000);

        // Different context should not be affected
        let other_ctx = b"world";
        assert_eq!(m.predict(other_ctx, bit_hist), 2048);
    }
}
