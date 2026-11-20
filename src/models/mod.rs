//! Online context models (order-0..n, sparse, and match models).

pub mod match_model;
pub mod order_n;
pub mod sparse;
pub mod state;
pub mod state_table;

use match_model::MatchModel;
use order_n::{Order0Model, OrderNModel};
use sparse::SparseModel;

/// Number of active models in the default pool.
pub const MODEL_COUNT: usize = 8;

/// Configuration options for the context model pool.
#[derive(Clone, Debug)]
pub struct ModelConfig {
    pub log2_order1_size: usize,
    pub log2_order2_size: usize,
    pub log2_order3_size: usize,
    pub log2_order4_size: usize,
    pub log2_order6_size: usize,
    pub log2_sparse_size: usize,
    pub sparse_stride: usize,
    pub log2_match_buf: usize,
    pub log2_match_idx: usize,
}

impl Default for ModelConfig {
    fn default() -> Self {
        // Laptop-friendly default (~4-8 MB total memory)
        Self {
            log2_order1_size: 16, // 64K entries
            log2_order2_size: 17, // 128K entries
            log2_order3_size: 18, // 256K entries
            log2_order4_size: 18, // 256K entries
            log2_order6_size: 18, // 256K entries
            log2_sparse_size: 16, // 64K entries
            sparse_stride: 4,     // Default stride 4 (32-bit words / tables)
            log2_match_buf: 16,   // 64KB match window
            log2_match_idx: 15,   // 32K match hash table
        }
    }
}

/// A coordinated pool of context models feeding predictions into the mixer.
pub struct ModelPool {
    pub order0: Order0Model,
    pub order1: OrderNModel,
    pub order2: OrderNModel,
    pub order3: OrderNModel,
    pub order4: OrderNModel,
    pub order6: OrderNModel,
    pub sparse: SparseModel,
    pub match_model: MatchModel,
}

impl ModelPool {
    pub fn new(config: &ModelConfig) -> Self {
        Self {
            order0: Order0Model::new(),
            order1: OrderNModel::new(1, config.log2_order1_size),
            order2: OrderNModel::new_state_map(2, config.log2_order2_size),
            order3: OrderNModel::new(3, config.log2_order3_size),
            order4: OrderNModel::new(4, config.log2_order4_size),
            order6: OrderNModel::new(6, config.log2_order6_size),
            sparse: SparseModel::new(config.sparse_stride, config.log2_sparse_size),
            match_model: MatchModel::new(config.log2_match_buf, config.log2_match_idx),
        }
    }

    /// Fills `predictions` array with P(bit == 1) in [1, 4095] from each model.
    pub fn predict_all(&self, context: &[u8], bit_history: u8, predictions: &mut [u32; MODEL_COUNT]) {
        predictions[0] = self.order0.predict(bit_history);
        predictions[1] = self.order1.predict(context, bit_history);
        predictions[2] = self.order2.predict(context, bit_history);
        predictions[3] = self.order3.predict(context, bit_history);
        predictions[4] = self.order4.predict(context, bit_history);
        predictions[5] = self.order6.predict(context, bit_history);
        predictions[6] = self.sparse.predict(context, bit_history);
        predictions[7] = self.match_model.predict(bit_history);
    }

    /// Updates all models with the observed bit.
    pub fn update_bit(&mut self, context: &[u8], bit_history: u8, bit: u8) {
        self.order0.update(bit_history, bit);
        self.order1.update(context, bit_history, bit);
        self.order2.update(context, bit_history, bit);
        self.order3.update(context, bit_history, bit);
        self.order4.update(context, bit_history, bit);
        self.order6.update(context, bit_history, bit);
        self.sparse.update(context, bit_history, bit);
        self.match_model.update_bit(bit, bit_history);
    }

    /// Called at byte boundary when a full byte has been processed.
    pub fn advance_byte(&mut self, byte: u8) {
        self.match_model.advance_byte(byte);
    }
}
