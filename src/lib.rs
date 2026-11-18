//! MiniMelogne: A tiny, laptop-friendly context-mixing compressor in Rust.
//!
//! Written from scratch with an integer-only arithmetic coder, online-learning
//! context models, logistic mixer, and statistical analysis frontend.

pub mod analysis;
pub mod coder;
pub mod engine;
pub mod format;
pub mod math;
pub mod mixer;
pub mod models;
pub mod transforms;
