# MiniMelogne

A tiny, laptop-friendly context-mixing compressor in Rust with a statistical analysis frontend. Written from scratch with zero external dependencies.

## Key Design Principles

1. **Deterministic & Integer-Only**: No floating-point operations anywhere in prediction, coding, or modeling. Decompression is bit-exact across architectures and compilers.
2. **Zero Training Dataset**: True online learning in the PAQ/lpaq family. The model starts tabula rasa (uninformed prior) and adapts to bytes as it sees them. The decoder mirrors this learning in lockstep.
3. **Statistical Analysis Frontend**: Detects data types, strides, and structure before applying reversible transforms (delta/stride, x86 E8/E9) to feed optimal causal context to the mixer.
4. **Zero Crates / Pure Rust**: Arithmetic coder, context models, mixer, and transforms are implemented completely from scratch.

## Architecture

- **`coder`**: Integer-precision binary arithmetic coder / range coder.
- **`math`**: Fixed-point stretch (logit) and squash (sigmoid) tables, deterministic hashing.
- **`models`**: Order-0 through Order-6 context models, bit-history state transitions, match model.
- **`mixer`**: Multi-input logistic regression mixer with online stochastic gradient descent updates, paired with Secondary Symbol Estimation (APM stage).
- **`transforms`**: Reversible stride/delta and x86 E8/E9 call filters.
- **`analysis`**: Entropy estimation, autocorrelation stride detection, and data characterization.
- **`format`**: Robust container format with magic bytes, checksums, and metadata.
