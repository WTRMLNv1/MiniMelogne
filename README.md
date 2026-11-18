# MiniMelogne

A tiny, laptop-friendly context-mixing compressor in Rust with a statistical analysis frontend. Written from scratch with zero external dependencies.

## Key Design Principles

1. **Deterministic & Integer-Only**: No floating-point operations anywhere in prediction, coding, or modeling. Arithmetic interval math and stretch/squash tables ensure bit-exact decompression across every OS and CPU.
2. **Online Learning (PAQ Family)**: No static pre-training or frozen datasets required. The model begins *tabula rasa* (uninformed prior) and adapts online as it reads bits. The decoder executes the identical learning loop in lockstep.
3. **Statistical Analysis Frontend**: Automatically discovers file characteristics (Shannon entropy, periodic record strides, x86 branch density) and selects optimal reversible transforms before feeding causal statistics into the model.
4. **Zero Crates / Pure Rust**: Built entirely in standard Rust with zero external crates for instant compilation and complete offline reliability.

---

## Architecture

```text
[Raw Input]
     │
     ▼
[Statistical Analysis Frontend]  ──► (Detects DataType, Entropy, Strides, x86 code)
     │
     ▼
[Reversible Transforms]          ──► (Stride/Delta Filter, x86 E8/E9 Jump Filter)
     │
     ▼
[Context Model Pool]             ──► (Order-0..6 n-grams, Sparse Strides, Match Model)
     │ (Bit probabilities: p_0 .. p_7)
     ▼
[Logistic Mixer & APM]           ──► (Online SGD Perceptron + Secondary Symbol Estimation)
     │ (Calibrated p_mixed)
     ▼
[Binary Arithmetic Coder]        ──► (Integer Interval Division, Witten-Neal-Cleary)
     │
     ▼
[MiniMelogne .mel File]
```

### Module Breakdown

- [src/coder/mod.rs](file:///C:/Users/ansh/Documents/Github/MiniMelogne/src/coder/mod.rs): Witten-Neal-Cleary binary arithmetic coder with 12-bit integer interval precision and underflow resolution.
- [src/math/mod.rs](file:///C:/Users/ansh/Documents/Github/MiniMelogne/src/math/mod.rs): Precomputed logit stretch and sigmoid squash tables, SplitMix64 state hashing, and IEEE 802.3 CRC-32.
- [src/models/](file:///C:/Users/ansh/Documents/Github/MiniMelogne/src/models/):
  - `state.rs`: 2-byte adaptive Laplace bit counters with exponential halving.
  - `order_n.rs`: Direct-mapped cache-hashed Order-0, 1, 2, 3, 4, 6 context models.
  - `sparse.rs`: Periodic stride models for structured tables and word boundaries.
  - `match_model.rs`: Circular buffer dictionary detector with variable confidence.
- [src/mixer/mod.rs](file:///C:/Users/ansh/Documents/Github/MiniMelogne/src/mixer/mod.rs): Multi-context online logistic regression updating weights on every bit via Stochastic Gradient Descent (SGD), chained with an Adaptive Probability Map (APM) for non-linear calibration.
- [src/transforms/mod.rs](file:///C:/Users/ansh/Documents/Github/MiniMelogne/src/transforms/mod.rs): Reversible Stride/Delta transforms and x86 E8/E9 CALL/JMP relative-to-absolute converters.
- [src/analysis/mod.rs](file:///C:/Users/ansh/Documents/Github/MiniMelogne/src/analysis/mod.rs): Shannon entropy estimation, autocorrelation stride detection, and data characterization.
- [src/format/mod.rs](file:///C:/Users/ansh/Documents/Github/MiniMelogne/src/format/mod.rs): Container file format specification with magic bytes (`MMEL`), flags, original size, and CRC-32 checksums.
- [src/engine/mod.rs](file:///C:/Users/ansh/Documents/Github/MiniMelogne/src/engine/mod.rs): Complete end-to-end compression and decompression engine.

---

## CLI Usage

### Build
```bash
cargo build --release
```

### Compress
```bash
./target/release/minimelogne compress <INPUT_FILE> [-o <OUTPUT.mel>] [--memory low|medium|high]
```

### Decompress
```bash
./target/release/minimelogne decompress <FILE.mel> [-o <OUTPUT_FILE>]
```

### Statistical Analysis
```bash
./target/release/minimelogne analyze <INPUT_FILE>
```

### Benchmark
```bash
./target/release/minimelogne bench <INPUT_FILE>
```

---

## Educational Guide: Entropy & Context Mixing

### 1. What is Shannon Entropy?
Claude Shannon showed that the theoretical limit of lossless compression for an information source with symbol probabilities $p_i$ is its entropy:
$$H = -\sum_{i} p_i \log_2(p_i) \quad \text{bits/symbol}$$
If a symbol has a probability of $50\%$, it costs $-\log_2(0.5) = 1$ bit. If a symbol has a probability of $99\%$, it costs only $-\log_2(0.99) \approx 0.014$ bits!
The entire goal of a context-mixing compressor is to predict the probability of the next bit as accurately as possible. The better the prediction, the fewer bits the arithmetic coder emits.

### 2. Why Context Mixing instead of Offline Deep Learning?
In traditional machine learning, models are trained offline on large datasets, frozen, and shipped. But in context-mixing compression:
- No weights are stored in the compressed file.
- Both the encoder and decoder start with blank (equal) weights.
- When encoding bit $t$, both sides use what they've learned from bits $0 \dots t-1$.
- After the real bit is processed, both the encoder and decoder update their model counters and mixer weights using **Stochastic Gradient Descent (SGD)**:
  $$w_i \leftarrow w_i + \eta \cdot (y - \hat{p}) \cdot \text{stretch}(p_i)$$
- Because the decoder sees the exact same bit sequence, it stays in 100% lockstep without transmitting a single parameter!
