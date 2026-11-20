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
  - `match_model.rs`: 1MB circular buffer match dictionary with length-scaled confidence.
  - `nest_model.rs`: Bracket nesting ({, (, [) and line indentation code-structure model.
  - `word_model.rs`: Identifier and keyword bigram context model.
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

---

## Benchmark Results

Evaluated on fixed git snapshot corpuses (`cat`-concatenated binary streams, identical byte-for-byte input to all compressors):
- **Tuning Corpus (AlgoMLN0 snapshot)**: commit `5d5624a97885a5fc99919b94c4e32a1855104f13` (93 `.rs` files, 817,038 bytes)
- **Held-Out Corpus (AlgoMLN2 snapshot)**: commit `440b2ac947eb6998e417973165072c0499e29536` (35 `.rs` files, 467,862 bytes)

### 1. Tuning Corpus (AlgoMLN0 — 817,038 bytes)

| Compressor | Compressed Size | bpc | Margin vs MiniMelogne |
| :--- | :--- | :--- | :--- |
| **MiniMelogne (Current)** | **130,659 B** | **1.279** | **Winner** |
| bzip2 `-9` | 133,134 B | 1.304 | +2,475 B (+1.89%) behind |
| xz `-9` | 142,564 B | 1.396 | +11,905 B (+9.11%) behind |
| Deflate level 9 | 180,076 B | 1.763 | +49,417 B (+37.8%) behind |

*MiniMelogne Container Overhead: 20 bytes fixed header (`MMEL`), 130,639 bytes arithmetic coded payload.*
*Throughput: ~307 KB/s compress, ~191 KB/s decompress (~4 MB total RAM usage).*

### 2. Held-Out Corpus (AlgoMLN2 — 467,862 bytes, zero tuning)

| Compressor | Compressed Size | bpc | Margin vs MiniMelogne |
| :--- | :--- | :--- | :--- |
| **MiniMelogne** | **72,512 B** | **1.240** | **Winner** |
| bzip2 `-9` | 75,510 B | 1.291 | +2,998 B (+4.13%) behind |
| xz `-9` | 79,320 B | 1.356 | +6,808 B (+9.39%) behind |
| Deflate level 9 | 97,261 B | 1.663 | +24,749 B (+34.1%) behind |

---

## Ablation Log

| # | Change | Corpus | Bytes Before | Bytes After | Change % | Kept? | Notes |
|---|---|---|---|---|---|---|---|
| 0 | Baseline | AlgoMLN0 | 137,716 | 137,716 | 0.00% | Yes | Order 0..6 + Sparse + Match(64K) |
| 1 | 7.1 Bit-history StateMap (order-2) | AlgoMLN0 | 137,716 | 140,603 | +2.10% | **No (Reverted)** | Degraded all 100KB blocks; 128K local counters beat shared 256-state map |
| 2 | 7.2 Match window expansion (1MB) | AlgoMLN0 | 137,716 | 134,842 | **-2.09%** | **Yes** | Allows long-range repeat detection across the 817KB file |
| 3 | 7.2 Mismatch recovery | AlgoMLN0 | 134,842 | 137,397 | +1.90% | **No (Reverted)** | Accidental false-resumptions on punctuation hurt confidence |
| 4 | 7.3 NestModel (bracket & indent) | AlgoMLN0 | 134,842 | 132,377 | **-1.83%** | **Yes** | **Beats bzip2 -9** (132,377 B vs 133,134 B) |
| 5 | 7.4 Coarse mixer context sets | AlgoMLN0 | 132,377 | 133,904 | +1.15% | **No (Reverted)** | Micro-contexts (`order1 ^ bit_history`) beat coarse classification |
| 6 | 7.7 WordModel (identifiers & tokens) | AlgoMLN0 | 132,377 | 131,794 | **-0.44%** | **Yes** | Predicts repeated Rust keywords & type names |
| 7 | 7.5 APM output blend (75% APM + 25% p) | AlgoMLN0 | 131,794 | 130,659 | **-0.86%** | **Yes** | Stabilizes sparse bins; **pushes bpc to 1.279** |

---

## Measurement Notes

- **Input size explanation**: Earlier benchmark tables showed a discrepancy between 817,038 bytes and 820,206 bytes. The 820,206 byte figure was produced by `ArchivePackage::pack`, which included 3,168 bytes of archive file path metadata, entry counts, and length headers. The benchmark above uses a raw, byte-exact `cat` concatenation of all 93 `.rs` files (817,038 bytes), ensuring completely identical input for every comparison tool.

---

## Credits and References

MiniMelogne implements concepts inspired by the data compression literature. All code is original, written from first principles:
- **Matt Mahoney**: Conceptual foundations of online context mixing, logistic perceptron mixing, stretch/squash tables, and the arithmetic coder (PAQ series, LPAQ1, and *Data Compression Explained*).
- **Vadim Yo**: Architecture of APM (Adaptive Probability Map) and secondary symbol estimation (SSE) in `paq8px`.
- **Alexander Ratushnyak**: Syntactic structural context modeling concepts from `cmix`.

