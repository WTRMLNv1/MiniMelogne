//! Statistical analysis frontend.
//!
//! Heuristics and detectors for discovering data structure, optimal strides,
//! Shannon entropy, and selecting reversible transforms.

/// Detected data classification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataType {
    Text,
    ExecutableX86,
    StructuredTable { stride: usize },
    HighEntropyRaw,
    GeneralBinary,
}

/// Analysis report produced by the frontend.
#[derive(Clone, Debug)]
pub struct AnalysisReport {
    pub data_type: DataType,
    pub entropy: f64,
    pub best_stride: Option<usize>,
    pub recommended_e8e9: bool,
}

/// Computes Shannon entropy of a byte slice in bits per symbol [0.0, 8.0].
pub fn shannon_entropy(data: &[u8]) -> f64 {
    if data.is_empty() {
        return 0.0;
    }
    let mut counts = [0u64; 256];
    for &b in data {
        counts[b as usize] += 1;
    }
    let total = data.len() as f64;
    let mut h = 0.0;
    for &c in &counts {
        if c > 0 {
            let p = c as f64 / total;
            h -= p * p.log2();
        }
    }
    h
}

/// Detects regular periodic strides (e.g. table columns, pixel channels) by
/// analyzing average absolute difference across candidate stride periods [1, max_stride].
pub fn detect_stride(data: &[u8], max_stride: usize) -> Option<usize> {
    if data.len() < max_stride * 4 {
        return None;
    }

    let mut best_stride = 0;
    let mut min_diff = u64::MAX;
    let baseline_samples = (data.len() - 1).min(16384);
    let mut baseline_diff: u64 = 0;
    for i in 1..=baseline_samples {
        baseline_diff += (data[i] as i32 - data[i - 1] as i32).abs() as u64;
    }
    let baseline_avg = baseline_diff / baseline_samples as u64;

    for s in 2..=max_stride {
        let n_samples = (data.len() - s).min(16384);
        let mut sum_diff: u64 = 0;
        for i in s..s + n_samples {
            sum_diff += (data[i] as i32 - data[i - s] as i32).abs() as u64;
        }
        let avg_diff = sum_diff / n_samples as u64;
        if avg_diff < min_diff {
            min_diff = avg_diff;
            best_stride = s;
        }
    }

    // Only accept stride if it cuts average residual difference by at least 25% vs baseline
    if best_stride > 0 && min_diff * 4 < baseline_avg * 3 {
        Some(best_stride)
    } else {
        None
    }
}

/// Analyzes an input buffer and recommends transforms and model configuration.
pub fn analyze_data(data: &[u8]) -> AnalysisReport {
    let entropy = shannon_entropy(data);

    // If data is already high entropy (e.g. zip, gzip, encrypted), mark raw
    if entropy > 7.96 && data.len() > 1024 {
        return AnalysisReport {
            data_type: DataType::HighEntropyRaw,
            entropy,
            best_stride: None,
            recommended_e8e9: false,
        };
    }

    // Check for printable ASCII text
    let mut ascii_printable = 0usize;
    let sample_len = data.len().min(8192);
    for &b in &data[..sample_len] {
        if b.is_ascii_graphic() || b == b' ' || b == b'\t' || b == b'\r' || b == b'\n' {
            ascii_printable += 1;
        }
    }
    let ascii_ratio = ascii_printable as f64 / sample_len as f64;
    if ascii_ratio > 0.95 {
        return AnalysisReport {
            data_type: DataType::Text,
            entropy,
            best_stride: None,
            recommended_e8e9: false,
        };
    }

    // Check for x86 executable: density of 0xE8 / 0xE9 opcodes
    let mut call_count = 0usize;
    for &b in &data[..sample_len] {
        if b == 0xE8 || b == 0xE9 {
            call_count += 1;
        }
    }
    let call_ratio = call_count as f64 / sample_len as f64;
    let is_x86 = call_ratio > 0.012 && entropy > 5.5 && entropy < 7.5;

    // Check for periodic table structure
    let stride = detect_stride(data, 64);

    let data_type = if is_x86 {
        DataType::ExecutableX86
    } else if let Some(s) = stride {
        DataType::StructuredTable { stride: s }
    } else {
        DataType::GeneralBinary
    };

    AnalysisReport {
        data_type,
        entropy,
        best_stride: stride,
        recommended_e8e9: is_x86,
    }
}

/// Causal rolling entropy counter (operable identically by both encoder and decoder).
pub struct CausalRollingStats {
    window_size: usize,
    history: Vec<u8>,
    counts: [u32; 256],
    pos: usize,
    filled: bool,
}

impl CausalRollingStats {
    pub fn new(window_size: usize) -> Self {
        Self {
            window_size,
            history: vec![0u8; window_size],
            counts: [0u32; 256],
            pos: 0,
            filled: false,
        }
    }

    /// Feeds next byte and updates rolling counts.
    pub fn push(&mut self, byte: u8) {
        if self.filled {
            let old = self.history[self.pos];
            self.counts[old as usize] = self.counts[old as usize].saturating_sub(1);
        }
        self.history[self.pos] = byte;
        self.counts[byte as usize] += 1;

        self.pos += 1;
        if self.pos == self.window_size {
            self.pos = 0;
            self.filled = true;
        }
    }

    /// Fast integer approximation of rolling entropy in range [0, 255].
    pub fn approximate_entropy(&self) -> u8 {
        let total = if self.filled { self.window_size } else { self.pos };
        if total < 4 {
            return 128;
        }
        let mut non_zero_symbols = 0usize;
        for &c in &self.counts {
            if c > 0 {
                non_zero_symbols += 1;
            }
        }
        // Scaled estimate: more distinct symbols in window -> higher entropy
        ((non_zero_symbols * 255) / 256).min(255) as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shannon_entropy() {
        // Uniform distribution: 256 unique bytes -> entropy should be 8.0
        let uniform: Vec<u8> = (0..256).map(|b| b as u8).collect();
        let h_uni = shannon_entropy(&uniform);
        assert!((h_uni - 8.0).abs() < 0.001);

        // Constant data: entropy should be 0.0
        let zeroes = vec![0u8; 500];
        assert_eq!(shannon_entropy(&zeroes), 0.0);
    }

    #[test]
    fn test_stride_detection() {
        // Generate interleaved table with record width 4: [A, B, C, D, A+1, B+1, ...]
        let mut table = Vec::new();
        for i in 0..1000 {
            table.push((i % 256) as u8);
            table.push(10);
            table.push(50);
            table.push(100);
        }
        let detected = detect_stride(&table, 16);
        assert_eq!(detected, Some(4));
    }

    #[test]
    fn test_text_classification() {
        let sample = b"MiniMelogne is a laptop-friendly context-mixing compressor in Rust.";
        let rep = analyze_data(sample);
        assert_eq!(rep.data_type, DataType::Text);
        assert!(!rep.recommended_e8e9);
    }
}
