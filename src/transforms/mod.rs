//! Reversible transforms (Stride/Delta, x86 E8/E9 call address filter).
//!
//! Every transform is strictly bit-exact and mathematically reversible.

/// Reversible byte-stride delta transform.
///
/// Converts correlated periodic sequences (e.g. 24-bit RGB, 32-bit float columns, fixed-width records)
/// into small residual differences centered near zero.
pub struct StrideDelta;

impl StrideDelta {
    pub fn forward(data: &[u8], stride: usize) -> Vec<u8> {
        if stride == 0 || data.len() <= stride {
            return data.to_vec();
        }
        let mut out = Vec::with_capacity(data.len());
        out.extend_from_slice(&data[..stride]);
        for i in stride..data.len() {
            out.push(data[i].wrapping_sub(data[i - stride]));
        }
        out
    }

    pub fn inverse(data: &[u8], stride: usize) -> Vec<u8> {
        if stride == 0 || data.len() <= stride {
            return data.to_vec();
        }
        let mut out = Vec::with_capacity(data.len());
        out.extend_from_slice(&data[..stride]);
        for i in stride..data.len() {
            let recovered = data[i].wrapping_add(out[i - stride]);
            out.push(recovered);
        }
        out
    }
}

/// Reversible x86 E8 (CALL) / E9 (JMP) address filter.
///
/// Converts relative branch offsets into absolute addresses during compression
/// so recurring function calls produce identical byte sequences, and converts
/// them back during decompression.
pub struct E8E9Filter;

impl E8E9Filter {
    pub fn forward(data: &[u8]) -> Vec<u8> {
        let mut out = data.to_vec();
        let len = out.len();
        if len < 5 {
            return out;
        }

        let mut i = 0;
        while i + 4 < len {
            let op = out[i];
            if op == 0xE8 || op == 0xE9 {
                let rel = i32::from_le_bytes([out[i + 1], out[i + 2], out[i + 3], out[i + 4]]);
                // Convert relative offset to absolute file address
                let abs = rel.wrapping_add((i as i32) + 5);
                let bytes = abs.to_le_bytes();
                out[i + 1..i + 5].copy_from_slice(&bytes);
                i += 5;
            } else {
                i += 1;
            }
        }
        out
    }

    pub fn inverse(data: &[u8]) -> Vec<u8> {
        let mut out = data.to_vec();
        let len = out.len();
        if len < 5 {
            return out;
        }

        let mut i = 0;
        while i + 4 < len {
            let op = out[i];
            if op == 0xE8 || op == 0xE9 {
                let abs = i32::from_le_bytes([out[i + 1], out[i + 2], out[i + 3], out[i + 4]]);
                // Convert absolute address back to relative offset
                let rel = abs.wrapping_sub((i as i32) + 5);
                let bytes = rel.to_le_bytes();
                out[i + 1..i + 5].copy_from_slice(&bytes);
                i += 5;
            } else {
                i += 1;
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stride_delta_roundtrip() {
        let original: Vec<u8> = (0..5000).map(|i| ((i * 7 + 13) % 256) as u8).collect();
        for stride in [1, 2, 3, 4, 8, 16] {
            let transformed = StrideDelta::forward(&original, stride);
            assert_eq!(transformed.len(), original.len());
            let recovered = StrideDelta::inverse(&transformed, stride);
            assert_eq!(recovered, original, "Stride {} failed roundtrip", stride);
        }
    }

    #[test]
    fn test_e8e9_filter_roundtrip() {
        // Construct simulated x86 code with calls
        let mut code = vec![0x90u8; 1000]; // NOPs
        code[10] = 0xE8;
        code[11..15].copy_from_slice(&1234567i32.to_le_bytes());
        code[200] = 0xE9;
        code[201..205].copy_from_slice(&(-500i32).to_le_bytes());
        code[450] = 0xE8;
        code[451..455].copy_from_slice(&42i32.to_le_bytes());

        let transformed = E8E9Filter::forward(&code);
        assert_ne!(transformed, code, "Transform should modify call offsets");
        let recovered = E8E9Filter::inverse(&transformed);
        assert_eq!(recovered, code, "E8E9 roundtrip mismatch");
    }
}
