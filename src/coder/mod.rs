//! Binary arithmetic coder with 12-bit integer-only interval arithmetic.
//!
//! Maps probabilities in the range `[1, 4095]` (where 4096 is 1.0) into a continuous
//! shrinking interval. Fully deterministic across all CPU architectures and platforms.

pub mod bitio;

use bitio::{BitReader, BitWriter};
use std::io::{self, Read, Write};

pub const SCALE_BITS: u32 = 12;
pub const SCALE: u32 = 1 << SCALE_BITS; // 4096
pub const HALF_SCALE: u32 = SCALE >> 1; // 2048 (p = 0.5)

const HALF: u32 = 0x8000_0000;
const FIRST_QTR: u32 = 0x4000_0000;
const THIRD_QTR: u32 = 0xC000_0000;

/// Binary arithmetic encoder.
pub struct ArithmeticEncoder<W: Write> {
    writer: BitWriter<W>,
    low: u32,
    high: u32,
    pending_bits: u32,
}

impl<W: Write> ArithmeticEncoder<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer: BitWriter::new(writer),
            low: 0,
            high: 0xFFFF_FFFF,
            pending_bits: 0,
        }
    }

    /// Encodes a single bit `bit` given `p1 = P(bit == 1)`, where `1 <= p1 < 4096`.
    pub fn encode_bit(&mut self, bit: u8, p1: u32) -> io::Result<()> {
        let p1 = p1.clamp(1, SCALE - 1);
        let range = (self.high as u64) - (self.low as u64) + 1;
        let p0 = (SCALE - p1) as u64;
        let mid = (self.low as u64) + ((range * p0) >> SCALE_BITS);

        if bit == 0 {
            self.high = (mid - 1) as u32;
        } else {
            self.low = mid as u32;
        }

        // Renormalization
        loop {
            if self.high < HALF {
                self.output_bit_and_pending(0)?;
                self.low <<= 1;
                self.high = (self.high << 1) | 1;
            } else if self.low >= HALF {
                self.output_bit_and_pending(1)?;
                self.low = (self.low - HALF) << 1;
                self.high = ((self.high - HALF) << 1) | 1;
            } else if self.low >= FIRST_QTR && self.high < THIRD_QTR {
                self.pending_bits += 1;
                self.low = (self.low - FIRST_QTR) << 1;
                self.high = ((self.high - FIRST_QTR) << 1) | 1;
            } else {
                break;
            }
        }
        Ok(())
    }

    fn output_bit_and_pending(&mut self, bit: u8) -> io::Result<()> {
        self.writer.write_bit(bit)?;
        let opposite = bit ^ 1;
        while self.pending_bits > 0 {
            self.writer.write_bit(opposite)?;
            self.pending_bits -= 1;
        }
        Ok(())
    }

    /// Finalizes the stream and flushes remaining interval bits and buffer.
    pub fn finish(mut self) -> io::Result<W> {
        self.pending_bits += 1;
        if self.low < FIRST_QTR {
            self.output_bit_and_pending(0)?;
        } else {
            self.output_bit_and_pending(1)?;
        }
        self.writer.into_inner()
    }
}

/// Binary arithmetic decoder.
pub struct ArithmeticDecoder<R: Read> {
    reader: BitReader<R>,
    low: u32,
    high: u32,
    value: u32,
}

impl<R: Read> ArithmeticDecoder<R> {
    pub fn new(reader: R) -> io::Result<Self> {
        let mut reader = BitReader::new(reader);
        let mut value = 0u32;
        for _ in 0..32 {
            value = (value << 1) | (reader.read_bit()? as u32);
        }

        Ok(Self {
            reader,
            low: 0,
            high: 0xFFFF_FFFF,
            value,
        })
    }

    /// Decodes a single bit given `p1 = P(bit == 1)`, where `1 <= p1 < 4096`.
    pub fn decode_bit(&mut self, p1: u32) -> io::Result<u8> {
        let p1 = p1.clamp(1, SCALE - 1);
        let range = (self.high as u64) - (self.low as u64) + 1;
        let p0 = (SCALE - p1) as u64;
        let mid = (self.low as u64) + ((range * p0) >> SCALE_BITS);

        let bit = if (self.value as u64) < mid {
            self.high = (mid - 1) as u32;
            0
        } else {
            self.low = mid as u32;
            1
        };

        // Renormalization
        loop {
            if self.high < HALF {
                self.low <<= 1;
                self.high = (self.high << 1) | 1;
                self.value = (self.value << 1) | (self.reader.read_bit()? as u32);
            } else if self.low >= HALF {
                self.low = (self.low - HALF) << 1;
                self.high = ((self.high - HALF) << 1) | 1;
                self.value = ((self.value - HALF) << 1) | (self.reader.read_bit()? as u32);
            } else if self.low >= FIRST_QTR && self.high < THIRD_QTR {
                self.low = (self.low - FIRST_QTR) << 1;
                self.high = ((self.high - FIRST_QTR) << 1) | 1;
                self.value = ((self.value - FIRST_QTR) << 1) | (self.reader.read_bit()? as u32);
            } else {
                break;
            }
        }

        Ok(bit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_fixed_50_percent() {
        let bits: Vec<u8> = (0..1000).map(|i| (i % 2) as u8).collect();
        let mut compressed = Vec::new();

        {
            let mut enc = ArithmeticEncoder::new(&mut compressed);
            for &b in &bits {
                enc.encode_bit(b, HALF_SCALE).unwrap();
            }
            enc.finish().unwrap();
        }

        // 1000 bits at 50% should be ~125 bytes + slight framing overhead
        assert!(compressed.len() >= 120 && compressed.len() <= 135);

        let mut dec = ArithmeticDecoder::new(compressed.as_slice()).unwrap();
        for &expected in &bits {
            let decoded = dec.decode_bit(HALF_SCALE).unwrap();
            assert_eq!(decoded, expected);
        }
    }

    #[test]
    fn test_skewed_data_compression() {
        // 90% zeros, 10% ones
        let n = 2000;
        let p1 = 410; // ~10% probability (410 / 4096 ≈ 0.10)
        let bits: Vec<u8> = (0..n).map(|i| if i % 10 == 0 { 1 } else { 0 }).collect();

        let mut compressed = Vec::new();
        {
            let mut enc = ArithmeticEncoder::new(&mut compressed);
            for &b in &bits {
                enc.encode_bit(b, p1).unwrap();
            }
            enc.finish().unwrap();
        }

        // 2000 bits raw is 250 bytes.
        // Theoretical entropy = - (0.9*log2(0.9) + 0.1*log2(0.1)) * 2000 / 8 ≈ 117 bytes.
        // It must compress well below 250 bytes!
        assert!(compressed.len() < 160, "Compressed len was {}", compressed.len());

        let mut dec = ArithmeticDecoder::new(compressed.as_slice()).unwrap();
        for (i, &expected) in bits.iter().enumerate() {
            let decoded = dec.decode_bit(p1).unwrap();
            assert_eq!(decoded, expected, "Mismatch at bit index {}", i);
        }
    }

    #[test]
    fn test_dynamic_probabilities_roundtrip() {
        let n = 5000;
        // Pseudo-random pseudo-probabilities
        let mut lcg = 123456789u32;
        let mut bits = Vec::with_capacity(n);
        let mut probs = Vec::with_capacity(n);

        for _ in 0..n {
            lcg = lcg.wrapping_mul(1664525).wrapping_add(1013904223);
            let p = ((lcg >> 16) % 4094) + 1; // [1, 4094]
            let bit = if (lcg & 0xFF) < ((p * 255) / 4096) { 1 } else { 0 };
            bits.push(bit);
            probs.push(p);
        }

        let mut compressed = Vec::new();
        {
            let mut enc = ArithmeticEncoder::new(&mut compressed);
            for i in 0..n {
                enc.encode_bit(bits[i], probs[i]).unwrap();
            }
            enc.finish().unwrap();
        }

        let mut dec = ArithmeticDecoder::new(compressed.as_slice()).unwrap();
        for i in 0..n {
            let decoded = dec.decode_bit(probs[i]).unwrap();
            assert_eq!(decoded, bits[i], "Mismatch at dynamic bit {}", i);
        }
    }
}
