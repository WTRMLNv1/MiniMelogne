//! Compression and decompression pipeline engine.
//!
//! Orchestrates the complete pipeline: analysis pass, reversible transforms,
//! online-learning context mixing model, and arithmetic coding.

use crate::analysis::{analyze_data, DataType};
use crate::coder::{ArithmeticDecoder, ArithmeticEncoder};
use crate::format::{FileHeader, FLAG_E8E9, FLAG_RAW, FLAG_STRIDE};
use crate::math::crc32;
use crate::mixer::LogisticMixer;
use crate::models::{ModelConfig, ModelPool, MODEL_COUNT};
use crate::transforms::{E8E9Filter, StrideDelta};
use std::io::{self, Cursor};

/// Compresses a slice of bytes into the MiniMelogne container format.
pub fn compress(input: &[u8], config: Option<ModelConfig>) -> io::Result<Vec<u8>> {
    let original_size = input.len() as u64;
    let original_crc = crc32(input);

    if input.is_empty() {
        let header = FileHeader::new(FLAG_RAW, 0, 0, original_crc);
        let mut out = Vec::new();
        header.write_to(&mut out)?;
        return Ok(out);
    }

    // Step 1: Statistical Analysis
    let analysis = analyze_data(input);

    // If data is incompressible high-entropy noise, store raw
    if analysis.data_type == DataType::HighEntropyRaw {
        let header = FileHeader::new(FLAG_RAW, 0, original_size, original_crc);
        let mut out = Vec::with_capacity(FileHeader::HEADER_SIZE + input.len());
        header.write_to(&mut out)?;
        out.extend_from_slice(input);
        return Ok(out);
    }

    // Step 2: Apply Reversible Transforms
    let mut flags = 0u8;
    let mut stride_val = 0u8;
    let mut stage_data = input.to_vec();

    if analysis.recommended_e8e9 {
        stage_data = E8E9Filter::forward(&stage_data);
        flags |= FLAG_E8E9;
    }

    if let Some(s) = analysis.best_stride {
        if s <= 255 {
            stage_data = StrideDelta::forward(&stage_data, s);
            flags |= FLAG_STRIDE;
            stride_val = s as u8;
        }
    }

    // Step 3: Setup Models and Arithmetic Coder
    let model_cfg = config.unwrap_or_else(|| {
        let mut cfg = ModelConfig::default();
        if stride_val > 0 {
            cfg.sparse_stride = stride_val as usize;
        }
        cfg
    });

    let mut models = ModelPool::new(&model_cfg);
    let mut mixer = LogisticMixer::new(MODEL_COUNT, 256);

    let mut encoded_payload = Vec::new();
    {
        let mut enc = ArithmeticEncoder::new(&mut encoded_payload);
        let mut preds = [2048u32; MODEL_COUNT];
        let mut context_window = Vec::with_capacity(stage_data.len());

        for &byte in &stage_data {
            let mut bit_history = 1u8;
            let order1_byte = context_window.last().copied().unwrap_or(0);

            for bit_pos in 0..8 {
                let actual_bit = (byte >> (7 - bit_pos)) & 1;

                // Context ID conditioned on order-1 byte and current bit tree position
                let context_id = (order1_byte as usize ^ (bit_history as usize)) & 0xFF;

                // 1. All models predict P(bit == 1)
                models.predict_all(&context_window, bit_history, &mut preds);

                // 2. Mixer combines predictions
                let mixed_p = mixer.mix(context_id, &preds);

                // 3. Arithmetic coder encodes real bit
                enc.encode_bit(actual_bit, mixed_p)?;

                // 4. Online learning update
                mixer.update(context_id, &preds, actual_bit);
                models.update_bit(&context_window, bit_history, actual_bit);

                // Advance bit history in byte tree
                bit_history = (bit_history << 1) | actual_bit;
            }

            models.advance_byte(byte);
            context_window.push(byte);
        }

        enc.finish()?;
    }

    // Check if compression actually won; if it expanded beyond raw, fall back to raw
    if encoded_payload.len() >= input.len() {
        let header = FileHeader::new(FLAG_RAW, 0, original_size, original_crc);
        let mut out = Vec::with_capacity(FileHeader::HEADER_SIZE + input.len());
        header.write_to(&mut out)?;
        out.extend_from_slice(input);
        return Ok(out);
    }

    // Step 4: Write Final Header and Payload
    let header = FileHeader::new(flags, stride_val, original_size, original_crc);
    let mut out = Vec::with_capacity(FileHeader::HEADER_SIZE + encoded_payload.len());
    header.write_to(&mut out)?;
    out.extend_from_slice(&encoded_payload);

    Ok(out)
}

/// Decompresses a MiniMelogne archive into the original bytes.
pub fn decompress(input: &[u8]) -> io::Result<Vec<u8>> {
    let mut cursor = Cursor::new(input);
    let header = FileHeader::read_from(&mut cursor)?;

    let header_len = cursor.position() as usize;
    let payload = &input[header_len..];

    // Case 1: Raw uncompressed payload
    if header.is_raw() {
        if payload.len() != header.original_size as usize {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Raw payload length does not match header original_size",
            ));
        }
        let recovered = payload.to_vec();
        let actual_crc = crc32(&recovered);
        if actual_crc != header.original_crc32 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("CRC32 checksum mismatch: expected {:#010X}, got {:#010X}", header.original_crc32, actual_crc),
            ));
        }
        return Ok(recovered);
    }

    // Case 2: Arithmetic-decoded stream
    let mut model_cfg = ModelConfig::default();
    if header.has_stride() && header.stride > 0 {
        model_cfg.sparse_stride = header.stride as usize;
    }

    let mut models = ModelPool::new(&model_cfg);
    let mut mixer = LogisticMixer::new(MODEL_COUNT, 256);
    let mut dec = ArithmeticDecoder::new(payload)?;

    let mut preds = [2048u32; MODEL_COUNT];
    let total_bytes = header.original_size as usize;
    let mut reconstructed = Vec::with_capacity(total_bytes);

    for _ in 0..total_bytes {
        let mut bit_history = 1u8;
        let mut current_byte = 0u8;
        let order1_byte = reconstructed.last().copied().unwrap_or(0);

        for _ in 0..8 {
            let context_id = (order1_byte as usize ^ (bit_history as usize)) & 0xFF;

            // 1. Models predict
            models.predict_all(&reconstructed, bit_history, &mut preds);

            // 2. Mixer combines
            let mixed_p = mixer.mix(context_id, &preds);

            // 3. Decode bit
            let decoded_bit = dec.decode_bit(mixed_p)?;
            current_byte = (current_byte << 1) | decoded_bit;

            // 4. Update models and mixer
            mixer.update(context_id, &preds, decoded_bit);
            models.update_bit(&reconstructed, bit_history, decoded_bit);

            bit_history = (bit_history << 1) | decoded_bit;
        }

        models.advance_byte(current_byte);
        reconstructed.push(current_byte);
    }

    // Invert transforms in reverse order
    let mut output = reconstructed;
    if header.has_stride() {
        output = StrideDelta::inverse(&output, header.stride as usize);
    }
    if header.has_e8e9() {
        output = E8E9Filter::inverse(&output);
    }

    // Verify bit-exact integrity
    let actual_crc = crc32(&output);
    if actual_crc != header.original_crc32 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Decompressed CRC32 mismatch: expected {:#010X}, got {:#010X}", header.original_crc32, actual_crc),
        ));
    }

    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_file_roundtrip() {
        let original = b"";
        let compressed = compress(original, None).unwrap();
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(decompressed.as_slice(), original);
    }

    #[test]
    fn test_text_roundtrip_and_compression() {
        let text = b"To be, or not to be, that is the question: Whether 'tis nobler in the mind to suffer \
The slings and arrows of outrageous fortune, Or to take arms against a sea of troubles \
And by opposing end them. To die - to sleep, No more; and by a sleep to say we end \
The heart-ache and the thousand natural shocks That flesh is heir to: 'tis a consummation \
Devoutly to be wish'd. To die, to sleep; To sleep, perchance to dream - ay, there's the rub: \
For in that sleep of death what dreams may come, When we have shuffled off this mortal coil, \
Must give us pause.";

        let compressed = compress(text, None).unwrap();
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(decompressed.as_slice(), text);
    }

    #[test]
    fn test_structured_table_roundtrip() {
        // Table with 4-byte records
        let mut table = Vec::new();
        for i in 0..500 {
            table.push((i % 256) as u8);
            table.push(((i * 2) % 256) as u8);
            table.push(42);
            table.push(100);
        }

        let compressed = compress(&table, None).unwrap();
        assert!(compressed.len() < table.len(), "Table should compress: orig={}, comp={}", table.len(), compressed.len());
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(decompressed, table);
    }

    #[test]
    fn test_high_entropy_fallback() {
        // Pseudo-random bytes
        let mut rng_data = Vec::with_capacity(2000);
        let mut x = 987654321u32;
        for _ in 0..2000 {
            x = x.wrapping_mul(1664525).wrapping_add(1013904223);
            rng_data.push((x >> 24) as u8);
        }

        let compressed = compress(&rng_data, None).unwrap();
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(decompressed, rng_data);
    }
}
