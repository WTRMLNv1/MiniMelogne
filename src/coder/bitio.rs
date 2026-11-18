//! Bit-level I/O abstractions for buffering bits into bytes.

use std::io::{self, Read, Write};

/// Helper to write individual bits into an underlying `Write` stream.
pub struct BitWriter<W: Write> {
    writer: W,
    buffer: u8,
    bits_in_buffer: u8,
    bytes_written: u64,
}

impl<W: Write> BitWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            buffer: 0,
            bits_in_buffer: 0,
            bytes_written: 0,
        }
    }

    /// Writes a single bit (0 or 1).
    pub fn write_bit(&mut self, bit: u8) -> io::Result<()> {
        debug_assert!(bit == 0 || bit == 1);
        self.buffer = (self.buffer << 1) | (bit & 1);
        self.bits_in_buffer += 1;

        if self.bits_in_buffer == 8 {
            self.writer.write_all(&[self.buffer])?;
            self.buffer = 0;
            self.bits_in_buffer = 0;
            self.bytes_written += 1;
        }
        Ok(())
    }

    /// Flushes any remaining unwritten bits in the buffer, zero-padding to byte boundary.
    pub fn flush(&mut self) -> io::Result<()> {
        if self.bits_in_buffer > 0 {
            self.buffer <<= 8 - self.bits_in_buffer;
            self.writer.write_all(&[self.buffer])?;
            self.buffer = 0;
            self.bits_in_buffer = 0;
            self.bytes_written += 1;
        }
        self.writer.flush()
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    pub fn into_inner(mut self) -> io::Result<W> {
        self.flush()?;
        Ok(self.writer)
    }
}

/// Helper to read individual bits from an underlying `Read` stream.
pub struct BitReader<R: Read> {
    reader: R,
    buffer: u8,
    bits_in_buffer: u8,
}

impl<R: Read> BitReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            buffer: 0,
            bits_in_buffer: 0,
        }
    }

    /// Reads a single bit. If EOF is reached, returns 0s.
    pub fn read_bit(&mut self) -> io::Result<u8> {
        if self.bits_in_buffer == 0 {
            let mut byte_buf = [0u8; 1];
            let n = self.reader.read(&mut byte_buf)?;
            if n == 0 {
                return Ok(0); // EOF pads with 0s to drain decoder safely
            }
            self.buffer = byte_buf[0];
            self.bits_in_buffer = 8;
        }

        self.bits_in_buffer -= 1;
        let bit = (self.buffer >> self.bits_in_buffer) & 1;
        Ok(bit)
    }
}
