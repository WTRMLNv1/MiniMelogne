//! MiniMelogne container file format and header definitions.
//!
//! Binary layout (20 bytes):
//! ```text
//! +---------------+---------+-------+--------+------------------+------------------+
//! | Magic (4B)    | Ver(1B) | Flags | Stride | Orig Size (8B)   | Orig CRC32 (4B)  |
//! | 'M','M','E','L'|   1     |  (1B) |  (1B)  | (Little Endian)  | (Little Endian)  |
//! +---------------+---------+-------+--------+------------------+------------------+
//! Followed by arithmetic-coded bitstream payload (or raw payload if FLAG_RAW).
//! ```

pub mod archive;

use std::io::{self, Read, Write};

pub const MAGIC: [u8; 4] = *b"MMEL";
pub const CURRENT_VERSION: u8 = 1;

pub const FLAG_RAW: u8 = 1 << 0;
pub const FLAG_E8E9: u8 = 1 << 1;
pub const FLAG_STRIDE: u8 = 1 << 2;

/// Header metadata stored at the beginning of every compressed `.mel` file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileHeader {
    pub version: u8,
    pub flags: u8,
    pub stride: u8,
    pub original_size: u64,
    pub original_crc32: u32,
}

impl FileHeader {
    pub const HEADER_SIZE: usize = 20;

    pub fn new(flags: u8, stride: u8, original_size: u64, original_crc32: u32) -> Self {
        Self {
            version: CURRENT_VERSION,
            flags,
            stride,
            original_size,
            original_crc32,
        }
    }

    pub fn is_raw(&self) -> bool {
        (self.flags & FLAG_RAW) != 0
    }

    pub fn has_e8e9(&self) -> bool {
        (self.flags & FLAG_E8E9) != 0
    }

    pub fn has_stride(&self) -> bool {
        (self.flags & FLAG_STRIDE) != 0
    }

    /// Serializes header to a writer.
    pub fn write_to<W: Write>(&self, mut writer: W) -> io::Result<()> {
        writer.write_all(&MAGIC)?;
        writer.write_all(&[self.version, self.flags, self.stride, 0])?; // 1 padding byte
        writer.write_all(&self.original_size.to_le_bytes())?;
        writer.write_all(&self.original_crc32.to_le_bytes())?;
        Ok(())
    }

    /// Deserializes header from a reader.
    pub fn read_from<R: Read>(mut reader: R) -> io::Result<Self> {
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if magic != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid MiniMelogne magic header",
            ));
        }

        let mut meta = [0u8; 4];
        reader.read_exact(&mut meta)?;
        let version = meta[0];
        if version != CURRENT_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unsupported MiniMelogne version: {}", version),
            ));
        }
        let flags = meta[1];
        let stride = meta[2];

        let mut size_bytes = [0u8; 8];
        reader.read_exact(&mut size_bytes)?;
        let original_size = u64::from_le_bytes(size_bytes);

        let mut crc_bytes = [0u8; 4];
        reader.read_exact(&mut crc_bytes)?;
        let original_crc32 = u32::from_le_bytes(crc_bytes);

        Ok(Self {
            version,
            flags,
            stride,
            original_size,
            original_crc32,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_roundtrip() {
        let header = FileHeader::new(FLAG_STRIDE | FLAG_E8E9, 4, 1_234_567, 0x12345678);
        let mut buf = Vec::new();
        header.write_to(&mut buf).unwrap();
        assert_eq!(buf.len(), FileHeader::HEADER_SIZE);

        let decoded = FileHeader::read_from(buf.as_slice()).unwrap();
        assert_eq!(header, decoded);
        assert!(decoded.has_stride());
        assert!(decoded.has_e8e9());
        assert!(!decoded.is_raw());
        assert_eq!(decoded.stride, 4);
    }
}
