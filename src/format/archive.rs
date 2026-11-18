//! Multi-file archive packaging for directory compression.
//!
//! Bundles multiple files into a deterministic sequential stream with relative paths,
//! sorted to maximize context-model redundancy across similar files.

use std::fs;
use std::io::{self, Cursor, Read, Write};
use std::path::Path;

/// A single file entry inside an archive package.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArchiveEntry {
    pub relative_path: String,
    pub data: Vec<u8>,
}

/// Helper for packing and unpacking collections of files.
pub struct ArchivePackage;

impl ArchivePackage {
    /// Packs a list of entries into a single deterministic byte buffer.
    pub fn pack(entries: &[ArchiveEntry]) -> io::Result<Vec<u8>> {
        let mut out = Vec::new();
        // Number of files (u32 LE)
        out.write_all(&(entries.len() as u32).to_le_bytes())?;

        for entry in entries {
            let path_bytes = entry.relative_path.as_bytes();
            if path_bytes.len() > u16::MAX as usize {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Path too long: {}", entry.relative_path),
                ));
            }
            // Path length (u16 LE) + Path UTF-8
            out.write_all(&(path_bytes.len() as u16).to_le_bytes())?;
            out.write_all(path_bytes)?;

            // Data length (u64 LE) + Data
            out.write_all(&(entry.data.len() as u64).to_le_bytes())?;
            out.write_all(&entry.data)?;
        }
        Ok(out)
    }

    /// Unpacks an archive byte buffer into individual entries.
    pub fn unpack(data: &[u8]) -> io::Result<Vec<ArchiveEntry>> {
        let mut cursor = Cursor::new(data);
        let mut num_files_bytes = [0u8; 4];
        cursor.read_exact(&mut num_files_bytes)?;
        let num_files = u32::from_le_bytes(num_files_bytes) as usize;

        let mut entries = Vec::with_capacity(num_files);
        for _ in 0..num_files {
            let mut path_len_bytes = [0u8; 2];
            cursor.read_exact(&mut path_len_bytes)?;
            let path_len = u16::from_le_bytes(path_len_bytes) as usize;

            let mut path_buf = vec![0u8; path_len];
            cursor.read_exact(&mut path_buf)?;
            let relative_path = String::from_utf8(path_buf).map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("Invalid UTF-8 in path: {}", e))
            })?;

            let mut data_len_bytes = [0u8; 8];
            cursor.read_exact(&mut data_len_bytes)?;
            let data_len = u64::from_le_bytes(data_len_bytes) as usize;

            let mut file_data = vec![0u8; data_len];
            cursor.read_exact(&mut file_data)?;

            entries.push(ArchiveEntry {
                relative_path,
                data: file_data,
            });
        }
        Ok(entries)
    }

    /// Recursively collects files from `root` matching filter criteria.
    /// Strictly filters out unwanted directories (like `target`, `.git`) and extensions.
    pub fn collect_files<P: AsRef<Path>>(
        root: P,
        include_ext: Option<&str>,
        exclude_dirs: &[&str],
        exclude_exts: &[&str],
    ) -> io::Result<Vec<ArchiveEntry>> {
        let root = root.as_ref();
        let mut entries = Vec::new();

        fn walk(
            dir: &Path,
            root: &Path,
            include_ext: Option<&str>,
            exclude_dirs: &[&str],
            exclude_exts: &[&str],
            entries: &mut Vec<ArchiveEntry>,
        ) -> io::Result<()> {
            if !dir.is_dir() {
                return Ok(());
            }

            for entry in fs::read_dir(dir)? {
                let entry = entry?;
                let path = entry.path();
                let file_name = entry.file_name().to_string_lossy().to_string();

                if path.is_dir() {
                    // Check excluded directory names
                    if exclude_dirs.iter().any(|&d| file_name.eq_ignore_ascii_case(d)) {
                        continue;
                    }
                    walk(&path, root, include_ext, exclude_dirs, exclude_exts, entries)?;
                } else if path.is_file() {
                    let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");

                    // Check excluded extensions
                    if exclude_exts.iter().any(|&e| ext.eq_ignore_ascii_case(e.trim_start_matches('.'))) {
                        continue;
                    }

                    // Check included extension if specified
                    if let Some(inc) = include_ext {
                        let target = inc.trim_start_matches('*').trim_start_matches('.');
                        if !ext.eq_ignore_ascii_case(target) {
                            continue;
                        }
                    }

                    // Read file
                    let relative = path
                        .strip_prefix(root)
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
                        .to_string_lossy()
                        .replace('\\', "/");

                    let data = fs::read(&path)?;
                    entries.push(ArchiveEntry {
                        relative_path: relative,
                        data,
                    });
                }
            }
            Ok(())
        }

        walk(root, root, include_ext, exclude_dirs, exclude_exts, &mut entries)?;

        // Sort entries deterministically by path so similar files cluster together
        entries.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));

        Ok(entries)
    }

    /// Extracts unpacked entries to a target destination directory.
    pub fn extract_all<P: AsRef<Path>>(entries: &[ArchiveEntry], dest_dir: P) -> io::Result<()> {
        let dest = dest_dir.as_ref();
        for entry in entries {
            let full_path = dest.join(&entry.relative_path);
            if let Some(parent) = full_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(full_path, &entry.data)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pack_unpack_roundtrip() {
        let entries = vec![
            ArchiveEntry {
                relative_path: "src/main.rs".to_string(),
                data: b"fn main() { println!(\"hello\"); }".to_vec(),
            },
            ArchiveEntry {
                relative_path: "src/lib.rs".to_string(),
                data: b"pub fn add(a: i32, b: i32) -> i32 { a + b }".to_vec(),
            },
        ];

        let packed = ArchivePackage::pack(&entries).unwrap();
        let unpacked = ArchivePackage::unpack(&packed).unwrap();
        assert_eq!(entries, unpacked);
    }
}
