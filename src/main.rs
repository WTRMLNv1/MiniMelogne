//! MiniMelogne CLI: A laptop-friendly context-mixing compressor in Rust.

use minimelogne::analysis::analyze_data;
use minimelogne::engine::{compress, decompress};
use minimelogne::format::archive::ArchivePackage;
use minimelogne::models::ModelConfig;
use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;
use std::time::Instant;

fn print_usage() {
    eprintln!(
        r#"MiniMelogne v{} - Statistical analysis & context-mixing compressor

USAGE:
    minimelogne <COMMAND> [OPTIONS] <TARGET>

COMMANDS:
    compress, -c <FILE> [-o <OUT.mel>] [--memory <low|medium|high>]
        Compress a single file into a .mel container.

    decompress, -d <FILE.mel> [-o <OUT>]
        Decompress a .mel container to its original form.

    archive, -ar <DIR> [-o <OUT.mel>] [--include <EXT>] [--exclude <DIRS>]
        Collect matching files from a directory, bundle, and compress them into a .mel archive.

    extract, -x <ARCHIVE.mel> -o <DEST_DIR>
        Decompress and unpack all files from a .mel archive into a target directory.

    analyze, -a <FILE>
        Run statistical frontend analysis (entropy, stride, data type).

    bench, -b <FILE> [--memory <low|medium|high>]
        Benchmark compression ratio, speed, and verify bit-exactness on a single file.

    bench-dir <DIR> [--include <EXT>] [--exclude <DIRS>] [--memory <low|medium|high>]
        Benchmark multi-file compression on filtered directory contents (e.g. only .rs).

OPTIONS:
    -o, --output <PATH>       Output file or destination directory path
    -i, --include <EXT>       File extension filter (e.g. '.rs' or 'rs')
    -e, --exclude <NAMES>     Comma-separated list of directories/extensions to skip
    -m, --memory <PRESET>     Memory budget: low (1MB), medium (8MB), high (32MB)
    -h, --help                Print help information
    -V, --version             Print version
"#,
        env!("CARGO_PKG_VERSION")
    );
}

fn memory_preset(name: &str) -> ModelConfig {
    match name.to_lowercase().as_str() {
        "low" => ModelConfig {
            log2_order1_size: 15,
            log2_order2_size: 15,
            log2_order3_size: 16,
            log2_order4_size: 16,
            log2_order6_size: 16,
            log2_sparse_size: 15,
            sparse_stride: 4,
            log2_match_buf: 15,
            log2_match_idx: 14,
        },
        "high" => ModelConfig {
            log2_order1_size: 17,
            log2_order2_size: 18,
            log2_order3_size: 19,
            log2_order4_size: 19,
            log2_order6_size: 19,
            log2_sparse_size: 17,
            sparse_stride: 4,
            log2_match_buf: 18,
            log2_match_idx: 17,
        },
        _ => ModelConfig::default(),
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        print_usage();
        return Ok(());
    }

    let command = args[1].as_str();
    if command == "-h" || command == "--help" || command == "help" {
        print_usage();
        return Ok(());
    }
    if command == "-V" || command == "--version" || command == "version" {
        println!("minimelogne {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    let mut target_path: Option<PathBuf> = None;
    let mut output_path: Option<PathBuf> = None;
    let mut include_ext: Option<String> = None;
    let mut exclude_dirs_str: Option<String> = None;
    let mut memory_str = "medium";

    let mut i = 2;
    while i < args.len() {
        match args[i].as_str() {
            "-o" | "--output" => {
                if i + 1 < args.len() {
                    output_path = Some(PathBuf::from(&args[i + 1]));
                    i += 1;
                }
            }
            "-i" | "--include" => {
                if i + 1 < args.len() {
                    include_ext = Some(args[i + 1].clone());
                    i += 1;
                }
            }
            "-e" | "--exclude" => {
                if i + 1 < args.len() {
                    exclude_dirs_str = Some(args[i + 1].clone());
                    i += 1;
                }
            }
            "-m" | "--memory" => {
                if i + 1 < args.len() {
                    memory_str = &args[i + 1];
                    i += 1;
                }
            }
            arg if !arg.starts_with('-') && target_path.is_none() => {
                target_path = Some(PathBuf::from(arg));
            }
            other => {
                eprintln!("Unknown argument: {}", other);
            }
        }
        i += 1;
    }

    let target = match target_path {
        Some(p) => p,
        None => {
            eprintln!("Error: Missing file or directory target.");
            print_usage();
            std::process::exit(1);
        }
    };

    // Parse exclude lists
    let default_exclude_dirs = ["target", ".git", "node_modules", ".gemini", ".idea", ".vscode"];
    let default_exclude_exts = ["parquet", "csv", "zip", "gz", "tar", "7z", "exe", "dll", "so", "bin"];

    let custom_excludes: Vec<String> = exclude_dirs_str
        .map(|s| s.split(',').map(|x| x.trim().to_string()).collect())
        .unwrap_or_default();

    let mut combined_exclude_dirs: Vec<&str> = default_exclude_dirs.to_vec();
    for c in &custom_excludes {
        combined_exclude_dirs.push(c.as_str());
    }

    match command {
        "compress" | "-c" => {
            let out_file = output_path.unwrap_or_else(|| {
                let mut p = target.clone();
                let ext = match p.extension() {
                    Some(e) => format!("{}.mel", e.to_string_lossy()),
                    None => "mel".to_string(),
                };
                p.set_extension(ext);
                p
            });

            println!("Reading {:?}...", target);
            let raw_data = fs::read(&target)?;
            let t0 = Instant::now();
            let config = memory_preset(memory_str);

            print!("Compressing ({}) ... ", memory_str);
            io::stdout().flush()?;
            let compressed = compress(&raw_data, Some(config))?;
            let elapsed = t0.elapsed();

            fs::write(&out_file, &compressed)?;
            let orig_len = raw_data.len();
            let comp_len = compressed.len();
            let ratio = if orig_len > 0 { (comp_len as f64 / orig_len as f64) * 100.0 } else { 100.0 };
            let bpc = if orig_len > 0 { (comp_len as f64 * 8.0) / orig_len as f64 } else { 0.0 };

            println!("Done in {:.2?}", elapsed);
            println!(
                "Original: {} bytes | Compressed: {} bytes ({:.2}%, {:.3} bpc)",
                orig_len, comp_len, ratio, bpc
            );
            println!("Saved to {:?}", out_file);
        }
        "decompress" | "-d" => {
            let out_file = output_path.unwrap_or_else(|| {
                let mut p = target.clone();
                let fname = p.file_stem().unwrap_or_default().to_string_lossy().to_string();
                p.set_file_name(fname);
                p
            });

            println!("Reading {:?}...", target);
            let comp_data = fs::read(&target)?;
            let t0 = Instant::now();

            print!("Decompressing & verifying ... ");
            io::stdout().flush()?;
            let recovered = decompress(&comp_data)?;
            let elapsed = t0.elapsed();

            fs::write(&out_file, &recovered)?;
            println!("Done in {:.2?}", elapsed);
            println!("Restored {} bytes to {:?}", recovered.len(), out_file);
        }
        "archive" | "-ar" => {
            let out_file = output_path.unwrap_or_else(|| {
                let mut p = target.clone();
                p.set_extension("mel");
                p
            });

            println!("Scanning directory {:?}...", target);
            if let Some(ref inc) = include_ext {
                println!("  Filter: include only '{}'", inc);
            }
            println!("  Excluded directories: {:?}", combined_exclude_dirs);

            let entries = ArchivePackage::collect_files(
                &target,
                include_ext.as_deref(),
                &combined_exclude_dirs,
                &default_exclude_exts,
            )?;

            if entries.is_empty() {
                eprintln!("No matching files found in {:?}", target);
                return Ok(());
            }

            let total_raw: usize = entries.iter().map(|e| e.data.len()).sum();
            println!("Collected {} files ({} bytes / {:.1} KB).", entries.len(), total_raw, total_raw as f64 / 1024.0);

            print!("Packing & compressing archive ... ");
            io::stdout().flush()?;
            let packed = ArchivePackage::pack(&entries)?;
            let t0 = Instant::now();
            let compressed = compress(&packed, Some(memory_preset(memory_str)))?;
            let elapsed = t0.elapsed();

            fs::write(&out_file, &compressed)?;
            let comp_len = compressed.len();
            let ratio = (comp_len as f64 / total_raw as f64) * 100.0;
            let bpc = (comp_len as f64 * 8.0) / total_raw as f64;

            println!("Done in {:.2?}", elapsed);
            println!("Archive: {} files | Original: {} bytes | Compressed: {} bytes ({:.2}%, {:.3} bpc)",
                entries.len(), total_raw, comp_len, ratio, bpc
            );
            println!("Saved archive to {:?}", out_file);
        }
        "extract" | "-x" => {
            let dest_dir = match output_path {
                Some(p) => p,
                None => {
                    eprintln!("Error: -o <DEST_DIR> is required for extraction.");
                    std::process::exit(1);
                }
            };

            println!("Reading archive {:?}...", target);
            let comp_data = fs::read(&target)?;
            let t0 = Instant::now();

            print!("Decompressing ... ");
            io::stdout().flush()?;
            let packed = decompress(&comp_data)?;
            let entries = ArchivePackage::unpack(&packed)?;
            let elapsed = t0.elapsed();

            println!("Done in {:.2?}", elapsed);
            println!("Unpacking {} files into {:?}...", entries.len(), dest_dir);
            ArchivePackage::extract_all(&entries, &dest_dir)?;
            println!("Extraction complete.");
        }
        "analyze" | "-a" => {
            let raw_data = fs::read(&target)?;
            let report = analyze_data(&raw_data);
            println!("=== MiniMelogne Statistical Analysis Report ===");
            println!("File: {:?}", target);
            println!("Size: {} bytes", raw_data.len());
            println!("Shannon Entropy: {:.4} bits/byte", report.entropy);
            println!("Data Classification: {:?}", report.data_type);
            println!(
                "Recommended Stride: {}",
                report.best_stride.map(|s| s.to_string()).unwrap_or_else(|| "None".to_string())
            );
            println!("Recommended E8/E9 Filter: {}", report.recommended_e8e9);
        }
        "bench" | "-b" => {
            let raw_data = fs::read(&target)?;
            let orig_len = raw_data.len();
            println!("=== MiniMelogne Benchmark ===");
            println!("File: {:?} ({} bytes)", target, orig_len);

            let t_comp_start = Instant::now();
            let compressed = compress(&raw_data, Some(memory_preset(memory_str)))?;
            let comp_time = t_comp_start.elapsed();

            let comp_len = compressed.len();
            let ratio = (comp_len as f64 / orig_len as f64) * 100.0;
            let bpc = (comp_len as f64 * 8.0) / orig_len as f64;

            let t_dec_start = Instant::now();
            let recovered = decompress(&compressed)?;
            let dec_time = t_dec_start.elapsed();

            assert_eq!(recovered, raw_data, "Decompressed output CRC/data mismatch!");

            println!("Compressed Size: {} bytes", comp_len);
            println!("Compression Ratio: {:.2}% ({:.3} bpc)", ratio, bpc);
            println!("Compression Speed: {:.2?} ({:.2} KB/s)", comp_time, (orig_len as f64 / 1024.0) / comp_time.as_secs_f64());
            println!("Decompression Speed: {:.2?} ({:.2} KB/s)", dec_time, (orig_len as f64 / 1024.0) / dec_time.as_secs_f64());
            println!("Integrity: PASS (Bit-exact round-trip verified)");
        }
        "bench-dir" => {
            println!("=== MiniMelogne Directory Benchmark ===");
            println!("Scanning directory {:?}...", target);
            if let Some(ref inc) = include_ext {
                println!("  Filter: include only '{}'", inc);
            }
            println!("  Excluded directories: {:?}", combined_exclude_dirs);

            let entries = ArchivePackage::collect_files(
                &target,
                include_ext.as_deref(),
                &combined_exclude_dirs,
                &default_exclude_exts,
            )?;

            if entries.is_empty() {
                eprintln!("No matching files found in {:?}", target);
                return Ok(());
            }

            let total_raw: usize = entries.iter().map(|e| e.data.len()).sum();
            println!("Collected {} files ({} bytes / {:.1} KB).", entries.len(), total_raw, total_raw as f64 / 1024.0);

            let packed = ArchivePackage::pack(&entries)?;

            print!("Compressing ... ");
            io::stdout().flush()?;
            let t_comp = Instant::now();
            let compressed = compress(&packed, Some(memory_preset(memory_str)))?;
            let comp_elapsed = t_comp.elapsed();

            let comp_len = compressed.len();
            let ratio = (comp_len as f64 / total_raw as f64) * 100.0;
            let bpc = (comp_len as f64 * 8.0) / total_raw as f64;

            print!("Done.\nDecompressing & verifying bit-exact integrity ... ");
            io::stdout().flush()?;
            let t_dec = Instant::now();
            let recovered_packed = decompress(&compressed)?;
            let dec_elapsed = t_dec.elapsed();

            assert_eq!(recovered_packed, packed, "Decompressed packed archive mismatch!");
            let unpacked = ArchivePackage::unpack(&recovered_packed)?;
            assert_eq!(unpacked, entries, "Extracted entries content mismatch!");

            println!("Done.");
            println!("--------------------------------------------------");
            println!("Files Processed:      {}", entries.len());
            println!("Total Raw Size:       {} bytes ({:.1} KB)", total_raw, total_raw as f64 / 1024.0);
            println!("Compressed Size:      {} bytes ({:.1} KB)", comp_len, comp_len as f64 / 1024.0);
            println!("Compression Ratio:    {:.2}% ({:.3} bpc)", ratio, bpc);
            println!("Compression Time:     {:.2?} ({:.2} KB/s)", comp_elapsed, (total_raw as f64 / 1024.0) / comp_elapsed.as_secs_f64());
            println!("Decompression Time:   {:.2?} ({:.2} KB/s)", dec_elapsed, (total_raw as f64 / 1024.0) / dec_elapsed.as_secs_f64());
            println!("Integrity:            PASS (All {} files match byte-for-byte)", entries.len());
        }
        unknown => {
            eprintln!("Unknown command: {}", unknown);
            print_usage();
            std::process::exit(1);
        }
    }

    Ok(())
}
