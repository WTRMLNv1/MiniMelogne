//! MiniMelogne CLI: A laptop-friendly context-mixing compressor in Rust.

use minimelogne::analysis::analyze_data;
use minimelogne::engine::{compress, decompress};
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
    minimelogne <COMMAND> [OPTIONS] <FILE>

COMMANDS:
    compress, -c <FILE> [-o <OUT>] [--memory <low|medium|high>]
        Compress a file into a .mel container.

    decompress, -d <FILE> [-o <OUT>]
        Decompress a .mel container to its original form.

    analyze, -a <FILE>
        Run statistical frontend analysis (entropy, stride, data type).

    bench, -b <FILE>
        Benchmark compression ratio, speed, and verify bit-exactness.

OPTIONS:
    -o, --output <PATH>       Output file path
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

    let mut input_path: Option<PathBuf> = None;
    let mut output_path: Option<PathBuf> = None;
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
            "-m" | "--memory" => {
                if i + 1 < args.len() {
                    memory_str = &args[i + 1];
                    i += 1;
                }
            }
            arg if !arg.starts_with('-') && input_path.is_none() => {
                input_path = Some(PathBuf::from(arg));
            }
            other => {
                eprintln!("Unknown argument: {}", other);
            }
        }
        i += 1;
    }

    let input_file = match input_path {
        Some(p) => p,
        None => {
            eprintln!("Error: Missing input file.");
            print_usage();
            std::process::exit(1);
        }
    };

    match command {
        "compress" | "-c" => {
            let out_file = output_path.unwrap_or_else(|| {
                let mut p = input_file.clone();
                let ext = match p.extension() {
                    Some(e) => format!("{}.mel", e.to_string_lossy()),
                    None => "mel".to_string(),
                };
                p.set_extension(ext);
                p
            });

            println!("Reading {:?}...", input_file);
            let raw_data = fs::read(&input_file)?;
            let t0 = Instant::now();
            let config = memory_preset(memory_str);

            print!("Compressing ({}) ... ", memory_str);
            io::stdout().flush()?;
            let compressed = compress(&raw_data, Some(config))?;
            let elapsed = t0.elapsed();

            fs::write(&out_file, &compressed)?;
            let orig_len = raw_data.len();
            let comp_len = compressed.len();
            let ratio = if orig_len > 0 {
                (comp_len as f64 / orig_len as f64) * 100.0
            } else {
                100.0
            };
            let bpc = if orig_len > 0 {
                (comp_len as f64 * 8.0) / orig_len as f64
            } else {
                0.0
            };

            println!("Done in {:.2?}", elapsed);
            println!(
                "Original: {} bytes | Compressed: {} bytes ({:.2}%, {:.3} bpc)",
                orig_len, comp_len, ratio, bpc
            );
            println!("Saved to {:?}", out_file);
        }
        "decompress" | "-d" => {
            let out_file = output_path.unwrap_or_else(|| {
                let mut p = input_file.clone();
                let fname = p.file_stem().unwrap_or_default().to_string_lossy().to_string();
                p.set_file_name(fname);
                p
            });

            println!("Reading {:?}...", input_file);
            let comp_data = fs::read(&input_file)?;
            let t0 = Instant::now();

            print!("Decompressing & verifying ... ");
            io::stdout().flush()?;
            let recovered = decompress(&comp_data)?;
            let elapsed = t0.elapsed();

            fs::write(&out_file, &recovered)?;
            println!("Done in {:.2?}", elapsed);
            println!("Restored {} bytes to {:?}", recovered.len(), out_file);
        }
        "analyze" | "-a" => {
            let raw_data = fs::read(&input_file)?;
            let report = analyze_data(&raw_data);
            println!("=== MiniMelogne Statistical Analysis Report ===");
            println!("File: {:?}", input_file);
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
            let raw_data = fs::read(&input_file)?;
            let orig_len = raw_data.len();
            println!("=== MiniMelogne Benchmark ===");
            println!("File: {:?} ({} bytes)", input_file, orig_len);

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
            println!("Compression Time: {:.2?} ({:.2} KB/s)", comp_time, (orig_len as f64 / 1024.0) / comp_time.as_secs_f64());
            println!("Decompression Time: {:.2?} ({:.2} KB/s)", dec_time, (orig_len as f64 / 1024.0) / dec_time.as_secs_f64());
            println!("Integrity: PASS (Bit-exact round-trip verified)");
        }
        unknown => {
            eprintln!("Unknown command: {}", unknown);
            print_usage();
            std::process::exit(1);
        }
    }

    Ok(())
}
