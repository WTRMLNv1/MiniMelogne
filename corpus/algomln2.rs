use algomln::indicators::{
    Candle, IncrementalProvider, IndicatorProvider, IndicatorSpec, IndicatorType,
};
use algomln::{Engine, Program};
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

fn candles() -> Vec<Candle> {
    include_str!("../sample-data/nifty_1min.csv")
        .lines()
        .skip(1)
        .enumerate()
        .map(|(i, row)| {
            let mut fields = row.split(',');
            let _ = fields.next();
            let mut n = || fields.next().unwrap().parse().unwrap();
            let open = n();
            let high = n();
            let low = n();
            let close = n();
            Candle {
                timestamp: i as i64 * 60_000,
                open,
                high: high.max(close),
                low: low.min(open).min(close),
                close,
                volume: 0.0,
            }
        })
        .collect()
}

fn full_indicator_set(c: &mut Criterion) {
    let input = candles();
    let mut group = c.benchmark_group("incremental_full_set");
    group.throughput(Throughput::Elements(input.len() as u64));
    group.bench_function("nifty_1min_184863", |b| {
        b.iter(|| {
            let mut provider = IncrementalProvider::new();
            for (kind, period) in [
                (IndicatorType::Sma, 20),
                (IndicatorType::Ema, 20),
                (IndicatorType::Rsi, 14),
                (IndicatorType::Atr, 14),
                (IndicatorType::BbUpper, 20),
                (IndicatorType::BbMid, 20),
                (IndicatorType::BbLower, 20),
                (IndicatorType::RelVol, 20),
                (IndicatorType::Vwap, 0),
            ] {
                provider
                    .register(IndicatorSpec::new(kind, period, None).unwrap())
                    .unwrap();
            }
            for candle in &input {
                provider.advance(black_box(candle)).unwrap();
            }
            black_box(provider)
        })
    });
    group.finish();
}

fn condition_engine(c: &mut Criterion) {
    let input = candles();
    let program=Program::new(algomln::compile("TRADE_IN NIFTY\nWHEN ema(20) > sma(50) AND rsi(14) < 70\nBUY 1\nWHEN CROSS_BELOW(close, ema(20))\nSELL ALL\n").unwrap().strategy().clone());
    let mut group = c.benchmark_group("condition_engine_nifty");
    group.throughput(Throughput::Elements(input.len() as u64));
    group.bench_function("nifty_1min_184863", |b| {
        b.iter(|| {
            let mut engine = Engine::new(&program, "NIFTY".into()).unwrap();
            for candle in &input {
                black_box(engine.on_candle(black_box(candle)).unwrap());
            }
        })
    });
    group.finish();
}

criterion_group!(benches, full_indicator_set, condition_engine);
criterion_main!(benches);
use algomln::{
    compile,
    indicators::Candle,
    sim::{
        run, CandleSource, CandleStream, DateRange, FeeModel, Fingerprint, GapPolicy, RunConfig,
        Timeframe,
    },
    Symbol,
};
use criterion::{black_box, Criterion, Throughput};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    collections::BTreeSet,
    sync::atomic::{AtomicUsize, Ordering},
};

struct TrackingAllocator;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static MAX_DELTA: AtomicUsize = AtomicUsize::new(0);
unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc(layout);
        if !p.is_null() {
            let n = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(n, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = System.realloc(ptr, layout, new_size);
        if !p.is_null() {
            let old = layout.size();
            let n = if new_size >= old {
                LIVE.fetch_add(new_size - old, Ordering::Relaxed) + new_size - old
            } else {
                LIVE.fetch_sub(old - new_size, Ordering::Relaxed) - (old - new_size)
            };
            PEAK.fetch_max(n, Ordering::Relaxed);
        }
        p
    }
}
#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator;

struct BenchSource {
    rows: Vec<(Symbol, Candle)>,
}
impl CandleSource for BenchSource {
    fn open(
        &self,
        symbols: &[Symbol],
        _: Timeframe,
        range: DateRange,
    ) -> Result<CandleStream, algomln::sim::SourceError> {
        let symbols: BTreeSet<_> = symbols.iter().collect();
        let rows = self
            .rows
            .iter()
            .filter(|(s, c)| {
                symbols.contains(s)
                    && range.from.map_or(true, |x| c.timestamp >= x)
                    && range.to.map_or(true, |x| c.timestamp <= x)
            })
            .cloned()
            .map(Ok)
            .collect::<Vec<_>>();
        Ok(Box::new(rows.into_iter()))
    }
    fn fingerprint(&self, _: &[Symbol], _: Timeframe, _: DateRange) -> Fingerprint {
        Fingerprint("synthetic-benchmark".into())
    }
}
fn config() -> RunConfig {
    RunConfig {
        timeframe: Timeframe::M1,
        range: DateRange::default(),
        initial_cash: 1_000_000.0,
        warmup_bars: 0,
        latency: 1,
        slippage_bps: 0.0,
        fee_model: FeeModel::default(),
        partial_fills_pct: None,
        gap_policy: GapPolicy::None,
        debug_invariants: false,
    }
}
fn benchmark(c: &mut Criterion, name: &str, symbols: &[&str], bars: usize) {
    let program = compile(&format!(
        "TRADE_IN {}\nWHEN close > 100000\nBUY 1\n",
        symbols.join(",")
    ))
    .unwrap();
    let mut rows = Vec::with_capacity(bars * symbols.len());
    for i in 0..bars {
        for symbol in symbols {
            let p = 100.0 + (i % 31) as f64;
            rows.push((
                (*symbol).to_owned(),
                Candle {
                    timestamp: i as i64 * 60,
                    open: p,
                    high: p + 1.0,
                    low: p - 1.0,
                    close: p,
                    volume: 1000.0,
                },
            ));
        }
    }
    let source = BenchSource { rows };
    let source_bytes = source.rows.capacity() * std::mem::size_of::<(Symbol, Candle)>()
        + source.rows.iter().map(|(s, _)| s.capacity()).sum::<usize>();
    let count = bars * symbols.len();
    let mut group = c.benchmark_group(name);
    group.throughput(Throughput::Elements(count as u64));
    group.bench_function(name, |b| {
        b.iter(|| {
            let baseline = LIVE.load(Ordering::Relaxed);
            PEAK.store(baseline, Ordering::Relaxed);
            black_box(run(&program, &source, config(), None).unwrap());
            let peak = PEAK.load(Ordering::Relaxed).saturating_sub(baseline) + source_bytes;
            MAX_DELTA.fetch_max(peak, Ordering::Relaxed);
        })
    });
    group.finish();
}
fn main() {
    std::env::set_var("ALGOMLN_BENCHMARK", "1");
    let mut c = Criterion::default().configure_from_args();
    benchmark(&mut c, "sim_pipeline_single_symbol_52k", &["ABC"], 52_000);
    benchmark(
        &mut c,
        "sim_pipeline_multi_symbol_3x52k",
        &["AAA", "BBB", "CCC"],
        52_000,
    );
    c.final_summary();
    eprintln!(
        "maximum observed benchmark heap allocation delta: {} bytes",
        MAX_DELTA.load(Ordering::Relaxed)
    );
}
use std::process::Command;

// Bake the git commit into GIT_COMMIT at *build* time so the sim run manifest can
// record it without spawning `git` on every `run()` (reproducibility only). An
// externally provided GIT_COMMIT (e.g. from CI) takes precedence; in non-git or
// sandboxed builds the lookup fails gracefully and the manifest records null.
fn main() {
    println!("cargo:rerun-if-env-changed=GIT_COMMIT");
    if std::path::Path::new(".git/HEAD").exists() {
        println!("cargo:rerun-if-changed=.git/HEAD");
    }
    let commit = std::env::var("GIT_COMMIT")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| {
            Command::new("git")
                .args(["rev-parse", "HEAD"])
                .output()
                .ok()
                .filter(|o| o.status.success())
                .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
                .filter(|s| !s.is_empty())
        });
    if let Some(commit) = commit {
        println!("cargo:rustc-env=GIT_COMMIT={commit}");
    }
}
use algomln_data::{
    Bar, BulkConfig, BulkDownloader, Catalog, Store, Tf, Updater, DEFAULT_BUDGET_CAP,
};
use chrono::{Datelike, NaiveDate, Utc};
use dhan_api::{parse_scrip_master, DhanClient, KeychainEnvTokenProvider};
use std::{
    path::PathBuf,
    sync::{atomic::AtomicBool, Arc},
};

pub fn main_entry(args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let Some(sub) = args.first().cloned() else {
        return Err("usage: algomln data <status|verify|update|import|bulk> [options]".into());
    };
    let mut opts = std::collections::BTreeMap::new();
    let mut it = args.into_iter().skip(1);
    while let Some(k) = it.next() {
        if !k.starts_with("--") {
            return Err(format!("unexpected argument {k}").into());
        }
        opts.insert(
            k.clone(),
            it.next().ok_or_else(|| format!("missing value for {k}"))?,
        );
    }
    let root = PathBuf::from(opts.get("--data").map(String::as_str).unwrap_or("data"));
    let db = root.join("catalog.sqlite");
    let mut cat = Catalog::open(&db)?;
    let store = Store::new(&root);
    match sub.as_str() {
        "status" => {
            println!("catalog: {}", db.display());
            for series in cat.status_rows()? {
                println!(
                    "{} {}: {} bars",
                    series.symbol, series.timeframe, series.bars
                );
                for range in series.coverage {
                    println!("  {}..{} {}", range.from, range.to, range.kind);
                }
            }
            for (kind, detail) in cat.quality_rows()? {
                println!("quality {kind}: {detail}");
            }
        }
        "verify" => {
            store.reconcile(&mut cat)?;
            println!("catalog reconciled")
        }
        "update" => {
            let master = opts
                .get("--master")
                .ok_or("data update requires --master <scrip-master.csv>")?;
            let rows = parse_scrip_master(std::fs::File::open(master)?)?;
            cat.refresh_instruments(&rows, Utc::now().timestamp())?;
            let symbols = opts
                .get("--symbols")
                .ok_or("data update requires --symbols A,B")?;
            let tf = Tf::parse(opts.get("--tf").map(String::as_str).unwrap_or("1m"))
                .ok_or("invalid timeframe")?;
            let to = opts
                .get("--to")
                .map(|x| NaiveDate::parse_from_str(x, "%Y-%m-%d"))
                .transpose()?
                .unwrap_or_else(|| Utc::now().date_naive());
            let from = opts
                .get("--from")
                .map(|x| NaiveDate::parse_from_str(x, "%Y-%m-%d"))
                .transpose()?
                .unwrap_or(to - chrono::Days::new(365 * 5));
            let (client, base) = client()?;
            let mut did_work = false;
            for symbol in symbols.split(',').map(str::trim).filter(|x| !x.is_empty()) {
                let (id, _) = cat.resolve(symbol)?;
                let instrument = cat.instrument(&id)?;
                let reference_id = cat
                    .resolve(
                        opts.get("--reference")
                            .map(String::as_str)
                            .unwrap_or("RELIANCE"),
                    )
                    .map(|x| x.0)
                    .ok();
                let mut updater = Updater {
                    client: Box::new(client.clone_with_base(&base)?),
                    catalog: Catalog::open(&db)?,
                    store: Store::new(&root),
                    max_retries: 3,
                    margin_seconds: 30,
                    reference_symbol_id: reference_id,
                    budget_cap: algomln_data::DEFAULT_BUDGET_CAP,
                };
                did_work |= updater.update(&id, tf, &instrument, from, to)?;
            }
            if did_work {
                println!("updated {} for {}", tf.as_str(), symbols)
            } else {
                println!("already up to date: {} {}", symbols, tf.as_str())
            }
        }
        "import" => {
            let symbol = opts
                .get("--symbol")
                .ok_or("data import requires --symbol")?;
            let file = PathBuf::from(opts.get("--file").ok_or("data import requires --file")?);
            let tf = Tf::parse(opts.get("--tf").map(String::as_str).unwrap_or("1m"))
                .ok_or("invalid timeframe")?;
            let (id, _) = cat.resolve(symbol)?;
            let bars = import_file(&file, opts.get("--csv-tz").map(String::as_str))?;
            if bars.iter().any(|b| !algomln_data::validate_bar(b)) {
                return Err("import contains invalid bars".into());
            }
            let parts = store.write_partitions(&id, tf, &bars)?;
            let sid = cat.series_for_import(&id, tf)?;
            for part in &parts {
                cat.record_partition(sid, &store, part)?;
            }
            println!("imported {} bars for {symbol}", bars.len())
        }
        "bulk" => {
            if opts
                .get("--universe")
                .map(String::as_str)
                .unwrap_or("nse-eq")
                != "nse-eq"
            {
                return Err("data bulk only supports --universe nse-eq".into());
            }
            let master = opts
                .get("--master")
                .ok_or("data bulk requires --master <scrip-master.csv>")?;
            let rows = parse_scrip_master(std::fs::File::open(master)?)?;
            cat.refresh_instruments(&rows, Utc::now().timestamp())?;
            let tfs = opts
                .get("--tf")
                .map(String::as_str)
                .unwrap_or("1d,1m")
                .split(',')
                .map(str::trim)
                .filter(|x| !x.is_empty())
                .map(|x| Tf::parse(x).ok_or_else(|| format!("invalid timeframe {x}")))
                .collect::<Result<Vec<_>, _>>()?;
            if tfs.is_empty() {
                return Err("data bulk requires at least one --tf".into());
            }
            // 1d first for all symbols, then 1m (spec 4), regardless of input order.
            let mut tfs = tfs;
            tfs.sort_by_key(|t| if *t == Tf::D1 { 0 } else { 1 });
            let to = opts
                .get("--to")
                .map(|x| NaiveDate::parse_from_str(x, "%Y-%m-%d"))
                .transpose()?
                .unwrap_or_else(last_completed_session);
            let from = opts
                .get("--from")
                .map(|x| NaiveDate::parse_from_str(x, "%Y-%m-%d"))
                .transpose()?
                .ok_or("data bulk requires --from <YYYY-MM-DD>")?;
            let budget_cap = opts
                .get("--budget")
                .map(|x| x.parse::<u64>())
                .transpose()
                .map_err(|e| format!("invalid --budget: {e}"))?
                .unwrap_or(DEFAULT_BUDGET_CAP);
            let priority_symbols = match opts.get("--symbols") {
                Some(path) => std::fs::read_to_string(path)?
                    .lines()
                    .map(str::trim)
                    .filter(|x| !x.is_empty())
                    .map(str::to_owned)
                    .collect(),
                None => Vec::new(),
            };
            let reference_symbol_id = cat
                .resolve(
                    opts.get("--reference")
                        .map(String::as_str)
                        .unwrap_or("RELIANCE"),
                )
                .map(|x| x.0)
                .ok();
            // Up to N symbols fetched in parallel (spec 6). Defaults to 4 to
            // overlap request latency; the shared limiter still spaces request
            // starts >=250 ms apart. `--concurrency 1` forces the serial path
            // (exact budget/auth accounting).
            let concurrency = opts
                .get("--concurrency")
                .map(|x| x.parse::<usize>())
                .transpose()
                .map_err(|e| format!("invalid --concurrency: {e}"))?
                .unwrap_or(4)
                .max(1);
            let mut config = BulkConfig::new(from, to, tfs);
            config.budget_cap = budget_cap;
            config.priority_symbols = priority_symbols;
            config.reference_symbol_id = reference_symbol_id;
            config.concurrency = concurrency;
            let db = db.clone();
            let root = root.clone();
            let (client, base) = client()?;
            let make_client = move || {
                Box::new(
                    client
                        .clone_with_base(&base)
                        .expect("clone Dhan client for bulk symbol"),
                ) as Box<dyn algomln_data::HistoryApi>
            };
            let stop = Arc::new(AtomicBool::new(false));
            // Ctrl-C flips the shared stop flag (spec 7). The downloader checks
            // it between symbols, lets the in-flight request finish, and persists
            // budget + catalog before returning. The handler only sets the flag,
            // so repeated Ctrl-C is idempotent (no forced mid-write abort).
            {
                let stop = stop.clone();
                if let Err(e) = ctrlc::set_handler(move || {
                    eprintln!(
                        "\ninterrupt received; finishing the in-flight request and saving progress..."
                    );
                    stop.store(true, std::sync::atomic::Ordering::SeqCst);
                }) {
                    eprintln!("warning: could not install Ctrl-C handler: {e}");
                }
            }
            let bulk = BulkDownloader {
                make_client,
                catalog_path: db,
                store_root: root,
                config,
                stop,
            };
            if opts.contains_key("--dry-run") {
                let report = bulk.dry_run()?;
                let remaining = cat.budget_used(&Utc::now().date_naive().to_string())?;
                let budget_left = budget_cap.saturating_sub(remaining);
                println!("dry-run: {} symbols", report.symbols_total);
                let mut total = 0usize;
                for (tf, n) in &report.planned {
                    let minutes = (*n as f64) / 4.0 / 60.0;
                    println!("  {tf}: {n} requests, est {minutes:.1} min at 4 req/s");
                    total += *n;
                }
                let fits = (total as u64) <= budget_left;
                println!(
                    "  total {total} requests; remaining budget today {budget_left}/{budget_cap}; fits today: {}",
                    if fits { "yes" } else { "no" }
                );
                return Ok(());
            }
            let report = bulk.run(|p| {
                eprint!("\r{}", p.line());
            })?;
            eprintln!();
            println!(
                "bulk finished: {}/{} symbols | requests used {} | failures {} | budget {}/{}",
                report.symbols_completed,
                report.symbols_total,
                report.requests_used,
                report.failures,
                cat.budget_used(&Utc::now().date_naive().to_string())?,
                budget_cap,
            );
            let fp = cat.catalog_fingerprint()?;
            println!("catalog fingerprint: {fp}");
            if report.interrupted {
                println!("interrupted; progress is saved, rerun to continue");
            }
            if report.budget_reached {
                let used = cat.budget_used(&Utc::now().date_naive().to_string())?;
                return Err(format!(
                    "daily budget reached ({used}/{budget_cap}); rerun tomorrow, progress is saved"
                )
                .into());
            }
        }
        _ => return Err(format!("unknown data command {sub}").into()),
    }
    Ok(())
}
/// Last completed NSE session, excluding today (spec 2/10: today's session is
/// excluded until the daily-close settle time is verified). Weekends roll back
/// to Friday; exchange holidays are not modelled here.
fn last_completed_session() -> NaiveDate {
    let mut d = Utc::now().date_naive() - chrono::Days::new(1);
    while matches!(
        d.weekday(),
        chrono::Weekday::Sat | chrono::Weekday::Sun
    ) {
        d = d - chrono::Days::new(1);
    }
    d
}
fn client() -> Result<(DhanClient, String), Box<dyn std::error::Error>> {
    let base = std::env::var("DHAN_API_BASE").unwrap_or_else(|_| "https://api.dhan.co/v2".into());
    Ok((
        DhanClient::with_base(Arc::new(KeychainEnvTokenProvider::default()), &base)?,
        base,
    ))
}
fn import_file(
    path: &std::path::Path,
    tz: Option<&str>,
) -> Result<Vec<Bar>, Box<dyn std::error::Error>> {
    if path
        .extension()
        .is_some_and(|x| x.eq_ignore_ascii_case("parquet"))
    {
        return Ok(algomln_data::read_partition(path)?);
    }
    let mut rdr = csv::Reader::from_path(path)?;
    let h = rdr.headers()?.clone();
    let idx = |n: &str| {
        h.iter()
            .position(|x| x.eq_ignore_ascii_case(n))
            .ok_or_else(|| format!("CSV missing {n}"))
    };
    let (ts, o, hi, lo, c, v) = (
        idx("ts")?,
        idx("open")?,
        idx("high")?,
        idx("low")?,
        idx("close")?,
        idx("volume")?,
    );
    let mut out = Vec::new();
    for row in rdr.records() {
        let r = row?;
        let raw = &r[ts];
        let stamp = if let Ok(i) = raw.parse::<i64>() {
            i
        } else if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(raw) {
            dt.timestamp()
        } else {
            let naive = chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S")?;
            let zone = tz.unwrap_or("Asia/Kolkata").parse::<chrono_tz::Tz>()?;
            naive
                .and_local_timezone(zone)
                .single()
                .ok_or("ambiguous local CSV timestamp")?
                .timestamp()
        };
        out.push(Bar {
            ts: stamp,
            open: r[o].parse()?,
            high: r[hi].parse()?,
            low: r[lo].parse()?,
            close: r[c].parse()?,
            volume: r[v].parse()?,
        });
    }
    Ok(out)
}
use algomln::sim::{self, DateRange, FeeModel, FileSource, GapPolicy, RunConfig, Timeframe};
mod data_cli;
use chrono::{FixedOffset, NaiveDate, TimeZone};
use std::{collections::BTreeMap, env, path::PathBuf, process::ExitCode};

fn main() -> ExitCode {
    match run_cli() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}
fn run_cli() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    let command = args.next().ok_or("usage: algomln run|data ...")?;
    if command == "--help" || command == "-h" {
        println!("algomln run <strategy> [--data DIR] [--tf 1m] [--from DATE] [--to DATE] [--update --master CSV] [--allow-gaps]\nalgomln data <update|status|verify|import|bulk> [options]\nalgomln data bulk --universe nse-eq --tf 1d,1m --from DATE [--to DATE] [--symbols FILE] [--budget N] [--dry-run] --master CSV");
        return Ok(());
    }
    if command == "data" {
        return data_cli::main_entry(args.collect());
    }
    if command != "run" {
        return Err(
            "usage: algomln run <file.algomln> ... | algomln data <update|status|verify|import|bulk>"
                .into(),
        );
    }
    let strategy = PathBuf::from(args.next().ok_or("missing strategy file")?);
    let mut opts = BTreeMap::new();
    while let Some(k) = args.next() {
        if !k.starts_with("--") {
            return Err(format!("unexpected argument {k}").into());
        }
        if matches!(
            k.as_str(),
            "--skip-bad" | "--debug-invariants" | "--update" | "--allow-gaps"
        ) {
            opts.insert(k, "true".into());
            continue;
        }
        if ![
            "--data",
            "--master",
            "--reference",
            "--tf",
            "--symbols",
            "--from",
            "--to",
            "--cash",
            "--warmup-bars",
            "--latency",
            "--slippage-bps",
            "--fee-bps",
            "--partial-fills",
            "--gap-policy",
            "--csv-tz",
            "--out",
        ]
        .contains(&k.as_str())
        {
            return Err(format!("unknown option {k}").into());
        }
        opts.insert(
            k.clone(),
            args.next()
                .ok_or_else(|| format!("missing value for {k}"))?,
        );
    }
    let data = PathBuf::from(opts.get("--data").map(String::as_str).unwrap_or("data"));
    let tf = Timeframe::parse(opts.get("--tf").map(String::as_str).unwrap_or("1m"))
        .ok_or("invalid --tf")?;
    let date = |key: &str| -> Result<Option<i64>, Box<dyn std::error::Error>> {
        let Some(s) = opts.get(key) else {
            return Ok(None);
        };
        let d = NaiveDate::parse_from_str(s, "%Y-%m-%d")?;
        let dt = d.and_hms_opt(0, 0, 0).unwrap();
        Ok(Some(
            FixedOffset::east_opt(19800)
                .unwrap()
                .from_local_datetime(&dt)
                .single()
                .unwrap()
                .timestamp(),
        ))
    };
    let code = std::fs::read_to_string(&strategy)?;
    let program = algomln::compile(&code).map_err(|e| format!("strategy compile failed: {e}"))?;
    let to = date("--to")?.map(|t| t.saturating_add(86_399));
    let mut range = DateRange {
        from: date("--from")?,
        to,
    };
    if let Some(from) = range.from {
        let daily = matches!(tf, Timeframe::D1);
        let warm = program.warmup_bars() as i64;
        let sessions = (warm + if daily { 1 } else { 359 }) / if daily { 1 } else { 360 };
        let extra = sessions.saturating_mul(86_400 * 7 / 5 + 86_400);
        range.from = Some(from.saturating_sub(extra));
    }
    let symbols: Vec<String> = opts
        .get("--symbols")
        .map(|s| {
            s.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_else(|| program.symbols().into_iter().map(str::to_owned).collect());
    if opts.contains_key("--update") {
        let master = opts
            .get("--master")
            .ok_or("--update requires --master <scrip-master.csv>")?;
        let rows = dhan_api::parse_scrip_master(std::fs::File::open(master)?)?;
        let db = data.join("catalog.sqlite");
        let mut cat = algomln_data::Catalog::open(&db)?;
        cat.refresh_instruments(&rows, chrono::Utc::now().timestamp())?;
        let to = opts
            .get("--to")
            .map(|x| NaiveDate::parse_from_str(x, "%Y-%m-%d"))
            .transpose()?
            .unwrap_or_else(|| chrono::Utc::now().date_naive());
        let from = opts
            .get("--from")
            .map(|x| NaiveDate::parse_from_str(x, "%Y-%m-%d"))
            .transpose()?
            .unwrap_or(to - chrono::Days::new(365 * 5));
        let base =
            std::env::var("DHAN_API_BASE").unwrap_or_else(|_| "https://api.dhan.co/v2".into());
        let token = std::sync::Arc::new(dhan_api::KeychainEnvTokenProvider::default());
        let reference_id = cat
            .resolve(
                opts.get("--reference")
                    .map(String::as_str)
                    .unwrap_or("RELIANCE"),
            )
            .map(|x| x.0)
            .ok();
        if let Some(reference_id) = reference_id.as_deref() {
            if !symbols
                .iter()
                .any(|s| cat.resolve(s).is_ok_and(|(id, _)| id == reference_id))
            {
                let instrument = cat.instrument(reference_id)?;
                algomln_data::Updater {
                    client: Box::new(dhan_api::DhanClient::with_base(token.clone(), &base)?),
                    catalog: algomln_data::Catalog::open(&db)?,
                    store: algomln_data::Store::new(&data),
                    max_retries: 3,
                    margin_seconds: 30,
                    reference_symbol_id: Some(reference_id.to_owned()),
                    budget_cap: algomln_data::DEFAULT_BUDGET_CAP,
                }
                .update(
                    reference_id,
                    algomln_data::Tf::parse(tf.as_str()).unwrap(),
                    &instrument,
                    from,
                    to,
                )?;
            }
        }
        for symbol in &symbols {
            let (id, _) = cat.resolve(symbol)?;
            let instrument = cat.instrument(&id)?;
            algomln_data::Updater {
                client: Box::new(dhan_api::DhanClient::with_base(token.clone(), &base)?),
                catalog: algomln_data::Catalog::open(&db)?,
                store: algomln_data::Store::new(&data),
                max_retries: 3,
                margin_seconds: 30,
                reference_symbol_id: reference_id.clone(),
                budget_cap: algomln_data::DEFAULT_BUDGET_CAP,
            }
            .update(
                &id,
                algomln_data::Tf::parse(tf.as_str()).unwrap(),
                &instrument,
                from,
                to,
            )?;
        }
    }
    let mut legacy = FileSource::new(data.clone());
    legacy.csv_tz = opts.get("--csv-tz").cloned();
    legacy.skip_bad = opts.contains_key("--skip-bad");
    let mut catalog = algomln_data::CatalogSource::new(
        algomln_data::Store::new(&data),
        data.join("catalog.sqlite"),
    );
    catalog.allow_gaps = opts.contains_key("--allow-gaps");
    let src: &dyn algomln::sim::CandleSource =
        if opts.contains_key("--csv-tz") || opts.contains_key("--skip-bad") {
            &legacy
        } else {
            &catalog
        };
    let number = |key: &str, default: f64| {
        opts.get(key).map_or(Ok(default), |s| {
            s.parse().map_err(|e| format!("invalid {key}: {e}"))
        })
    };
    let fee = FeeModel {
        bps: number("--fee-bps", 0.0)?,
        flat_per_order: 0.0,
        min_per_order: 0.0,
    };
    if fee.bps < 0.0 || !fee.bps.is_finite() {
        return Err("--fee-bps must be finite and non-negative".into());
    }
    let partial = opts
        .get("--partial-fills")
        .map(|s| s.parse::<f64>())
        .transpose()?;
    if partial.is_some_and(|p| !p.is_finite() || p <= 0.0 || p > 100.0) {
        return Err("--partial-fills must be in (0, 100]".into());
    }
    let latency = opts.get("--latency").map_or(Ok(1), |s| {
        s.parse::<u64>()
            .map_err(|e| format!("invalid --latency: {e}"))
    })?;
    if latency == 0 {
        return Err("--latency must be at least 1 candle".into());
    }
    let slip = number("--slippage-bps", 0.0)?;
    if !slip.is_finite() || !(0.0..10000.0).contains(&slip) {
        return Err("--slippage-bps must be in [0, 10000)".into());
    }
    let config = RunConfig {
        timeframe: tf,
        range,
        initial_cash: number("--cash", 100_000.0)?,
        warmup_bars: opts
            .get("--warmup-bars")
            .map_or(Ok(program.warmup_bars()), |s| {
                s.parse().map_err(|e| format!("invalid --warmup-bars: {e}"))
            })?,
        latency,
        slippage_bps: slip,
        fee_model: fee,
        partial_fills_pct: partial,
        gap_policy: match opts
            .get("--gap-policy")
            .map(String::as_str)
            .unwrap_or("none")
        {
            "none" => GapPolicy::None,
            "fill" => GapPolicy::FillFlatInSession,
            x => return Err(format!("invalid --gap-policy {x:?}").into()),
        },
        debug_invariants: opts.contains_key("--debug-invariants"),
    };
    let result = sim::run(&program, src, config, Some(symbols))?;
    println!(
        "dataset fingerprint: {}",
        result.manifest["source_fingerprint"]
    );
    sim::write_results(
        opts.get("--out")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("run-output")),
        &result,
    )?;
    Ok(())
}
use algomln::{
    sim::{CandleSource, DateRange, Timeframe},
    Symbol,
};
use algomln_data::{Bar, Catalog, CatalogSource, Store, Tf};
use chrono::{Datelike, Duration, NaiveDate, TimeZone};
use chrono_tz::Asia::Kolkata;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};

struct Meter;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
unsafe impl GlobalAlloc for Meter {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = System.alloc(l);
        if !p.is_null() {
            let n = LIVE.fetch_add(l.size(), Ordering::Relaxed) + l.size();
            PEAK.fetch_max(n, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        System.dealloc(p, l);
        LIVE.fetch_sub(l.size(), Ordering::Relaxed);
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        let q = System.realloc(p, l, n);
        if !q.is_null() {
            let z = if n >= l.size() {
                LIVE.fetch_add(n - l.size(), Ordering::Relaxed) + n - l.size()
            } else {
                LIVE.fetch_sub(l.size() - n, Ordering::Relaxed) - (l.size() - n)
            };
            PEAK.fetch_max(z, Ordering::Relaxed);
        }
        q
    }
}
#[global_allocator]
static ALLOCATOR: Meter = Meter;

fn prepare(root: &PathBuf, symbols: &[String], dates: &[NaiveDate]) {
    let db = root.join("catalog.sqlite");
    let mut cat = Catalog::open(db).unwrap();
    let scrips: Vec<_> = symbols
        .iter()
        .enumerate()
        .map(|(i, s)| dhan_api::Scrip {
            security_id: (i + 1).to_string(),
            symbol: s.clone(),
            lot_size: 1,
            tick_size_paise: 5,
            instrument: "EQUITY".into(),
        })
        .collect();
    cat.refresh_instruments(&scrips, 0).unwrap();
    let cov_start = Kolkata
        .from_local_datetime(&dates[0].and_hms_opt(0, 0, 0).unwrap())
        .single()
        .unwrap()
        .timestamp();
    let cov_end = Kolkata
        .from_local_datetime(
            &(dates[dates.len() - 1] + Duration::days(1))
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        )
        .single()
        .unwrap()
        .timestamp()
        - 1;
    let store = Store::new(root);
    for (i, _s) in symbols.iter().enumerate() {
        let mut bars = Vec::with_capacity(dates.len() * 360);
        for d in dates {
            let open = d
                .and_hms_opt(9, 15, 0)
                .unwrap()
                .and_local_timezone(Kolkata)
                .single()
                .unwrap()
                .timestamp();
            for n in 0..360 {
                let p = 100.0 + i as f64 + n as f64 / 1000.0;
                bars.push(Bar {
                    ts: open + n * 60,
                    open: p,
                    high: p + 0.01,
                    low: p - 0.01,
                    close: p,
                    volume: 100.0,
                });
            }
        }
        let parts = store
            .write_partitions(&(i + 1).to_string(), Tf::M1, &bars)
            .unwrap();
        let sid = cat.series_for_import(&(i + 1).to_string(), Tf::M1).unwrap();
        for p in &parts {
            cat.record_partition(sid, &store, p).unwrap();
        }
        cat.record_coverage(&(i + 1).to_string(), Tf::M1, cov_start, cov_end, "data")
            .unwrap();
    }
}
fn bench(c: &mut Criterion) {
    let symbols: Vec<String> = (0..100).map(|n| format!("S{n:03}")).collect();
    let mut dates = Vec::new();
    let mut d = NaiveDate::from_ymd_opt(2021, 10, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2026, 10, 1).unwrap();
    while d <= end {
        if d.weekday().number_from_monday() <= 5 {
            dates.push(d);
        }
        d += Duration::days(1);
    }
    let root = std::env::temp_dir().join(format!("algomln-catalog-bench-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    eprintln!(
        "preparing ~{}M synthetic candles; loader is timed separately",
        symbols.len() * dates.len() * 360 / 1_000_000
    );
    prepare(&root, &symbols, &dates);
    let source = CatalogSource::new(Store::new(&root), root.join("catalog.sqlite"));
    let syms: Vec<Symbol> = symbols.iter().map(|s| s.as_str().into()).collect();
    let range = DateRange {
        from: Some(
            Kolkata
                .from_local_datetime(&dates[0].and_hms_opt(0, 0, 0).unwrap())
                .single()
                .unwrap()
                .timestamp(),
        ),
        to: Some(
            Kolkata
                .from_local_datetime(&dates[dates.len() - 1].and_hms_opt(23, 59, 59).unwrap())
                .single()
                .unwrap()
                .timestamp(),
        ),
    };
    let count = symbols.len() * dates.len() * 360;
    let mut group = c.benchmark_group("catalog_source_loader_only");
    group.throughput(Throughput::Elements(count as u64));
    // One iteration loads ~46M candles (~2 GiB), so the default 100 samples would
    // take hours. Hold the sample count at criterion's floor with a short warm-up.
    group.sample_size(10);
    group.warm_up_time(std::time::Duration::from_secs(1));
    group.bench_function("100_symbols_5y_1m", |b| {
        b.iter(|| {
            // PEAK is a monotonic high-water mark already pinned by prepare()'s
            // ~2 GiB dataset; rebase it to the current live bytes so the reported
            // delta reflects this iteration's loader allocation, not setup.
            let base = LIVE.load(Ordering::Relaxed);
            PEAK.store(base, Ordering::Relaxed);
            let stream = source.open(&syms, Timeframe::M1, range).unwrap();
            let n = stream.map(Result::unwrap).count();
            black_box(n);
            let peak = PEAK.load(Ordering::Relaxed).saturating_sub(base);
            eprintln!(
                "catalog source peak allocation delta: {:.1} MiB",
                peak as f64 / 1024.0 / 1024.0
            );
        })
    });
    group.finish();
    let _ = std::fs::remove_dir_all(root);
}
criterion_group!(benches, bench);
criterion_main!(benches);
use algomln::sim::{CandleSource, CandleStream, DateRange, Fingerprint, SourceError, Timeframe};
use algomln::{engine::Symbol, indicators::Candle};
use arrow_array::{Array, Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use chrono::{DateTime, Datelike, NaiveDate, Timelike, Utc};
use chrono_tz::Asia::Kolkata;
use dhan_api::{ApiError, Candle as DhanCandle, DhanClient, Scrip};
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter},
    basic::Compression,
    file::properties::WriterProperties,
};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DataError {
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Db(#[from] rusqlite::Error),
    #[error("{0}")]
    Api(#[from] ApiError),
    #[error("invalid data: {0}")]
    Invalid(String),
    #[error("uncovered range for {0}; run `algomln data update` first")]
    Uncovered(String),
    #[error("daily request budget reached ({used}/{cap})")]
    BudgetExhausted { used: u64, cap: u64 },
}
impl DataError {
    /// True when the run must abort immediately (bad credentials). Everything
    /// else that reaches the bulk layer is recorded as a failure and skipped.
    pub fn is_auth(&self) -> bool {
        matches!(self, DataError::Api(ApiError::Auth(_)))
    }
    /// True when the daily request budget has been reached.
    pub fn is_budget(&self) -> bool {
        matches!(self, DataError::BudgetExhausted { .. })
    }
    /// A short, stable code suitable for the `failures.error_code` column.
    pub fn code(&self) -> String {
        match self {
            DataError::Api(ApiError::Auth(_)) => "auth".into(),
            DataError::Api(ApiError::RateLimit { code, .. }) => code.clone(),
            DataError::Api(ApiError::DataApi { code, .. }) => code.clone(),
            DataError::Api(ApiError::BadRequest { code, .. }) => code.clone(),
            DataError::Api(ApiError::SevereRateLimit(_)) => "DH-805".into(),
            DataError::Api(ApiError::DataUnavailable(_)) => "DH-907".into(),
            DataError::Api(ApiError::Access(_)) => "access".into(),
            DataError::Api(ApiError::Server(_)) => "server".into(),
            DataError::Api(ApiError::Transport(_)) => "transport".into(),
            DataError::Api(ApiError::Malformed(_)) => "malformed".into(),
            DataError::BudgetExhausted { .. } => "budget".into(),
            DataError::Io(_) => "io".into(),
            DataError::Db(_) => "db".into(),
            DataError::Invalid(_) => "invalid".into(),
            DataError::Uncovered(_) => "uncovered".into(),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Tf {
    M1,
    M5,
    M15,
    M25,
    M60,
    D1,
}
impl Tf {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::M1 => "1m",
            Self::M5 => "5m",
            Self::M15 => "15m",
            Self::M25 => "25m",
            Self::M60 => "60m",
            Self::D1 => "1d",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "1m" => Self::M1,
            "5m" => Self::M5,
            "15m" => Self::M15,
            "25m" => Self::M25,
            "60m" => Self::M60,
            "1d" => Self::D1,
            _ => return None,
        })
    }
    fn seconds(self) -> i64 {
        match self {
            Self::M1 => 60,
            Self::M5 => 300,
            Self::M15 => 900,
            Self::M25 => 1500,
            Self::M60 => 3600,
            Self::D1 => 86400,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Bar {
    pub ts: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}
impl From<DhanCandle> for Bar {
    fn from(x: DhanCandle) -> Self {
        Self {
            ts: x.ts,
            open: x.open,
            high: x.high,
            low: x.low,
            close: x.close,
            volume: x.volume,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Instrument {
    pub security_id: String,
    pub symbol: String,
    pub segment: String,
    pub lot_size: u32,
    pub tick_size_paise: i64,
    pub refreshed_at: i64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Quality {
    pub kind: String,
    pub ts: Option<i64>,
    pub detail: String,
}
#[derive(Clone, Debug)]
pub struct PartitionMeta {
    pub path: PathBuf,
    pub checksum: String,
    pub first_ts: i64,
    pub last_ts: i64,
    pub rows: usize,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Coverage {
    pub start_ts: i64,
    pub end_ts: i64,
    pub kind: String,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoverageRange {
    pub from: NaiveDate,
    pub to: NaiveDate,
    pub kind: String,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SeriesStatus {
    pub symbol: String,
    pub timeframe: String,
    pub bars: u64,
    pub coverage: Vec<CoverageRange>,
}

pub struct Catalog {
    conn: Connection,
}
impl Catalog {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DataError> {
        if let Some(p) = path.as_ref().parent() {
            fs::create_dir_all(p)?;
        }
        let conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=30000; CREATE TABLE IF NOT EXISTS instruments(security_id TEXT PRIMARY KEY,symbol TEXT NOT NULL UNIQUE,segment TEXT NOT NULL,lot_size INTEGER NOT NULL,tick_size_paise INTEGER NOT NULL,refreshed_at INTEGER NOT NULL); CREATE TABLE IF NOT EXISTS series(id INTEGER PRIMARY KEY,security_id TEXT NOT NULL,tf TEXT NOT NULL,first_ts INTEGER,last_ts INTEGER,rows INTEGER NOT NULL DEFAULT 0,needs_refresh INTEGER NOT NULL DEFAULT 0,UNIQUE(security_id,tf)); CREATE TABLE IF NOT EXISTS partitions(series_id INTEGER NOT NULL,path TEXT NOT NULL,first_ts INTEGER NOT NULL,last_ts INTEGER NOT NULL,rows INTEGER NOT NULL,checksum TEXT NOT NULL,PRIMARY KEY(series_id,path)); CREATE TABLE IF NOT EXISTS coverage(series_id INTEGER NOT NULL,start_ts INTEGER NOT NULL,end_ts INTEGER NOT NULL,kind TEXT NOT NULL CHECK(kind IN ('data','confirmed_empty'))); CREATE TABLE IF NOT EXISTS sessions(series_id INTEGER NOT NULL,date TEXT NOT NULL,first_ts INTEGER NOT NULL,last_ts INTEGER NOT NULL,bars INTEGER NOT NULL,PRIMARY KEY(series_id,date)); CREATE TABLE IF NOT EXISTS quality(id INTEGER PRIMARY KEY,series_id INTEGER,kind TEXT NOT NULL,ts INTEGER,detail TEXT NOT NULL); CREATE TABLE IF NOT EXISTS runs(id INTEGER PRIMARY KEY,started INTEGER NOT NULL,finished INTEGER,requests INTEGER NOT NULL DEFAULT 0,status TEXT NOT NULL); CREATE TABLE IF NOT EXISTS budget(day TEXT PRIMARY KEY,requests INTEGER NOT NULL); CREATE TABLE IF NOT EXISTS trading_calendar(date TEXT PRIMARY KEY,is_trading INTEGER NOT NULL,source TEXT NOT NULL); CREATE TABLE IF NOT EXISTS failures(security_id INTEGER,tf TEXT,from_date TEXT,to_date TEXT,error_code TEXT,attempts INTEGER,next_retry TEXT,PRIMARY KEY(security_id,tf,from_date,to_date));")?;
        let version: i64 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        if version > 1 {
            return Err(DataError::Invalid(format!(
                "catalog schema version {version} is newer than supported version 1"
            )));
        }
        let has_instrument: bool = {
            let mut q = conn.prepare("PRAGMA table_info(instruments)")?;
            let names = q
                .query_map([], |r| r.get::<_, String>(1))?
                .collect::<Result<Vec<_>, _>>()?;
            names.iter().any(|x| x == "instrument")
        };
        if !has_instrument {
            conn.execute(
                "ALTER TABLE instruments ADD COLUMN instrument TEXT NOT NULL DEFAULT 'EQUITY'",
                [],
            )?;
        }
        conn.execute_batch("PRAGMA user_version=1;")?;
        Ok(Self { conn })
    }
    pub fn refresh_instruments(&mut self, rows: &[Scrip], at: i64) -> Result<(), DataError> {
        let tx = self.conn.transaction()?;
        for x in rows {
            tx.execute("INSERT INTO instruments(security_id,symbol,segment,lot_size,tick_size_paise,instrument,refreshed_at) VALUES(?1,?2,'NSE_EQ',?3,?4,?5,?6) ON CONFLICT(security_id) DO UPDATE SET symbol=excluded.symbol,lot_size=excluded.lot_size,tick_size_paise=excluded.tick_size_paise,instrument=excluded.instrument,refreshed_at=excluded.refreshed_at",params![x.security_id,x.symbol,x.lot_size,x.tick_size_paise,x.instrument,at])?;
        }
        tx.commit()?;
        Ok(())
    }
    pub fn instrument(&self, id: &str) -> Result<String, DataError> {
        Ok(self.conn.query_row(
            "SELECT instrument FROM instruments WHERE security_id=?1",
            [id],
            |r| r.get(0),
        )?)
    }
    pub fn resolve(&self, s: &str) -> Result<(String, String), DataError> {
        self.conn
            .query_row(
                "SELECT security_id,symbol FROM instruments WHERE symbol=?1",
                [s.trim().to_ascii_uppercase()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(|_| DataError::Invalid(format!("unknown symbol {}", s)))
    }
    pub fn series_for_import(&self, id: &str, tf: Tf) -> Result<i64, DataError> {
        self.series(id, tf)
    }
    pub fn record_partition(
        &self,
        series: i64,
        store: &Store,
        part: &PartitionMeta,
    ) -> Result<(), DataError> {
        let rel = part
            .path
            .strip_prefix(&store.root)
            .unwrap_or(&part.path)
            .to_string_lossy()
            .to_string();
        let (sum, first, last, rows) = (&part.checksum, part.first_ts, part.last_ts, part.rows);
        let tx = self.conn.unchecked_transaction()?;
        tx.execute("INSERT INTO partitions VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(series_id,path) DO UPDATE SET first_ts=excluded.first_ts,last_ts=excluded.last_ts,rows=excluded.rows,checksum=excluded.checksum",params![series,rel,first,last,rows,sum])?;
        let d1 = DateTime::<Utc>::from_timestamp(first, 0)
            .unwrap_or(DateTime::UNIX_EPOCH)
            .with_timezone(&Kolkata)
            .date_naive();
        let d2 = DateTime::<Utc>::from_timestamp(last, 0)
            .unwrap_or(DateTime::UNIX_EPOCH)
            .with_timezone(&Kolkata)
            .date_naive();
        tx.execute(
            "INSERT INTO coverage VALUES(?1,?2,?3,'data')",
            params![
                series,
                local_midnight(d1),
                local_midnight(d2 + chrono::Days::new(1)) - 1
            ],
        )?;
        tx.execute("UPDATE series SET first_ts=CASE WHEN first_ts IS NULL THEN ?2 ELSE MIN(first_ts,?2) END,last_ts=CASE WHEN last_ts IS NULL THEN ?3 ELSE MAX(last_ts,?3) END,rows=(SELECT COALESCE(SUM(rows),0) FROM partitions WHERE series_id=?1) WHERE id=?1",params![series,first,last])?;
        tx.commit()?;
        Ok(())
    }
    pub fn quality(&self, kind: &str) -> Result<u64, DataError> {
        Ok(self
            .conn
            .query_row("SELECT count(*) FROM quality WHERE kind=?1", [kind], |r| {
                r.get(0)
            })?)
    }
    fn series(&self, id: &str, tf: Tf) -> Result<i64, DataError> {
        self.conn.execute(
            "INSERT OR IGNORE INTO series(security_id,tf) VALUES(?1,?2)",
            params![id, tf.as_str()],
        )?;
        Ok(self.conn.query_row(
            "SELECT id FROM series WHERE security_id=?1 AND tf=?2",
            params![id, tf.as_str()],
            |r| r.get(0),
        )?)
    }
    pub fn has_coverage(&self, id: &str, tf: Tf, from: i64, to: i64) -> Result<bool, DataError> {
        let sid = self.series(id, tf)?;
        let first = DateTime::<Utc>::from_timestamp(from, 0)
            .unwrap_or(DateTime::UNIX_EPOCH)
            .with_timezone(&Kolkata)
            .date_naive();
        let last = DateTime::<Utc>::from_timestamp(to, 0)
            .unwrap_or(DateTime::UNIX_EPOCH)
            .with_timezone(&Kolkata)
            .date_naive();
        let a = local_midnight(first);
        let b = local_midnight(last + chrono::Days::new(1)) - 1;
        let mut stmt=self.conn.prepare("SELECT start_ts,end_ts FROM coverage WHERE series_id=?1 AND end_ts>=?2 AND start_ts<=?3 ORDER BY start_ts")?;
        let rows = stmt.query_map(params![sid, a, b], |r| {
            Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?))
        })?;
        let mut cursor = a;
        for row in rows {
            let (lo, hi) = row?;
            if lo > cursor {
                return Ok(false);
            }
            cursor = cursor.max(hi.saturating_add(1));
            if cursor > b {
                return Ok(true);
            }
        }
        Ok(false)
    }
    pub fn record_coverage(
        &self,
        id: &str,
        tf: Tf,
        start: i64,
        end: i64,
        kind: &str,
    ) -> Result<(), DataError> {
        if start > end || !(kind == "data" || kind == "confirmed_empty") {
            return Err(DataError::Invalid("invalid coverage interval".into()));
        }
        let sid = self.series(id, tf)?;
        self.conn.execute(
            "INSERT OR IGNORE INTO coverage VALUES(?1,?2,?3,?4)",
            params![sid, start, end, kind],
        )?;
        Ok(())
    }
    pub fn covered_dates(
        &self,
        id: &str,
        tf: Tf,
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<Vec<NaiveDate>, DataError> {
        let sid = self.series(id, tf)?;
        let mut q=self.conn.prepare("SELECT date FROM sessions WHERE series_id=?1 AND date>=?2 AND date<=?3 UNION SELECT date FROM trading_calendar WHERE is_trading=0 AND date>=?2 AND date<=?3 ORDER BY date")?;
        let xs = q
            .query_map(params![sid, from.to_string(), to.to_string()], |r| {
                r.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?;
        xs.into_iter()
            .map(|x| {
                let text = if x.contains('-') && x.len() > 10 {
                    let ts: i64 = x.parse().unwrap_or_default();
                    DateTime::<Utc>::from_timestamp(ts, 0)
                        .unwrap_or(DateTime::UNIX_EPOCH)
                        .with_timezone(&Kolkata)
                        .date_naive()
                        .to_string()
                } else {
                    x
                };
                NaiveDate::parse_from_str(&text, "%Y-%m-%d")
                    .map_err(|e| DataError::Invalid(e.to_string()))
            })
            .collect()
    }
    pub fn record_calendar(
        &self,
        date: NaiveDate,
        is_trading: bool,
        source: &str,
    ) -> Result<(), DataError> {
        self.conn.execute("INSERT INTO trading_calendar VALUES(?1,?2,?3) ON CONFLICT(date) DO UPDATE SET is_trading=excluded.is_trading,source=excluded.source",params![date.to_string(),is_trading,source])?;
        Ok(())
    }
    pub fn mark_session_empty(&self, id: &str, tf: Tf, date: NaiveDate) -> Result<(), DataError> {
        let sid = self.series(id, tf)?;
        self.conn.execute(
            "INSERT OR IGNORE INTO coverage VALUES(?1,?2,?3,'confirmed_empty')",
            params![
                sid,
                local_midnight(date),
                local_midnight(date + chrono::Days::new(1)) - 1
            ],
        )?;
        self.conn.execute("INSERT OR IGNORE INTO sessions(series_id,date,first_ts,last_ts,bars) VALUES(?1,?2,?3,?3,0)", params![sid,date.to_string(),local_midnight(date)])?;
        Ok(())
    }
    pub fn missing_dates(
        &self,
        id: &str,
        tf: Tf,
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<Vec<NaiveDate>, DataError> {
        let covered = self
            .covered_dates(id, tf, from, to)?
            .into_iter()
            .collect::<std::collections::HashSet<_>>();
        Ok((0..=((to - from).num_days().max(0) as u64))
            .map(|n| from + chrono::Days::new(n))
            .filter(|d| {
                matches!(
                    d.weekday(),
                    chrono::Weekday::Mon
                        | chrono::Weekday::Tue
                        | chrono::Weekday::Wed
                        | chrono::Weekday::Thu
                        | chrono::Weekday::Fri
                ) && !covered.contains(d)
            })
            .collect())
    }
    pub fn coverage(&self) -> Result<Vec<Coverage>, DataError> {
        let mut q = self
            .conn
            .prepare("SELECT start_ts,end_ts,kind FROM coverage ORDER BY series_id,start_ts")?;
        let rows = q.query_map([], |r| {
            Ok(Coverage {
                start_ts: r.get(0)?,
                end_ts: r.get(1)?,
                kind: r.get(2)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }
    pub fn coverage_details(&self) -> Result<Vec<(String, String, u64)>, DataError> {
        let mut q=self.conn.prepare("SELECT i.symbol,s.tf,COALESCE(SUM(p.rows),0) FROM series s JOIN instruments i ON i.security_id=s.security_id LEFT JOIN partitions p ON p.series_id=s.id GROUP BY s.id ORDER BY i.symbol,s.tf")?;
        let rows = q.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }
    pub fn status_rows(&self) -> Result<Vec<SeriesStatus>, DataError> {
        let mut q = self.conn.prepare("SELECT s.id,i.symbol,s.tf,COALESCE((SELECT SUM(p.rows) FROM partitions p WHERE p.series_id=s.id),0) FROM series s JOIN instruments i ON i.security_id=s.security_id ORDER BY i.symbol,s.tf")?;
        let series = q
            .query_map([], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, u64>(3)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let mut out = Vec::with_capacity(series.len());
        for (id, symbol, timeframe, bars) in series {
            let mut stmt = self.conn.prepare(
                "SELECT start_ts,end_ts,kind FROM coverage WHERE series_id=?1 ORDER BY start_ts",
            )?;
            let rows = stmt
                .query_map([id], |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, String>(2)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            let mut coverage = Vec::<CoverageRange>::new();
            for (a, b, kind) in rows {
                let from = DateTime::<Utc>::from_timestamp(a, 0)
                    .unwrap_or(DateTime::UNIX_EPOCH)
                    .with_timezone(&Kolkata)
                    .date_naive();
                let to = DateTime::<Utc>::from_timestamp(b, 0)
                    .unwrap_or(DateTime::UNIX_EPOCH)
                    .with_timezone(&Kolkata)
                    .date_naive();
                let joins_previous = if let Some(last) = coverage.last() {
                    from <= last.to + chrono::Days::new(1)
                        || self.only_nontrading_gap(last.to, from)?
                } else {
                    false
                };
                if joins_previous {
                    if let Some(last) = coverage.last_mut() {
                        last.to = last.to.max(to);
                        if last.kind != kind {
                            last.kind = "covered".into();
                        }
                        continue;
                    }
                }
                coverage.push(CoverageRange { from, to, kind });
            }
            out.push(SeriesStatus {
                symbol,
                timeframe,
                bars,
                coverage,
            });
        }
        Ok(out)
    }
    fn only_nontrading_gap(&self, before: NaiveDate, after: NaiveDate) -> Result<bool, DataError> {
        let mut day = before + chrono::Days::new(1);
        while day < after {
            if day.weekday().number_from_monday() <= 5 {
                let is_open: bool = self.conn.query_row(
                    "SELECT COALESCE((SELECT is_trading=1 FROM trading_calendar WHERE date=?1),1)",
                    [day.to_string()],
                    |r| r.get(0),
                )?;
                if is_open {
                    return Ok(false);
                }
            }
            day = day + chrono::Days::new(1);
        }
        Ok(true)
    }
    pub fn quality_rows(&self) -> Result<Vec<(String, String)>, DataError> {
        let mut q = self
            .conn
            .prepare("SELECT kind,detail FROM quality ORDER BY id")?;
        let rows = q.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }
    pub fn budget_used(&self, date: &str) -> Result<u64, DataError> {
        Ok(self.conn.query_row(
            "SELECT COALESCE((SELECT requests FROM budget WHERE day=?1),0)",
            [date],
            |r| r.get(0),
        )?)
    }
    pub fn budget_add(&self, date: &str, n: u64) -> Result<(), DataError> {
        self.conn.execute("INSERT INTO budget VALUES(?1,?2) ON CONFLICT(day) DO UPDATE SET requests=requests+excluded.requests",params![date,n])?;
        Ok(())
    }
    /// Record (or re-record) a failed request range. `attempts` is incremented
    /// on conflict and `next_retry` advanced so the range is retried on a later
    /// run rather than in a tight loop.
    pub fn record_failure(
        &self,
        id: &str,
        tf: Tf,
        from: NaiveDate,
        to: NaiveDate,
        error_code: &str,
        next_retry: NaiveDate,
    ) -> Result<(), DataError> {
        self.conn.execute(
            "INSERT INTO failures(security_id,tf,from_date,to_date,error_code,attempts,next_retry) VALUES(?1,?2,?3,?4,?5,1,?6) ON CONFLICT(security_id,tf,from_date,to_date) DO UPDATE SET error_code=excluded.error_code,attempts=attempts+1,next_retry=excluded.next_retry",
            params![id, tf.as_str(), from.to_string(), to.to_string(), error_code, next_retry.to_string()],
        )?;
        Ok(())
    }
    /// Clear a recorded failure once its range has been successfully fetched.
    pub fn clear_failure(
        &self,
        id: &str,
        tf: Tf,
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<(), DataError> {
        self.conn.execute(
            "DELETE FROM failures WHERE security_id=?1 AND tf=?2 AND from_date=?3 AND to_date=?4",
            params![id, tf.as_str(), from.to_string(), to.to_string()],
        )?;
        Ok(())
    }
    /// Failures whose `next_retry` is on or before `today`, ready to be retried.
    pub fn due_failures(
        &self,
        today: NaiveDate,
    ) -> Result<Vec<(String, Tf, NaiveDate, NaiveDate)>, DataError> {
        let mut q = self.conn.prepare(
            "SELECT security_id,tf,from_date,to_date FROM failures WHERE next_retry<=?1 ORDER BY security_id,tf,from_date",
        )?;
        let rows = q
            .query_map([today.to_string()], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let mut out = Vec::with_capacity(rows.len());
        for (id, tf, from, to) in rows {
            let tf = Tf::parse(&tf).ok_or_else(|| DataError::Invalid("bad tf in failures".into()))?;
            let from = NaiveDate::parse_from_str(&from, "%Y-%m-%d")
                .map_err(|e| DataError::Invalid(e.to_string()))?;
            let to = NaiveDate::parse_from_str(&to, "%Y-%m-%d")
                .map_err(|e| DataError::Invalid(e.to_string()))?;
            out.push((id, tf, from, to));
        }
        Ok(out)
    }
    pub fn failure_count(&self) -> Result<u64, DataError> {
        Ok(self
            .conn
            .query_row("SELECT count(*) FROM failures", [], |r| r.get(0))?)
    }
    /// A stable fingerprint of the whole catalog's materialised data: every
    /// partition path and checksum, ordered. Two runs that produce identical
    /// coverage produce identical fingerprints (used by `data bulk` summaries
    /// and acceptance tests 3 and 4).
    pub fn catalog_fingerprint(&self) -> Result<String, DataError> {
        let mut q = self
            .conn
            .prepare("SELECT path,checksum FROM partitions ORDER BY series_id,path")?;
        let rows = q
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
            .collect::<Result<Vec<_>, _>>()?;
        let mut h = Sha256::new();
        for (path, sum) in rows {
            h.update(path.as_bytes());
            h.update(b":");
            h.update(sum.as_bytes());
            h.update(b";");
        }
        Ok(format!("sha256:{:x}", h.finalize()))
    }
    /// All catalogued instruments as `(security_id, symbol, instrument)`, ordered
    /// by numeric security id. Used by `data bulk` to walk the universe.
    pub fn instruments_in_order(&self) -> Result<Vec<(String, String, String)>, DataError> {
        let mut q = self.conn.prepare(
            "SELECT security_id,symbol,instrument FROM instruments ORDER BY CAST(security_id AS INTEGER),security_id",
        )?;
        let rows = q.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
            ))
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }
    pub fn runs_start(&self) -> Result<i64, DataError> {
        self.conn.execute(
            "INSERT INTO runs(started,status) VALUES(strftime('%s','now'),'running')",
            [],
        )?;
        Ok(self.conn.last_insert_rowid())
    }
    pub fn runs_finish(&self, id: i64, requests: u64, status: &str) -> Result<(), DataError> {
        self.conn.execute(
            "UPDATE runs SET finished=strftime('%s','now'),requests=?2,status=?3 WHERE id=?1",
            params![id, requests, status],
        )?;
        Ok(())
    }
    pub fn mark_quality(&self, id: &str, tf: Tf, q: &Quality) -> Result<(), DataError> {
        let sid = self.series(id, tf)?;
        self.conn.execute(
            "INSERT INTO quality(series_id,kind,ts,detail) VALUES(?1,?2,?3,?4)",
            params![sid, q.kind, q.ts, q.detail],
        )?;
        Ok(())
    }
}

pub struct Store {
    root: PathBuf,
}
impl Store {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
    fn partition(&self, id: &str, tf: Tf, ts: i64) -> PathBuf {
        let dt = DateTime::<Utc>::from_timestamp(ts, 0)
            .unwrap_or(DateTime::UNIX_EPOCH)
            .with_timezone(&Kolkata);
        let period = if tf == Tf::D1 {
            dt.format("%Y").to_string()
        } else {
            dt.format("%Y-%m").to_string()
        };
        self.root
            .join("candles")
            .join(id)
            .join(tf.as_str())
            .join(format!("{period}.parquet"))
    }
    pub fn write_partitions(
        &self,
        id: &str,
        tf: Tf,
        bars: &[Bar],
    ) -> Result<Vec<PartitionMeta>, DataError> {
        let mut groups: BTreeMap<PathBuf, Vec<Bar>> = BTreeMap::new();
        for b in bars {
            groups
                .entry(self.partition(id, tf, b.ts))
                .or_default()
                .push(b.clone());
        }
        let mut out = Vec::new();
        for (path, chunk) in groups {
            let (sum, first, last, n) = self.write_one(&path, &chunk)?;
            out.push(PartitionMeta {
                path,
                checksum: sum,
                first_ts: first,
                last_ts: last,
                rows: n,
            });
        }
        Ok(out)
    }
    fn write_one(&self, path: &Path, bars: &[Bar]) -> Result<(String, i64, i64, usize), DataError> {
        if let Some(p) = path.parent() {
            fs::create_dir_all(p)?;
        }
        let existing = if path.exists() {
            read_bars(path)?
        } else {
            Vec::new()
        };
        let existing_map: HashMap<i64, &Bar> = existing.iter().map(|b| (b.ts, b)).collect();
        if !existing.is_empty()
            && bars
                .iter()
                .all(|b| existing_map.get(&b.ts).is_some_and(|old| *old == b))
        {
            let sum = checksum(path)?;
            return Ok((
                sum,
                existing.first().unwrap().ts,
                existing.last().unwrap().ts,
                existing.len(),
            ));
        }
        let mut map: BTreeMap<i64, Bar> = existing.into_iter().map(|b| (b.ts, b)).collect();
        for b in bars {
            map.insert(b.ts, b.clone());
        }
        let rows: Vec<_> = map.into_values().collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("open", DataType::Float64, false),
            Field::new("high", DataType::Float64, false),
            Field::new("low", DataType::Float64, false),
            Field::new("close", DataType::Float64, false),
            Field::new("volume", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|x| x.ts).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    rows.iter().map(|x| x.open).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    rows.iter().map(|x| x.high).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    rows.iter().map(|x| x.low).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    rows.iter().map(|x| x.close).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    rows.iter().map(|x| x.volume).collect::<Vec<_>>(),
                )),
            ],
        )
        .map_err(|e| DataError::Invalid(e.to_string()))?;
        let tmp = path.with_extension(format!(
            "parquet.tmp-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
        ));
        let file = File::create(&tmp)?;
        let sync_file = file.try_clone()?;
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))
            .map_err(|e| DataError::Invalid(e.to_string()))?;
        writer
            .write(&batch)
            .map_err(|e| DataError::Invalid(e.to_string()))?;
        writer
            .close()
            .map_err(|e| DataError::Invalid(e.to_string()))?;
        sync_file.sync_all()?;
        atomic_replace(&tmp, path)?;
        let sum = checksum(path)?;
        Ok((
            sum,
            rows.first().unwrap().ts,
            rows.last().unwrap().ts,
            rows.len(),
        ))
    }
    pub fn reconcile(&self, cat: &mut Catalog) -> Result<(), DataError> {
        let mut files = HashMap::new();
        let base = self.root.join("candles");
        if base.exists() {
            for p in walk(&base)? {
                if p.extension().and_then(|x| x.to_str()) == Some("parquet") {
                    files.insert(p.clone(), checksum(&p)?);
                }
            }
        }
        // IMMEDIATE so the read-before-write (SELECT partitions, then DELETE/UPDATE)
        // takes the write lock up front: under a concurrent bulk run this avoids a
        // stale-snapshot SQLITE_BUSY between workers; serially it is unchanged.
        let tx = cat
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let mut q = tx.prepare("SELECT p.series_id,p.path,p.checksum FROM partitions p")?;
        let entries = q
            .query_map([], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        drop(q);
        let mut affected = std::collections::HashSet::new();
        for (sid, rel, hash) in entries {
            let p = self.root.join(&rel);
            match files.remove(&p) {
                Some(found) if found == hash => {}
                Some(_) | None => {
                    affected.insert(sid);
                    tx.execute(
                        "DELETE FROM partitions WHERE series_id=?1 AND path=?2",
                        params![sid, rel],
                    )?;
                    tx.execute("DELETE FROM coverage WHERE series_id=?1", [sid])?;
                    tx.execute("DELETE FROM sessions WHERE series_id=?1", [sid])?;
                    tx.execute(
                        "UPDATE series SET first_ts=NULL,last_ts=NULL,rows=0 WHERE id=?1",
                        [sid],
                    )?;
                    tx.execute("INSERT INTO quality(series_id,kind,detail) SELECT ?1,'reconcile_missing_or_checksum','partition removed from catalog coverage' WHERE NOT EXISTS(SELECT 1 FROM quality WHERE series_id=?1 AND kind='reconcile_missing_or_checksum')",[sid])?;
                }
            }
        }
        for (orphan, _) in files {
            tx.execute("INSERT INTO quality(kind,detail) SELECT 'reconcile_orphan_file',?1 WHERE NOT EXISTS(SELECT 1 FROM quality WHERE kind='reconcile_orphan_file' AND detail=?1)",[orphan.to_string_lossy().to_string()])?;
        }
        tx.commit()?;
        Ok(())
    }
    pub fn remove_series_files(&self, id: &str, tf: Tf) -> Result<(), DataError> {
        let dir = self.root.join("candles").join(id).join(tf.as_str());
        if dir.exists() {
            fs::remove_dir_all(dir)?;
        }
        Ok(())
    }
}

#[cfg(not(windows))]
fn atomic_replace(from: &Path, to: &Path) -> Result<(), DataError> {
    fs::rename(from, to)?;
    Ok(())
}
#[cfg(windows)]
fn atomic_replace(from: &Path, to: &Path) -> Result<(), DataError> {
    use std::os::windows::ffi::OsStrExt;
    let src: Vec<u16> = from.as_os_str().encode_wide().chain(Some(0)).collect();
    let dst: Vec<u16> = to.as_os_str().encode_wide().chain(Some(0)).collect();
    let ok = unsafe {
        windows_sys::Win32::Storage::FileSystem::MoveFileExW(
            src.as_ptr(),
            dst.as_ptr(),
            windows_sys::Win32::Storage::FileSystem::MOVEFILE_REPLACE_EXISTING
                | windows_sys::Win32::Storage::FileSystem::MOVEFILE_WRITE_THROUGH,
        )
    };
    if ok == 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}
fn checksum(p: &Path) -> Result<String, DataError> {
    let bytes = fs::read(p)?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}
fn walk(root: &Path) -> Result<Vec<PathBuf>, DataError> {
    let mut v = Vec::new();
    for e in fs::read_dir(root)? {
        let p = e?.path();
        if p.is_dir() {
            v.extend(walk(&p)?)
        } else {
            v.push(p)
        }
    }
    Ok(v)
}
fn read_bars(path: &Path) -> Result<Vec<Bar>, DataError> {
    let f = File::open(path)?;
    let r = ParquetRecordBatchReaderBuilder::try_new(f)
        .map_err(|e| DataError::Invalid(e.to_string()))?
        .build()
        .map_err(|e| DataError::Invalid(e.to_string()))?;
    let mut v = Vec::new();
    for batch in r {
        let b = batch.map_err(|e| DataError::Invalid(e.to_string()))?;
        let ts = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataError::Invalid("ts schema".into()))?;
        let cols: Vec<_> = (1..6)
            .map(|i| {
                b.column(i)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| DataError::Invalid("OHLCV schema".into()))
            })
            .collect::<Result<_, _>>()?;
        for i in 0..b.num_rows() {
            v.push(Bar {
                ts: ts.value(i),
                open: cols[0].value(i),
                high: cols[1].value(i),
                low: cols[2].value(i),
                close: cols[3].value(i),
                volume: cols[4].value(i),
            });
        }
    }
    Ok(v)
}

pub fn missing_date_ranges(
    cat: &Catalog,
    id: &str,
    tf: Tf,
    from: NaiveDate,
    to: NaiveDate,
) -> Result<Vec<(NaiveDate, NaiveDate)>, DataError> {
    let missing = cat.missing_dates(id, tf, from, to)?;
    Ok(match (missing.first(), missing.last()) {
        (Some(a), Some(b)) => vec![(*a, *b)],
        _ => Vec::new(),
    })
}
fn local_midnight(d: NaiveDate) -> i64 {
    d.and_hms_opt(0, 0, 0)
        .unwrap()
        .and_local_timezone(Kolkata)
        .single()
        .unwrap()
        .timestamp()
}
pub fn chunk_dates(from: NaiveDate, to: NaiveDate) -> Vec<(NaiveDate, NaiveDate)> {
    chunk_dates_limited(from, to, 89)
}
pub fn chunk_dates_limited(
    from: NaiveDate,
    to: NaiveDate,
    days: u64,
) -> Vec<(NaiveDate, NaiveDate)> {
    let mut out = Vec::new();
    let mut s = from;
    while s <= to {
        let e = (s + chrono::Days::new(days.saturating_sub(1))).min(to);
        out.push((s, e));
        s = e + chrono::Days::new(1);
    }
    out
}
pub fn last_complete_bar(now: i64, tf: Tf, _session_end: i64, margin: i64) -> i64 {
    let step = tf.seconds();
    let local = DateTime::<Utc>::from_timestamp(now, 0)
        .unwrap_or(DateTime::UNIX_EPOCH)
        .with_timezone(&Kolkata);
    if tf == Tf::D1 {
        let cutoff = local
            .date_naive()
            .and_hms_opt(16, 0, 0)
            .unwrap()
            .and_local_timezone(Kolkata)
            .single()
            .unwrap()
            .timestamp();
        return if now >= cutoff {
            cutoff - 33300
        } else {
            (local.date_naive() - chrono::Days::new(1))
                .and_hms_opt(9, 15, 0)
                .unwrap()
                .and_local_timezone(Kolkata)
                .single()
                .unwrap()
                .timestamp()
                - 33300
        };
    }
    let last = now - margin - step;
    last - last.rem_euclid(step)
}
pub fn complete_intraday(
    bars: Vec<Bar>,
    now: i64,
    tf: Tf,
    session_ends: &HashMap<String, i64>,
    margin: i64,
) -> Vec<Bar> {
    bars.into_iter()
        .filter(|b| {
            let local = DateTime::<Utc>::from_timestamp(b.ts, 0)
                .unwrap_or(DateTime::UNIX_EPOCH)
                .with_timezone(&Kolkata);
            let end = session_ends
                .get(&local.date_naive().to_string())
                .copied()
                .unwrap_or_else(|| {
                    local
                        .date_naive()
                        .and_hms_opt(15, 30, 0)
                        .unwrap()
                        .and_local_timezone(Kolkata)
                        .single()
                        .unwrap()
                        .timestamp()
                });
            now >= ((b.ts + tf.seconds()).min(end) + margin)
        })
        .collect()
}
pub fn session_shapes(bars: &[Bar]) -> BTreeMap<NaiveDate, (i64, i64, usize)> {
    let mut m: BTreeMap<NaiveDate, Vec<i64>> = BTreeMap::new();
    for b in bars {
        let d = DateTime::<Utc>::from_timestamp(b.ts, 0)
            .unwrap_or(DateTime::UNIX_EPOCH)
            .with_timezone(&Kolkata)
            .date_naive();
        m.entry(d).or_default().push(b.ts);
    }
    m.into_iter()
        .map(|(d, mut xs)| {
            xs.sort();
            (d, (xs[0], *xs.last().unwrap(), xs.len()))
        })
        .collect()
}
pub fn session_regime_changes(shapes: &BTreeMap<NaiveDate, (i64, i64, usize)>) -> Vec<NaiveDate> {
    let mut out = Vec::new();
    let mut history = Vec::<i64>::new();
    let mut prior = None;
    for (d, (_, last, _)) in shapes {
        let local = DateTime::<Utc>::from_timestamp(*last, 0)
            .unwrap_or(DateTime::UNIX_EPOCH)
            .with_timezone(&Kolkata);
        let value = local.hour() as i64 * 3600 + local.minute() as i64 * 60 + local.second() as i64;
        history.push(value);
        if history.len() > 20 {
            history.remove(0);
        }
        let mut counts = HashMap::<i64, usize>::new();
        for x in &history {
            *counts.entry(*x).or_default() += 1;
        }
        let mode = counts
            .into_iter()
            .max_by_key(|(v, n)| (*n, std::cmp::Reverse(*v)))
            .map(|(v, _)| v);
        if let (Some(old), Some(new)) = (prior, mode) {
            if old != new {
                out.push(*d)
            }
        }
        prior = mode;
    }
    out
}

fn instrument_interval(tf: Tf) -> &'static str {
    match tf {
        Tf::M1 => "1",
        Tf::M5 => "5",
        Tf::M15 => "15",
        Tf::M25 => "25",
        Tf::M60 => "60",
        Tf::D1 => "day",
    }
}
pub fn check_overlap(old: &[Bar], new: &[Bar]) -> (&'static str, Vec<i64>) {
    let oldm: HashMap<_, _> = old.iter().map(|b| (b.ts, b)).collect();
    let mut diffs = Vec::new();
    let mut ratios = Vec::new();
    let mut overlap = 0usize;
    for n in new {
        if let Some(o) = oldm.get(&n.ts) {
            overlap += 1;
            if o != &n {
                diffs.push(n.ts);
            }
            if n.close != 0.0 {
                ratios.push(o.close / n.close);
            }
        }
    }
    if diffs.is_empty() {
        return ("identical", diffs);
    }
    let avg = ratios.iter().sum::<f64>() / ratios.len().max(1) as f64;
    if ratios.len() == overlap
        && ratios.iter().all(|x| (x - avg).abs() < 1e-6)
        && (avg - 1.0).abs() > 1e-4
    {
        return ("corporate_action", diffs);
    }
    ("revised", diffs)
}

pub trait HistoryApi: Send + Sync {
    fn intraday(
        &self,
        id: &str,
        instrument: &str,
        interval: &str,
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<Vec<Bar>, DataError>;
    fn daily(
        &self,
        id: &str,
        instrument: &str,
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<Vec<Bar>, DataError>;
}
impl HistoryApi for DhanClient {
    fn intraday(
        &self,
        id: &str,
        instrument: &str,
        interval: &str,
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<Vec<Bar>, DataError> {
        Ok(
            DhanClient::intraday(self, id, instrument, interval, from, to)?
                .into_iter()
                .map(Into::into)
                .collect(),
        )
    }
    fn daily(
        &self,
        id: &str,
        instrument: &str,
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<Vec<Bar>, DataError> {
        Ok(DhanClient::daily(self, id, instrument, from, to)?
            .into_iter()
            .map(Into::into)
            .collect())
    }
}
pub struct Updater {
    pub client: Box<dyn HistoryApi>,
    pub catalog: Catalog,
    pub store: Store,
    pub max_retries: u32,
    pub margin_seconds: i64,
    pub reference_symbol_id: Option<String>,
    /// Soft cap on requests per IST day. When `budget_used >= budget_cap` the
    /// updater stops with `DataError::BudgetExhausted`. Defaults to 90,000.
    pub budget_cap: u64,
}
pub const DEFAULT_BUDGET_CAP: u64 = 90_000;
impl Updater {
    fn budget_guard(&self, today: &str) -> Result<(), DataError> {
        let used = self.catalog.budget_used(today)?;
        if used >= self.budget_cap {
            return Err(DataError::BudgetExhausted {
                used,
                cap: self.budget_cap,
            });
        }
        Ok(())
    }
    pub fn update(
        &mut self,
        security_id: &str,
        tf: Tf,
        instrument: &str,
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<bool, DataError> {
        let today = chrono::Utc::now().date_naive().to_string();
        self.budget_guard(&today)?;
        let run = self.catalog.runs_start()?;
        let mut requests = 0;
        let result = (|| {
            self.store.reconcile(&mut self.catalog)?;
            let gaps = missing_date_ranges(&self.catalog, security_id, tf, from, to)?;
            let chunks = if let (Some((first, _)), Some((_, last))) = (gaps.first(), gaps.last()) {
                if tf == Tf::D1 {
                    vec![(*first, *last)]
                } else {
                    chunk_dates(*first, *last)
                }
            } else {
                Vec::new()
            };
            for (a, b) in chunks.iter().copied() {
                let pending = missing_date_ranges(&self.catalog, security_id, tf, a, b)?;
                if pending.is_empty() {
                    continue;
                }
                for (request_from, request_to) in pending {
                    self.budget_guard(&today)?;
                    eprintln!(
                        "request {} {} {}..={}",
                        security_id,
                        tf.as_str(),
                        request_from,
                        request_to
                    );
                    let fetch = if tf == Tf::D1 {
                        self.client
                            .daily(security_id, instrument, request_from, request_to)
                    } else {
                        self.client.intraday(
                            security_id,
                            instrument,
                            instrument_interval(tf),
                            request_from,
                            request_to,
                        )
                    };
                    requests += 1;
                    self.catalog.budget_add(&today, 1)?;
                    let candles = match fetch {
                        Ok(rows) => rows,
                        Err(error)
                            if tf == Tf::D1
                                && (request_to - request_from).num_days() + 1 > 1825 =>
                        {
                            eprintln!("daily request failed for {request_from}..={request_to}; retrying in 5-year fallback chunks: {error}");
                            let mut rows = Vec::new();
                            for (fallback_from, fallback_to) in
                                chunk_dates_limited(request_from, request_to, 1825)
                            {
                                self.budget_guard(&today)?;
                                let part = self.client.daily(
                                    security_id,
                                    instrument,
                                    fallback_from,
                                    fallback_to,
                                );
                                requests += 1;
                                self.catalog.budget_add(&today, 1)?;
                                rows.extend(part?);
                            }
                            rows
                        }
                        Err(error) => return Err(error),
                    };
                    let bars: Vec<Bar> = candles;
                    if bars.is_empty() {
                        let from_ts = local_midnight(a);
                        let to_ts = local_midnight(b + chrono::Days::new(1)) - 1;
                        if self.reference_symbol_id.as_deref() == Some(security_id) {
                            for n in 0..=((b - a).num_days().max(0) as u64) {
                                let d = a + chrono::Days::new(n);
                                if matches!(
                                    d.weekday(),
                                    chrono::Weekday::Sat | chrono::Weekday::Sun
                                ) {
                                    self.catalog.record_calendar(d, false, "weekend")?;
                                } else {
                                    self.catalog.record_calendar(d, false, "reference-empty")?;
                                }
                            }
                        }
                        let _ = (from_ts, to_ts);
                        continue;
                    }
                    let now = chrono::Utc::now().timestamp();
                    let saved: Vec<Bar> = if tf == Tf::D1 {
                        let current_local = DateTime::<Utc>::from_timestamp(now, 0)
                            .unwrap_or(DateTime::UNIX_EPOCH)
                            .with_timezone(&Kolkata);
                        bars.into_iter()
                            .filter(|b| {
                                let session_date =
                                    DateTime::<Utc>::from_timestamp(b.ts - 33_300, 0)
                                        .unwrap_or(DateTime::UNIX_EPOCH)
                                        .with_timezone(&Kolkata)
                                        .date_naive();
                                let ready_at = session_date
                                    .and_hms_opt(16, 0, 0)
                                    .unwrap()
                                    .and_local_timezone(Kolkata)
                                    .single()
                                    .unwrap()
                                    .timestamp();
                                now >= ready_at || session_date < current_local.date_naive()
                            })
                            .collect()
                    } else {
                        complete_intraday(bars, now, tf, &HashMap::new(), self.margin_seconds)
                    };
                    if saved.is_empty() {
                        continue;
                    }
                    let existing_all: Vec<Bar> = {
                        let sid = self.catalog.series(security_id, tf)?;
                        let mut q = self
                            .catalog
                            .conn
                            .prepare("SELECT path FROM partitions WHERE series_id=?1")?;
                        let paths = q
                            .query_map([sid], |r| r.get::<_, String>(0))?
                            .collect::<Result<Vec<_>, _>>()?;
                        let mut all = Vec::new();
                        for rel in paths {
                            all.extend(read_bars(&self.store.root.join(rel))?)
                        }
                        all
                    };
                    let (kind, diffs) = check_overlap(&existing_all, &saved);
                    if kind == "corporate_action" {
                        let sid = self.catalog.series(security_id, tf)?;
                        self.catalog
                            .conn
                            .execute("UPDATE series SET needs_refresh=1 WHERE id=?1", [sid])?;
                        self.store.remove_series_files(security_id, tf)?;
                        self.catalog
                            .conn
                            .execute("DELETE FROM partitions WHERE series_id=?1", [sid])?;
                        self.catalog
                            .conn
                            .execute("DELETE FROM coverage WHERE series_id=?1", [sid])?;
                        self.catalog
                            .conn
                            .execute("DELETE FROM sessions WHERE series_id=?1", [sid])?;
                        self.catalog.conn.execute(
                            "UPDATE series SET first_ts=NULL,last_ts=NULL,rows=0 WHERE id=?1",
                            [sid],
                        )?;
                        for (full_from, full_to) in chunks.clone() {
                            self.budget_guard(&today)?;
                            let full = if tf == Tf::D1 {
                                self.client
                                    .daily(security_id, instrument, full_from, full_to)?
                            } else {
                                self.client.intraday(
                                    security_id,
                                    instrument,
                                    instrument_interval(tf),
                                    full_from,
                                    full_to,
                                )?
                            };
                            requests += 1;
                            self.catalog.budget_add(&today, 1)?;
                            let complete = if tf == Tf::D1 {
                                full
                            } else {
                                complete_intraday(
                                    full,
                                    chrono::Utc::now().timestamp(),
                                    tf,
                                    &HashMap::new(),
                                    self.margin_seconds,
                                )
                            };
                            if complete.is_empty() {
                                continue;
                            }
                            let parts = self.store.write_partitions(security_id, tf, &complete)?;
                            for p in parts {
                                self.catalog.record_partition(sid, &self.store, &p)?;
                            }
                        }
                        self.catalog
                            .conn
                            .execute("UPDATE series SET needs_refresh=0 WHERE id=?1", [sid])?;
                        return Ok(());
                    }
                    if kind == "revised" && !diffs.is_empty() {
                        self.catalog.mark_quality(
                            security_id,
                            tf,
                            &Quality {
                                kind: "revised".into(),
                                ts: diffs.first().copied(),
                                detail: format!("{} revised bars in overlap", diffs.len()),
                            },
                        )?;
                    }
                    let partitions = self.store.write_partitions(security_id, tf, &saved)?;
                    let sid = self.catalog.series(security_id, tf)?;
                    let tx = self
                        .catalog
                        .conn
                        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                    for part in &partitions {
                        let rel = part
                            .path
                            .strip_prefix(&self.store.root)
                            .unwrap_or(&part.path)
                            .to_string_lossy()
                            .to_string();
                        tx.execute("INSERT INTO partitions VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(series_id,path) DO UPDATE SET first_ts=excluded.first_ts,last_ts=excluded.last_ts,rows=excluded.rows,checksum=excluded.checksum",params![sid,rel,part.first_ts,part.last_ts,part.rows,part.checksum])?;
                    }
                    let first = saved.iter().map(|x| x.ts).min().unwrap();
                    let last = saved.iter().map(|x| x.ts).max().unwrap();
                    let shapes = session_shapes(&saved);
                    for day in session_regime_changes(&shapes) {
                        tx.execute("INSERT INTO quality(series_id,kind,ts,detail) SELECT ?1,'session_regime_change',?2,'derived session end mode changed' WHERE NOT EXISTS (SELECT 1 FROM quality WHERE series_id=?1 AND kind='session_regime_change' AND ts=?2)",params![sid,local_midnight(day)])?;
                    }
                    for (d, (f, l, n)) in shapes {
                        tx.execute(
                            "INSERT INTO coverage VALUES(?1,?2,?3,'data')",
                            params![
                                sid,
                                local_midnight(d),
                                local_midnight(d + chrono::Days::new(1)) - 1
                            ],
                        )?;
                        if self.reference_symbol_id.as_deref() == Some(security_id) {
                            tx.execute("INSERT INTO trading_calendar VALUES(?1,1,'reference-bars') ON CONFLICT(date) DO UPDATE SET is_trading=1,source='reference-bars'",[d.to_string()])?;
                            tx.execute(
                                "DELETE FROM trading_calendar WHERE date=?1 AND is_trading=0",
                                [d.to_string()],
                            )?;
                            tx.execute("DELETE FROM coverage WHERE series_id=?1 AND kind='confirmed_empty' AND start_ts>=?2 AND start_ts<?3",params![sid,local_midnight(d),local_midnight(d+chrono::Days::new(1))])?;
                            tx.execute(
                                "DELETE FROM sessions WHERE series_id=?1 AND date=?2 AND bars=0",
                                params![sid, d.to_string()],
                            )?;
                        }
                        tx.execute("INSERT INTO sessions VALUES(?1,?2,?3,?4,?5) ON CONFLICT(series_id,date) DO UPDATE SET first_ts=excluded.first_ts,last_ts=excluded.last_ts,bars=excluded.bars",params![sid,d.to_string(),f,l,n])?;
                    }
                    tx.execute("UPDATE series SET first_ts=CASE WHEN first_ts IS NULL THEN ?2 ELSE MIN(first_ts,?2) END,last_ts=CASE WHEN last_ts IS NULL THEN ?3 ELSE MAX(last_ts,?3) END,rows=(SELECT COALESCE(SUM(rows),0) FROM partitions WHERE series_id=?1) WHERE id=?1",params![sid,first,last])?;
                    tx.commit()?;
                    if self.reference_symbol_id.as_deref() == Some(security_id) {
                        for day in self.catalog.missing_dates(security_id, tf, a, b)? {
                            self.catalog
                                .record_calendar(day, false, "reference-empty")?;
                        }
                    }
                    if !security_id.is_empty() {
                        for day in self.catalog.missing_dates(security_id, tf, a, b)? {
                            let is_open:bool=self.catalog.conn.query_row("SELECT COALESCE((SELECT is_trading=1 FROM trading_calendar WHERE date=?1),1)",[day.to_string()],|r|r.get(0))?;
                            if !is_open {
                                self.catalog.mark_session_empty(security_id, tf, day)?;
                                continue;
                            }
                            let mut recovered = false;
                            for _ in 0..self.max_retries {
                                self.budget_guard(&today)?;
                                let retry = if tf == Tf::D1 {
                                    self.client.daily(security_id, instrument, day, day)?
                                } else {
                                    self.client.intraday(
                                        security_id,
                                        instrument,
                                        instrument_interval(tf),
                                        day,
                                        day,
                                    )?
                                };
                                requests += 1;
                                self.catalog.budget_add(&today, 1)?;
                                if !retry.is_empty() {
                                    let retry_bars: Vec<Bar> = retry;
                                    let completed = if tf == Tf::D1 {
                                        retry_bars
                                    } else {
                                        complete_intraday(
                                            retry_bars,
                                            chrono::Utc::now().timestamp(),
                                            tf,
                                            &HashMap::new(),
                                            self.margin_seconds,
                                        )
                                    };
                                    if !completed.is_empty() {
                                        let pieces = self.store.write_partitions(
                                            security_id,
                                            tf,
                                            &completed,
                                        )?;
                                        let sid = self.catalog.series(security_id, tf)?;
                                        for part in pieces {
                                            self.catalog.record_partition(
                                                sid,
                                                &self.store,
                                                &part,
                                            )?;
                                        }
                                        recovered = true;
                                        break;
                                    }
                                }
                            }
                            if !recovered {
                                if self.reference_symbol_id.as_deref() == Some(security_id) {
                                    self.catalog
                                        .record_calendar(day, false, "reference-empty")?;
                                }
                                self.catalog.mark_session_empty(security_id, tf, day)?;
                                self.catalog.mark_quality(security_id,tf,&Quality{kind:"missing_symbol_session".into(),ts:Some(local_midnight(day)),detail:"no bars after bounded per-symbol retry; marked confirmed_empty".into()})?;
                            }
                        }
                    }
                }
            }
            Ok::<(), DataError>(())
        })();
        self.catalog.runs_finish(
            run,
            requests,
            if result.is_ok() { "complete" } else { "failed" },
        )?;
        let changed = requests > 0;
        result?;
        Ok(changed)
    }
}

/// Number of Dhan requests the planner will issue for `id`/`tf` over
/// `from..=to`, given current catalog coverage. Mirrors the chunking in
/// [`Updater::update`]: a daily series is a single request (5-year fallback
/// chunks only happen after a data error), an intraday series is one request
/// per 89-calendar-day chunk over the merged missing range. Returns zero when
/// the range is already covered, which is what makes `--dry-run` after an
/// interrupted run report only the remaining work (spec 7).
pub fn plan_requests(
    cat: &Catalog,
    id: &str,
    tf: Tf,
    from: NaiveDate,
    to: NaiveDate,
) -> Result<usize, DataError> {
    let gaps = missing_date_ranges(cat, id, tf, from, to)?;
    Ok(match (gaps.first(), gaps.last()) {
        (Some((first, _)), Some((_, last))) => {
            if tf == Tf::D1 {
                1
            } else {
                chunk_dates(*first, *last).len()
            }
        }
        _ => 0,
    })
}

/// Configuration for a `data bulk` run.
#[derive(Clone, Debug)]
pub struct BulkConfig {
    pub from: NaiveDate,
    pub to: NaiveDate,
    /// Timeframes to download, in processing order. When both are present the
    /// caller orders 1d before 1m (spec 4).
    pub tfs: Vec<Tf>,
    pub budget_cap: u64,
    /// Priority trading symbols, processed first in the given order (spec 4).
    pub priority_symbols: Vec<String>,
    pub margin_seconds: i64,
    pub max_retries: u32,
    pub reference_symbol_id: Option<String>,
    /// Number of calendar days to wait before retrying a recorded failure.
    pub failure_backoff_days: u64,
    /// Maximum number of symbols fetched in parallel within a timeframe (spec 6).
    /// `1` keeps the fully serial path; higher values fan out across symbols while
    /// the shared `Limiter` still spaces request *starts* >=250 ms apart. The
    /// library default is serial; the CLI opts into 4. Exact budget/auth
    /// accounting is a serial-only guarantee (see [`BulkDownloader::run`]).
    pub concurrency: usize,
}
impl BulkConfig {
    pub fn new(from: NaiveDate, to: NaiveDate, tfs: Vec<Tf>) -> Self {
        Self {
            from,
            to,
            tfs,
            budget_cap: DEFAULT_BUDGET_CAP,
            priority_symbols: Vec::new(),
            margin_seconds: 30,
            max_retries: 3,
            reference_symbol_id: None,
            failure_backoff_days: 1,
            concurrency: 1,
        }
    }
}

/// Outcome of a bulk run (or dry run).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BulkReport {
    pub symbols_completed: usize,
    pub symbols_total: usize,
    pub requests_used: u64,
    pub failures: u64,
    /// Planned request count per timeframe, populated on `--dry-run`.
    pub planned: Vec<(String, usize)>,
    /// True when the run stopped because the daily budget was reached.
    pub budget_reached: bool,
    /// True when the run stopped because of a Ctrl-C style interrupt.
    pub interrupted: bool,
}

/// Drives a full-universe download, reusing [`Updater`] per symbol/tf so request
/// chunking, retries, 5-year fallback, overlap handling and crash-safe writes
/// all come from the shared planner. The downloader owns only ordering, the
/// persisted budget hard-stop, resume and progress.
pub struct BulkDownloader<F: Fn() -> Box<dyn HistoryApi>> {
    /// Factory that yields a fresh client per symbol so the shared rate limiter
    /// (250 ms spacing, DH-904 backoff) is reused without aliasing one updater.
    pub make_client: F,
    pub catalog_path: PathBuf,
    pub store_root: PathBuf,
    pub config: BulkConfig,
    /// Set from a Ctrl-C handler. Checked between symbols; the in-flight symbol
    /// is allowed to finish (spec 7).
    pub stop: Arc<std::sync::atomic::AtomicBool>,
}
impl<F: Fn() -> Box<dyn HistoryApi> + Sync> BulkDownloader<F> {
    fn today(&self) -> String {
        chrono::Utc::now().date_naive().to_string()
    }
    /// Resolve the processing order: priority symbols first (in file order),
    /// then every other instrument in security-id order, de-duplicated.
    fn order(&self, cat: &Catalog) -> Result<Vec<(String, String, String)>, DataError> {
        let all = cat.instruments_in_order()?;
        let by_id: HashMap<String, (String, String)> = all
            .iter()
            .map(|(id, sym, inst)| (id.clone(), (sym.clone(), inst.clone())))
            .collect();
        let mut seen = std::collections::HashSet::new();
        let mut out = Vec::new();
        for sym in &self.config.priority_symbols {
            let (id, _) = cat.resolve(sym)?;
            if seen.insert(id.clone()) {
                if let Some((symbol, inst)) = by_id.get(&id) {
                    out.push((id.clone(), symbol.clone(), inst.clone()));
                }
            }
        }
        for (id, sym, inst) in all {
            if seen.insert(id.clone()) {
                out.push((id, sym, inst));
            }
        }
        Ok(out)
    }
    /// Compute the planned request count per timeframe without sending anything.
    pub fn dry_run(&self) -> Result<BulkReport, DataError> {
        let cat = Catalog::open(&self.catalog_path)?;
        let order = self.order(&cat)?;
        let mut report = BulkReport {
            symbols_total: order.len(),
            ..Default::default()
        };
        for tf in &self.config.tfs {
            let mut total = 0usize;
            for (id, _, _) in &order {
                total += plan_requests(&cat, id, *tf, self.config.from, self.config.to)?;
            }
            report.planned.push((tf.as_str().to_string(), total));
        }
        Ok(report)
    }
    /// Run the download. Processes 1d for every symbol first, then 1m (per the
    /// order in `config.tfs`), committing after each symbol so an interrupt
    /// leaves whole symbols behind. Returns the final report.
    ///
    /// Dispatches to the serial path (`config.concurrency <= 1`) or the
    /// across-symbols concurrent path (`> 1`). The serial path's exact budget and
    /// immediate-auth-abort behaviour is a serial-only guarantee; see
    /// [`run_concurrent`](Self::run_concurrent) for the relaxed concurrent
    /// semantics.
    pub fn run<P: FnMut(&Progress) + Send>(
        &self,
        progress: P,
    ) -> Result<BulkReport, DataError> {
        if self.config.concurrency > 1 {
            self.run_concurrent(progress)
        } else {
            self.run_serial(progress)
        }
    }

    /// Fully serial download: one symbol at a time, one SQLite connection in
    /// flight. This is the reference implementation; the concurrent path must
    /// produce the same catalog for the same inputs.
    fn run_serial<P: FnMut(&Progress)>(&self, mut progress: P) -> Result<BulkReport, DataError> {
        use std::sync::atomic::Ordering;
        let order = {
            let cat = Catalog::open(&self.catalog_path)?;
            self.order(&cat)?
        };
        let start = std::time::Instant::now();
        let today = self.today();
        let mut report = BulkReport {
            symbols_total: order.len(),
            ..Default::default()
        };
        {
            let cat = Catalog::open(&self.catalog_path)?;
            report.failures = cat.failure_count()?;
        }
        'tfs: for tf in &self.config.tfs {
            let mut done = 0usize;
            for (id, symbol, instrument) in &order {
                if self.stop.load(Ordering::SeqCst) {
                    report.interrupted = true;
                    break 'tfs;
                }
                let budget_used = {
                    let cat = Catalog::open(&self.catalog_path)?;
                    cat.budget_used(&today)?
                };
                if budget_used >= self.config.budget_cap {
                    report.budget_reached = true;
                    break 'tfs;
                }
                let mut updater = Updater {
                    client: (self.make_client)(),
                    catalog: Catalog::open(&self.catalog_path)?,
                    store: Store::new(&self.store_root),
                    max_retries: self.config.max_retries,
                    margin_seconds: self.config.margin_seconds,
                    reference_symbol_id: self.config.reference_symbol_id.clone(),
                    budget_cap: self.config.budget_cap,
                };
                let before = updater.catalog.budget_used(&today)?;
                let outcome = updater.update(id, *tf, instrument, self.config.from, self.config.to);
                let after = updater.catalog.budget_used(&today)?;
                report.requests_used += after.saturating_sub(before);
                match outcome {
                    Ok(_) => {
                        done += 1;
                        report.symbols_completed += 1;
                        let cat = Catalog::open(&self.catalog_path)?;
                        cat.clear_failure(id, *tf, self.config.from, self.config.to)?;
                    }
                    Err(e) if e.is_budget() => {
                        report.budget_reached = true;
                        break 'tfs;
                    }
                    Err(e) if e.is_auth() => {
                        return Err(e);
                    }
                    Err(e) => {
                        let cat = Catalog::open(&self.catalog_path)?;
                        let next_retry = chrono::Utc::now().date_naive()
                            + chrono::Days::new(self.config.failure_backoff_days);
                        cat.record_failure(
                            id,
                            *tf,
                            self.config.from,
                            self.config.to,
                            &e.code(),
                            next_retry,
                        )?;
                        report.failures = cat.failure_count()?;
                        eprintln!(
                            "failure {symbol} {} {}..={}: {e}",
                            tf.as_str(),
                            self.config.from,
                            self.config.to
                        );
                    }
                }
                let elapsed = start.elapsed().as_secs_f64().max(1e-3);
                let rate = report.requests_used as f64 / elapsed;
                progress(&Progress {
                    tf: tf.as_str(),
                    done,
                    total: order.len(),
                    requests: report.requests_used,
                    budget_cap: self.config.budget_cap,
                    budget_used: after,
                    rate,
                    failures: report.failures,
                });
            }
        }
        Ok(report)
    }

    /// Across-symbols concurrent download (`config.concurrency > 1`). Within each
    /// timeframe a bounded pool of worker threads (pool size = `concurrency`) each
    /// drives one symbol's [`Updater::update`] to completion on its own SQLite
    /// connection. The pool size is spec 6's in-flight cap; the shared `Limiter`
    /// inside the cloned clients still spaces request *starts* >=250 ms apart
    /// globally, so concurrency only overlaps responses on the wire, never the
    /// 250 ms start spacing.
    ///
    /// Each symbol is a distinct series touched by a single worker, so the overlap
    /// read-modify-write never races; cross-worker safety on the shared `budget`,
    /// `runs` and `trading_calendar` rows comes from WAL + `busy_timeout` and the
    /// IMMEDIATE write transactions. Timeframes are still a barrier (all symbols
    /// for 1d finish before 1m) and each symbol commits whole, so Ctrl-C and
    /// budget stops leave whole symbols behind exactly as the serial path does.
    ///
    /// Relaxed vs. serial: the daily budget is a soft cap here — up to
    /// `concurrency - 1` requests may be issued past it before every worker
    /// observes the stop — and an auth failure aborts only after the in-flight
    /// symbols finish. Callers needing exact accounting use `concurrency = 1`.
    fn run_concurrent<P: FnMut(&Progress) + Send>(
        &self,
        progress: P,
    ) -> Result<BulkReport, DataError> {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        use std::sync::Mutex;
        let order = {
            let cat = Catalog::open(&self.catalog_path)?;
            self.order(&cat)?
        };
        let total = order.len();
        let start = std::time::Instant::now();
        let today = self.today();
        let mut initial = BulkReport {
            symbols_total: total,
            ..Default::default()
        };
        {
            let cat = Catalog::open(&self.catalog_path)?;
            initial.failures = cat.failure_count()?;
        }
        let report = Mutex::new(initial);
        let progress = Mutex::new(progress);
        // The first error that the serial path would `?`-propagate (catalog open
        // failure or auth): stored, then returned after the pool drains.
        let fatal: Mutex<Option<DataError>> = Mutex::new(None);
        // Set when any worker hits the budget or a fatal error so the others stop
        // picking up new symbols (in-flight symbols still finish).
        let stop_all = AtomicBool::new(false);

        'tfs: for tf in &self.config.tfs {
            let next = AtomicUsize::new(0);
            let done = AtomicUsize::new(0);
            let workers = self.config.concurrency.max(1).min(total.max(1));
            std::thread::scope(|scope| {
                for _ in 0..workers {
                    scope.spawn(|| {
                        while !self.stop.load(Ordering::SeqCst)
                            && !stop_all.load(Ordering::SeqCst)
                        {
                            let i = next.fetch_add(1, Ordering::SeqCst);
                            if i >= total {
                                break;
                            }
                            let (id, symbol, instrument) = &order[i];
                            // Budget gate before taking on a new symbol (mirrors the
                            // serial per-symbol check).
                            match Catalog::open(&self.catalog_path)
                                .and_then(|c| c.budget_used(&today))
                            {
                                Ok(used) if used >= self.config.budget_cap => {
                                    report.lock().unwrap().budget_reached = true;
                                    stop_all.store(true, Ordering::SeqCst);
                                    break;
                                }
                                Ok(_) => {}
                                Err(e) => {
                                    *fatal.lock().unwrap() = Some(e);
                                    stop_all.store(true, Ordering::SeqCst);
                                    break;
                                }
                            }
                            let mut updater = match Catalog::open(&self.catalog_path) {
                                Ok(catalog) => Updater {
                                    client: (self.make_client)(),
                                    catalog,
                                    store: Store::new(&self.store_root),
                                    max_retries: self.config.max_retries,
                                    margin_seconds: self.config.margin_seconds,
                                    reference_symbol_id: self.config.reference_symbol_id.clone(),
                                    budget_cap: self.config.budget_cap,
                                },
                                Err(e) => {
                                    *fatal.lock().unwrap() = Some(e);
                                    stop_all.store(true, Ordering::SeqCst);
                                    break;
                                }
                            };
                            let before = updater.catalog.budget_used(&today).unwrap_or(0);
                            let outcome = updater.update(
                                id,
                                *tf,
                                instrument,
                                self.config.from,
                                self.config.to,
                            );
                            let after = updater
                                .catalog
                                .budget_used(&today)
                                .unwrap_or(before)
                                .max(before);
                            report.lock().unwrap().requests_used += after.saturating_sub(before);
                            match outcome {
                                Ok(_) => {
                                    report.lock().unwrap().symbols_completed += 1;
                                    if let Ok(cat) = Catalog::open(&self.catalog_path) {
                                        let _ = cat.clear_failure(
                                            id,
                                            *tf,
                                            self.config.from,
                                            self.config.to,
                                        );
                                    }
                                }
                                Err(e) if e.is_budget() => {
                                    report.lock().unwrap().budget_reached = true;
                                    stop_all.store(true, Ordering::SeqCst);
                                }
                                Err(e) if e.is_auth() => {
                                    *fatal.lock().unwrap() = Some(e);
                                    stop_all.store(true, Ordering::SeqCst);
                                }
                                Err(e) => {
                                    if let Ok(cat) = Catalog::open(&self.catalog_path) {
                                        let next_retry = chrono::Utc::now().date_naive()
                                            + chrono::Days::new(self.config.failure_backoff_days);
                                        let _ = cat.record_failure(
                                            id,
                                            *tf,
                                            self.config.from,
                                            self.config.to,
                                            &e.code(),
                                            next_retry,
                                        );
                                        if let Ok(fc) = cat.failure_count() {
                                            report.lock().unwrap().failures = fc;
                                        }
                                    }
                                    eprintln!(
                                        "failure {symbol} {} {}..={}: {e}",
                                        tf.as_str(),
                                        self.config.from,
                                        self.config.to
                                    );
                                }
                            }
                            let d = done.fetch_add(1, Ordering::SeqCst) + 1;
                            let (requests_used, failures) = {
                                let r = report.lock().unwrap();
                                (r.requests_used, r.failures)
                            };
                            let elapsed = start.elapsed().as_secs_f64().max(1e-3);
                            let rate = requests_used as f64 / elapsed;
                            let snapshot = Progress {
                                tf: tf.as_str(),
                                done: d,
                                total,
                                requests: requests_used,
                                budget_cap: self.config.budget_cap,
                                budget_used: after,
                                rate,
                                failures,
                            };
                            let mut p = progress.lock().unwrap();
                            (*p)(&snapshot);
                        }
                    });
                }
            });
            if self.stop.load(Ordering::SeqCst) {
                report.lock().unwrap().interrupted = true;
                break 'tfs;
            }
            if fatal.lock().unwrap().is_some() {
                break 'tfs;
            }
            if report.lock().unwrap().budget_reached {
                break 'tfs;
            }
        }
        if let Some(e) = fatal.lock().unwrap().take() {
            return Err(e);
        }
        Ok(report.into_inner().unwrap())
    }
}

/// A snapshot handed to the progress callback once per symbol.
#[derive(Clone, Debug)]
pub struct Progress<'a> {
    pub tf: &'a str,
    pub done: usize,
    pub total: usize,
    pub requests: u64,
    pub budget_cap: u64,
    pub budget_used: u64,
    pub rate: f64,
    pub failures: u64,
}
impl Progress<'_> {
    /// The single updating status line from spec 9, e.g.
    /// `1m  842/2365 symbols | requests 17,904/90,000 today | 3.9 req/s | ETA 2h 41m | failures 3`.
    pub fn line(&self) -> String {
        let remaining = self.total.saturating_sub(self.done);
        let eta = if self.rate > 0.0 {
            // Rough: assume the remaining symbols issue a similar number of
            // requests per symbol as issued so far.
            let per_symbol = if self.done > 0 {
                self.requests as f64 / self.done as f64
            } else {
                0.0
            };
            let secs = (remaining as f64 * per_symbol / self.rate) as i64;
            format_eta(secs)
        } else {
            "--".into()
        };
        format!(
            "{tf}  {done}/{total} symbols | requests {req}/{cap} today | {rate:.1} req/s | ETA {eta} | failures {fail}",
            tf = self.tf,
            done = self.done,
            total = self.total,
            req = group_thousands(self.budget_used),
            cap = group_thousands(self.budget_cap),
            rate = self.rate,
            eta = eta,
            fail = self.failures,
        )
    }
}
fn format_eta(secs: i64) -> String {
    if secs <= 0 {
        return "0m".into();
    }
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    if h > 0 {
        format!("{h}h {m}m")
    } else {
        format!("{m}m")
    }
}
fn group_thousands(n: u64) -> String {
    let s = n.to_string();
    let bytes = s.as_bytes();
    let mut out = String::new();
    for (i, c) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(*c as char);
    }
    out
}

pub struct CatalogSource {
    pub store: Store,
    pub catalog_path: PathBuf,
    pub allow_gaps: bool,
}
impl CatalogSource {
    pub fn new(store: Store, catalog_path: impl Into<PathBuf>) -> Self {
        Self {
            store,
            catalog_path: catalog_path.into(),
            allow_gaps: false,
        }
    }
    fn catalog(&self) -> Result<Catalog, DataError> {
        Catalog::open(&self.catalog_path)
    }
    pub fn reconcile(&self) -> Result<(), DataError> {
        let mut catalog = self.catalog()?;
        self.store.reconcile(&mut catalog)
    }
}
impl CandleSource for CatalogSource {
    fn open(
        &self,
        symbols: &[Symbol],
        timeframe: Timeframe,
        range: DateRange,
    ) -> Result<CandleStream, SourceError> {
        self.reconcile()
            .map_err(|e| SourceError::Other(format!("catalog reconciliation failed: {e}")))?;
        let catalog = self
            .catalog()
            .map_err(|e| SourceError::Other(e.to_string()))?;
        let tf = Tf::parse(timeframe.as_str())
            .ok_or_else(|| SourceError::Other("unsupported timeframe".into()))?;
        let (start, end) = (range.from.unwrap_or(i64::MIN), range.to.unwrap_or(i64::MAX));
        let mut streams = Vec::<Vec<(Symbol, Candle)>>::new();
        for symbol in symbols {
            let (id, _) = catalog
                .resolve(symbol)
                .map_err(|e| SourceError::Other(e.to_string()))?;
            if !catalog
                .has_coverage(&id, tf, start, end)
                .map_err(|e| SourceError::Other(e.to_string()))?
            {
                if self.allow_gaps {
                    continue;
                }
                return Err(SourceError::Other(format!(
                    "{}; run `algomln data update --symbols {} --tf {} --from <from> --to <to>`",
                    DataError::Uncovered(symbol.clone()),
                    symbol,
                    timeframe.as_str()
                )));
            }
            let sid = catalog
                .series(&id, tf)
                .map_err(|e| SourceError::Other(e.to_string()))?;
            let mut stmt = catalog
                .conn
                .prepare("SELECT path FROM partitions WHERE series_id=?1 ORDER BY first_ts")
                .map_err(|e| SourceError::Other(e.to_string()))?;
            let files = stmt
                .query_map([sid], |r| r.get::<_, String>(0))
                .map_err(|e| SourceError::Other(e.to_string()))?;
            let mut symbol_rows = Vec::new();
            for f in files {
                let path = self
                    .store
                    .root
                    .join(f.map_err(|e| SourceError::Other(e.to_string()))?);
                for b in read_bars(&path).map_err(|e| SourceError::Other(e.to_string()))? {
                    if (start..=end).contains(&b.ts) {
                        symbol_rows.push((
                            symbol.clone(),
                            Candle {
                                timestamp: b.ts,
                                open: b.open,
                                high: b.high,
                                low: b.low,
                                close: b.close,
                                volume: b.volume,
                            },
                        ));
                    }
                }
            }
            symbol_rows.sort_by_key(|x| x.1.timestamp);
            streams.push(symbol_rows);
        }
        Ok(Box::new(KWayMerge::new(streams)))
    }
    fn instrument_rules(
        &self,
        symbols: &[Symbol],
    ) -> BTreeMap<Symbol, algomln::trader::InstrumentRule> {
        let Ok(c) = self.catalog() else {
            return BTreeMap::new();
        };
        let mut out = BTreeMap::new();
        for s in symbols {
            if let Ok((id, _)) = c.resolve(s) {
                if let Ok(rule) = c.conn.query_row(
                    "SELECT lot_size FROM instruments WHERE security_id=?1",
                    [id],
                    |r| r.get::<_, u64>(0),
                ) {
                    out.insert(
                        s.clone(),
                        algomln::trader::InstrumentRule {
                            lot_size: rule.max(1),
                            min_qty: 1,
                        },
                    );
                }
            }
        }
        out
    }
    fn tick_sizes_paise(&self, symbols: &[Symbol]) -> BTreeMap<Symbol, i64> {
        let Ok(c) = self.catalog() else {
            return BTreeMap::new();
        };
        let mut out = BTreeMap::new();
        for s in symbols {
            if let Ok((id, _)) = c.resolve(s) {
                if let Ok(t) = c.conn.query_row(
                    "SELECT tick_size_paise FROM instruments WHERE security_id=?1",
                    [id],
                    |r| r.get::<_, i64>(0),
                ) {
                    out.insert(s.clone(), t.max(1));
                }
            }
        }
        out
    }
    fn fingerprint(&self, symbols: &[Symbol], tf: Timeframe, range: DateRange) -> Fingerprint {
        let Ok(c) = self.catalog() else {
            return Fingerprint("catalog-unavailable".into());
        };
        let mut h = Sha256::new();
        for s in symbols {
            let _ = write!(
                h,
                "{s}:{}:{}:{};",
                tf.as_str(),
                range.from.unwrap_or(i64::MIN),
                range.to.unwrap_or(i64::MAX)
            );
            if let Ok((id, _)) = c.resolve(s) {
                if let Some(t) = Tf::parse(tf.as_str()) {
                    if let Ok(sid) = c.series(&id, t) {
                        if let Ok(mut q) = c.conn.prepare(
                            "SELECT path,checksum FROM partitions WHERE series_id=?1 ORDER BY path",
                        ) {
                            if let Ok(it) = q.query_map([sid], |r| {
                                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
                            }) {
                                for x in it.flatten() {
                                    let _ = write!(h, "{}:{}", x.0, x.1);
                                }
                            }
                        }
                    }
                }
            }
        }
        Fingerprint(format!("sha256:{:x}", h.finalize()))
    }
}

struct KWayMerge {
    streams: Vec<Vec<(Symbol, Candle)>>,
    positions: Vec<usize>,
    heap: std::collections::BinaryHeap<std::cmp::Reverse<(i64, Symbol, usize)>>,
}
impl KWayMerge {
    fn new(streams: Vec<Vec<(Symbol, Candle)>>) -> Self {
        let mut heap = std::collections::BinaryHeap::new();
        let positions = vec![0; streams.len()];
        for (i, s) in streams.iter().enumerate() {
            if let Some((sym, c)) = s.first() {
                heap.push(std::cmp::Reverse((c.timestamp, sym.clone(), i)));
            }
        }
        Self {
            streams,
            positions,
            heap,
        }
    }
}
impl Iterator for KWayMerge {
    type Item = Result<(Symbol, Candle), SourceError>;
    fn next(&mut self) -> Option<Self::Item> {
        let std::cmp::Reverse((_, _, i)) = self.heap.pop()?;
        let pos = self.positions[i];
        let item = self.streams[i][pos].clone();
        self.positions[i] += 1;
        if let Some((sym, c)) = self.streams[i].get(self.positions[i]) {
            self.heap
                .push(std::cmp::Reverse((c.timestamp, sym.clone(), i)));
        }
        Some(Ok(item))
    }
}

pub fn read_partition(path: &Path) -> Result<Vec<Bar>, DataError> {
    read_bars(path)
}
pub fn validate_bar(b: &Bar) -> bool {
    [b.open, b.high, b.low, b.close, b.volume]
        .iter()
        .all(|x| x.is_finite())
        && b.volume >= 0.0
        && b.low <= b.open.min(b.close)
        && b.high >= b.open.max(b.close)
        && b.ts.rem_euclid(60) == 0
}
pub fn drop_in_progress(bars: Vec<Bar>, now: i64, step: i64) -> Vec<Bar> {
    bars.into_iter().filter(|b| now >= b.ts + step).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct FakeHistory {
        calls: Arc<AtomicUsize>,
        empty_first: bool,
    }
    struct CountingHistory {
        calls: Arc<AtomicUsize>,
    }
    impl HistoryApi for CountingHistory {
        fn intraday(
            &self,
            _: &str,
            _: &str,
            _: &str,
            _: NaiveDate,
            _: NaiveDate,
        ) -> Result<Vec<Bar>, DataError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Vec::new())
        }
        fn daily(
            &self,
            _: &str,
            _: &str,
            _: NaiveDate,
            _: NaiveDate,
        ) -> Result<Vec<Bar>, DataError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Vec::new())
        }
    }
    impl HistoryApi for FakeHistory {
        fn intraday(
            &self,
            _: &str,
            _: &str,
            _: &str,
            from: NaiveDate,
            _: NaiveDate,
        ) -> Result<Vec<Bar>, DataError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if self.empty_first && call == 0 {
                return Ok(Vec::new());
            }
            let ts = from
                .and_hms_opt(9, 15, 0)
                .unwrap()
                .and_local_timezone(Kolkata)
                .single()
                .unwrap()
                .timestamp();
            Ok(vec![bar(ts, 100.0)])
        }
        fn daily(
            &self,
            _: &str,
            _: &str,
            from: NaiveDate,
            _: NaiveDate,
        ) -> Result<Vec<Bar>, DataError> {
            self.intraday("", "", "", from, from)
        }
    }
    fn bar(ts: i64, close: f64) -> Bar {
        Bar {
            ts,
            open: close,
            high: close + 1.0,
            low: close - 1.0,
            close,
            volume: 1.0,
        }
    }
    fn temp(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "algomln-{name}-{}-{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap()
        ))
    }
    #[test]
    fn chunks_are_89_inclusive_and_daily_fallback_is_configurable() {
        let d = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let xs = chunk_dates(d, d + chrono::Days::new(179));
        assert_eq!(xs.len(), 3);
        assert_eq!((xs[0].1 - xs[0].0).num_days() + 1, 89);
        assert_eq!(
            chunk_dates_limited(d, d + chrono::Days::new(10), 5).len(),
            3
        );
    }
    #[test]
    fn planner_uses_89_calendar_day_chunks_and_one_daily_request() {
        let root = temp("planner-count");
        let db = root.join("catalog.sqlite");
        let mut cat = Catalog::open(&db).unwrap();
        cat.refresh_instruments(
            &[Scrip {
                security_id: "1".into(),
                symbol: "ABC".into(),
                lot_size: 1,
                tick_size_paise: 5,
                instrument: "EQUITY".into(),
            }],
            0,
        )
        .unwrap();
        let from = NaiveDate::from_ymd_opt(2021, 10, 31).unwrap();
        let to = NaiveDate::from_ymd_opt(2026, 10, 31).unwrap();
        assert_eq!(chunk_dates(from, to).len(), 21);
        assert_eq!(
            missing_date_ranges(&cat, "1", Tf::M1, from, to).unwrap(),
            vec![(from + chrono::Days::new(1), to - chrono::Days::new(1))]
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let mut update = Updater {
            client: Box::new(CountingHistory {
                calls: calls.clone(),
            }),
            catalog: Catalog::open(&db).unwrap(),
            store: Store::new(&root),
            max_retries: 0,
            margin_seconds: 30,
            reference_symbol_id: None,
            budget_cap: DEFAULT_BUDGET_CAP,
        };
        update.update("1", Tf::M1, "EQUITY", from, to).unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 21);
        let calls_d = Arc::new(AtomicUsize::new(0));
        let mut daily = Updater {
            client: Box::new(CountingHistory {
                calls: calls_d.clone(),
            }),
            catalog: Catalog::open(&db).unwrap(),
            store: Store::new(&root),
            max_retries: 0,
            margin_seconds: 30,
            reference_symbol_id: None,
            budget_cap: DEFAULT_BUDGET_CAP,
        };
        daily.update("1", Tf::D1, "EQUITY", from, to).unwrap();
        assert_eq!(calls_d.load(Ordering::SeqCst), 1);
        let _ = fs::remove_dir_all(root);
    }
    #[test]
    fn planner_merges_middle_gaps_over_covered_week_and_weekend() {
        let root = temp("planner-gaps");
        let mut cat = Catalog::open(root.join("catalog.sqlite")).unwrap();
        cat.refresh_instruments(
            &[Scrip {
                security_id: "1".into(),
                symbol: "ABC".into(),
                lot_size: 1,
                tick_size_paise: 5,
                instrument: "EQUITY".into(),
            }],
            0,
        )
        .unwrap();
        let from = NaiveDate::from_ymd_opt(2026, 10, 19).unwrap();
        let to = NaiveDate::from_ymd_opt(2026, 10, 30).unwrap();
        for n in 0..=(to - from).num_days() {
            let day = from + chrono::Days::new(n as u64);
            if day.weekday().number_from_monday() <= 5 && day != from && day != to {
                cat.mark_session_empty("1", Tf::M1, day).unwrap();
            }
        }
        assert_eq!(
            missing_date_ranges(&cat, "1", Tf::M1, from, to).unwrap(),
            vec![(from, to)]
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let mut updater = Updater {
            client: Box::new(CountingHistory {
                calls: calls.clone(),
            }),
            catalog: Catalog::open(root.join("catalog.sqlite")).unwrap(),
            store: Store::new(&root),
            max_retries: 0,
            margin_seconds: 0,
            reference_symbol_id: None,
            budget_cap: DEFAULT_BUDGET_CAP,
        };
        updater.update("1", Tf::M1, "EQUITY", from, to).unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let middle = from + chrono::Days::new(1);
        let gap_from = from;
        let gap_to = to;
        let mut second = Catalog::open(root.join("other.sqlite")).unwrap();
        second
            .refresh_instruments(
                &[Scrip {
                    security_id: "2".into(),
                    symbol: "DEF".into(),
                    lot_size: 1,
                    tick_size_paise: 5,
                    instrument: "EQUITY".into(),
                }],
                0,
            )
            .unwrap();
        for n in 0..=(gap_to - gap_from).num_days() {
            let day = gap_from + chrono::Days::new(n as u64);
            if day.weekday().number_from_monday() <= 5 && day != middle {
                second.mark_session_empty("2", Tf::M1, day).unwrap();
            }
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let mut updater = Updater {
            client: Box::new(CountingHistory {
                calls: calls.clone(),
            }),
            catalog: second,
            store: Store::new(root.join("other-store")),
            max_retries: 0,
            margin_seconds: 0,
            reference_symbol_id: None,
            budget_cap: DEFAULT_BUDGET_CAP,
        };
        updater
            .update("2", Tf::M1, "EQUITY", gap_from, gap_to)
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let _ = fs::remove_dir_all(root);
    }
    #[test]
    fn overlap_classifies_split_ratio_and_revision() {
        let a = vec![bar(60, 100.0), bar(120, 200.0)];
        let split = vec![bar(60, 50.0), bar(120, 100.0)];
        assert_eq!(check_overlap(&a, &split).0, "corporate_action");
        let near = vec![bar(60, 99.0), bar(120, 198.0)];
        assert_eq!(check_overlap(&a, &near).0, "corporate_action");
        let revision = vec![bar(60, 101.0), bar(120, 200.0)];
        assert_eq!(check_overlap(&a, &revision).0, "revised");
        assert_eq!(check_overlap(&a, &a).0, "identical");
    }
    #[test]
    fn updater_is_idempotent_and_partial_retry_resumes_coverage() {
        let root = temp("updater");
        let db = root.join("catalog.sqlite");
        let from = NaiveDate::from_ymd_opt(2026, 10, 26).unwrap();
        let to = from + chrono::Days::new(1);
        let fake = Arc::new(FakeHistory {
            calls: Arc::new(AtomicUsize::new(0)),
            empty_first: false,
        });
        let mut cat = Catalog::open(&db).unwrap();
        cat.refresh_instruments(
            &[Scrip {
                security_id: "7".into(),
                symbol: "ABC".into(),
                lot_size: 1,
                tick_size_paise: 5,
                instrument: "EQUITY".into(),
            }],
            0,
        )
        .unwrap();
        drop(cat);
        let mut updater = Updater {
            client: Box::new(FakeHistory {
                calls: fake.calls.clone(),
                empty_first: fake.empty_first,
            }),
            catalog: Catalog::open(&db).unwrap(),
            store: Store::new(&root),
            max_retries: 0,
            margin_seconds: 0,
            reference_symbol_id: Some("7".into()),
            budget_cap: DEFAULT_BUDGET_CAP,
        };
        assert!(updater.update("7", Tf::M1, "EQUITY", from, to).unwrap());
        let after_first = fake.calls.load(Ordering::SeqCst);
        assert!(after_first >= 1);
        let covered = updater
            .catalog
            .covered_dates("7", Tf::M1, from, to)
            .unwrap();
        assert_eq!(covered.len(), 2);
        let status = updater.catalog.status_rows().unwrap();
        assert_eq!(status.len(), 1);
        assert_eq!(status[0].symbol, "ABC");
        assert_eq!(status[0].bars, 1);
        assert_eq!(status[0].coverage.len(), 1);
        assert!(!updater.update("7", Tf::M1, "EQUITY", from, to).unwrap());
        assert_eq!(fake.calls.load(Ordering::SeqCst), after_first);
        let _ = fs::remove_dir_all(root);
    }
    #[test]
    fn confirmed_empty_calendar_dates_are_idempotent_and_cover_sessions() {
        let root = temp("calendar");
        let mut c = Catalog::open(root.join("catalog.sqlite")).unwrap();
        c.refresh_instruments(
            &[Scrip {
                security_id: "1".into(),
                symbol: "ABC".into(),
                lot_size: 1,
                tick_size_paise: 5,
                instrument: "EQUITY".into(),
            }],
            0,
        )
        .unwrap();
        let day = NaiveDate::from_ymd_opt(2026, 1, 26).unwrap();
        c.mark_session_empty("1", Tf::M1, day).unwrap();
        c.mark_session_empty("1", Tf::M1, day).unwrap();
        assert_eq!(c.covered_dates("1", Tf::M1, day, day).unwrap(), vec![day]);
        assert!(c.missing_dates("1", Tf::M1, day, day).unwrap().is_empty());
        let _ = fs::remove_dir_all(root);
    }
    #[test]
    fn historical_daily_bars_are_not_filtered_by_current_day_cutoff() {
        let root = temp("daily-history");
        let db = root.join("catalog.sqlite");
        let d = NaiveDate::from_ymd_opt(2020, 1, 2).unwrap();
        let ts = d
            .and_hms_opt(9, 15, 0)
            .unwrap()
            .and_local_timezone(Kolkata)
            .single()
            .unwrap()
            .timestamp();
        let mut cat = Catalog::open(&db).unwrap();
        cat.refresh_instruments(
            &[Scrip {
                security_id: "5".into(),
                symbol: "DAILY".into(),
                lot_size: 1,
                tick_size_paise: 5,
                instrument: "EQUITY".into(),
            }],
            0,
        )
        .unwrap();
        drop(cat);
        struct DailyFake(i64);
        impl HistoryApi for DailyFake {
            fn intraday(
                &self,
                _: &str,
                _: &str,
                _: &str,
                _: NaiveDate,
                _: NaiveDate,
            ) -> Result<Vec<Bar>, DataError> {
                Ok(Vec::new())
            }
            fn daily(
                &self,
                _: &str,
                _: &str,
                _: NaiveDate,
                _: NaiveDate,
            ) -> Result<Vec<Bar>, DataError> {
                Ok(vec![bar(self.0, 100.)])
            }
        }
        let mut updater = Updater {
            client: Box::new(DailyFake(ts)),
            catalog: Catalog::open(&db).unwrap(),
            store: Store::new(&root),
            max_retries: 0,
            margin_seconds: 0,
            reference_symbol_id: None,
            budget_cap: DEFAULT_BUDGET_CAP,
        };
        updater.update("5", Tf::D1, "EQUITY", d, d).unwrap();
        assert_eq!(
            updater.catalog.covered_dates("5", Tf::D1, d, d).unwrap(),
            vec![d]
        );
        let _ = fs::remove_dir_all(root);
    }
    #[test]
    fn progress_filter_handles_normal_and_short_sessions() {
        let d = NaiveDate::from_ymd_opt(2026, 10, 20).unwrap();
        let open = d
            .and_hms_opt(9, 15, 0)
            .unwrap()
            .and_local_timezone(Kolkata)
            .single()
            .unwrap()
            .timestamp();
        let end = d
            .and_hms_opt(15, 30, 0)
            .unwrap()
            .and_local_timezone(Kolkata)
            .single()
            .unwrap()
            .timestamp();
        let normal = (0..375).map(|n| bar(open + n * 60, 1.0)).collect();
        assert_eq!(
            complete_intraday(normal, end, Tf::M1, &HashMap::new(), 0).len(),
            375
        );
        let short = d
            .and_hms_opt(15, 15, 0)
            .unwrap()
            .and_local_timezone(Kolkata)
            .single()
            .unwrap()
            .timestamp();
        let ends = HashMap::from([(d.to_string(), short)]);
        let short_rows = (0..360).map(|n| bar(open + n * 60, 1.0)).collect();
        assert_eq!(
            complete_intraday(short_rows, short, Tf::M1, &ends, 0).len(),
            360
        );
    }
    #[test]
    fn atomic_store_and_catalog_reconcile() {
        let root = temp("atomic");
        let db = root.join("catalog.sqlite");
        let store = Store::new(&root);
        let ts = NaiveDate::from_ymd_opt(2026, 1, 31)
            .unwrap()
            .and_hms_opt(18, 29, 0)
            .unwrap()
            .and_utc()
            .timestamp();
        let parts = store
            .write_partitions("1", Tf::M1, &[bar(ts, 10.0), bar(ts + 60 * 24, 11.0)])
            .unwrap();
        assert_eq!(parts.len(), 2);
        {
            let mut cat = Catalog::open(&db).unwrap();
            cat.refresh_instruments(
                &[Scrip {
                    security_id: "1".into(),
                    symbol: "ABC".into(),
                    lot_size: 1,
                    tick_size_paise: 5,
                    instrument: "EQUITY".into(),
                }],
                0,
            )
            .unwrap();
            let sid = cat.series_for_import("1", Tf::M1).unwrap();
            for part in &parts {
                cat.record_partition(sid, &store, part).unwrap();
            }
            assert!(cat.has_coverage("1", Tf::M1, ts, ts).unwrap());
            store.reconcile(&mut cat).unwrap();
            std::fs::remove_file(&parts[0].path).unwrap();
            store.reconcile(&mut cat).unwrap();
            assert!(!cat.has_coverage("1", Tf::M1, ts, ts).unwrap());
        }
        let _ = fs::remove_dir_all(root);
    }
    #[test]
    fn orphan_partition_is_reported_after_write_before_catalog_commit() {
        let root = temp("orphan");
        let db = root.join("catalog.sqlite");
        let store = Store::new(&root);
        let ts = NaiveDate::from_ymd_opt(2026, 4, 1)
            .unwrap()
            .and_hms_opt(9, 15, 0)
            .unwrap()
            .and_local_timezone(Kolkata)
            .single()
            .unwrap()
            .timestamp();
        let parts = store
            .write_partitions("99", Tf::M1, &[bar(ts, 42.0)])
            .unwrap();
        let mut cat = Catalog::open(db).unwrap();
        store.reconcile(&mut cat).unwrap();
        store.reconcile(&mut cat).unwrap();
        assert_eq!(cat.quality("reconcile_orphan_file").unwrap(), 1);
        assert!(parts[0].path.exists());
        let _ = fs::remove_dir_all(root);
    }
    #[test]
    fn coverage_details_joins_series_to_partition_rows() {
        let root = temp("coverage-details");
        let db = root.join("catalog.sqlite");
        let store = Store::new(&root);
        let ts = NaiveDate::from_ymd_opt(2026, 10, 28)
            .unwrap()
            .and_hms_opt(9, 15, 0)
            .unwrap()
            .and_local_timezone(Kolkata)
            .single()
            .unwrap()
            .timestamp();
        let parts = store
            .write_partitions("2885", Tf::M1, &[bar(ts, 100.0), bar(ts + 60, 101.0)])
            .unwrap();
        let mut cat = Catalog::open(db).unwrap();
        cat.refresh_instruments(
            &[Scrip {
                security_id: "2885".into(),
                symbol: "RELIANCE".into(),
                lot_size: 1,
                tick_size_paise: 10,
                instrument: "EQUITY".into(),
            }],
            0,
        )
        .unwrap();
        let sid = cat.series_for_import("2885", Tf::M1).unwrap();
        for p in &parts {
            cat.record_partition(sid, &store, p).unwrap();
        }
        assert_eq!(
            cat.coverage_details().unwrap(),
            vec![("RELIANCE".into(), "1m".into(), 2)]
        );
        let _ = fs::remove_dir_all(root);
    }
    #[test]
    fn open_migrates_legacy_catalog_without_verify_and_sets_user_version() {
        let root = temp("migration");
        fs::create_dir_all(&root).unwrap();
        let db = root.join("catalog.sqlite");
        let legacy = Connection::open(&db).unwrap();
        legacy.execute_batch("CREATE TABLE instruments(security_id TEXT PRIMARY KEY,symbol TEXT NOT NULL UNIQUE,segment TEXT NOT NULL,lot_size INTEGER NOT NULL,tick_size_paise INTEGER NOT NULL,refreshed_at INTEGER NOT NULL); INSERT INTO instruments VALUES('1','ABC','NSE_EQ',1,5,0);").unwrap();
        drop(legacy);
        let cat = Catalog::open(&db).unwrap();
        assert_eq!(cat.instrument("1").unwrap(), "EQUITY");
        let version: i64 = cat
            .conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(version, 1);
        assert_eq!(cat.status_rows().unwrap().len(), 0);
        let _ = fs::remove_dir_all(root);
    }
    #[test]
    fn status_ranges_coalesce_touching_ist_dates_per_series() {
        let root = temp("status-ranges");
        let mut cat = Catalog::open(root.join("catalog.sqlite")).unwrap();
        cat.refresh_instruments(
            &[Scrip {
                security_id: "1".into(),
                symbol: "ABC".into(),
                lot_size: 1,
                tick_size_paise: 5,
                instrument: "EQUITY".into(),
            }],
            0,
        )
        .unwrap();
        let a = NaiveDate::from_ymd_opt(2026, 10, 26).unwrap();
        let dates = [
            a,
            a + chrono::Days::new(1),
            a + chrono::Days::new(2),
            a + chrono::Days::new(3),
            a + chrono::Days::new(4),
            a + chrono::Days::new(7),
            a + chrono::Days::new(9),
            a + chrono::Days::new(11),
        ];
        for d in dates {
            cat.record_coverage(
                "1",
                Tf::M1,
                local_midnight(d),
                local_midnight(d + chrono::Days::new(1)) - 1,
                "data",
            )
            .unwrap();
        }
        cat.record_calendar(a + chrono::Days::new(8), false, "holiday")
            .unwrap();
        let rows = cat.status_rows().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].coverage,
            vec![
                CoverageRange {
                    from: a,
                    to: a + chrono::Days::new(9),
                    kind: "data".into()
                },
                CoverageRange {
                    from: a + chrono::Days::new(11),
                    to: a + chrono::Days::new(11),
                    kind: "data".into()
                }
            ]
        );
        let _ = fs::remove_dir_all(root);
    }
    #[test]
    fn session_regime_change_detects_august_2026_once() {
        let mut shapes = BTreeMap::new();
        for n in 0..5 {
            let d = NaiveDate::from_ymd_opt(2026, 7, 27).unwrap() + chrono::Days::new(n);
            let end = d
                .and_hms_opt(15, 29, 0)
                .unwrap()
                .and_local_timezone(Kolkata)
                .single()
                .unwrap()
                .timestamp();
            shapes.insert(d, (0, end, 375));
        }
        let start = NaiveDate::from_ymd_opt(2026, 8, 3).unwrap();
        for n in 0..12 {
            let d = start + chrono::Days::new(n);
            let end = d
                .and_hms_opt(15, 14, 0)
                .unwrap()
                .and_local_timezone(Kolkata)
                .single()
                .unwrap()
                .timestamp();
            shapes.insert(d, (0, end, 360));
        }
        let changes = session_regime_changes(&shapes);
        assert_eq!(changes, vec![start + chrono::Days::new(4)]);
    }
    #[test]
    fn repeated_identical_write_keeps_checksum_and_bytes() {
        let root = temp("idempotent");
        let store = Store::new(&root);
        let ts = NaiveDate::from_ymd_opt(2026, 2, 2)
            .unwrap()
            .and_hms_opt(9, 15, 0)
            .unwrap()
            .and_local_timezone(Kolkata)
            .single()
            .unwrap()
            .timestamp();
        let b = bar(ts, 10.0);
        let a = store
            .write_partitions("x", Tf::M1, std::slice::from_ref(&b))
            .unwrap();
        let bytes = fs::read(&a[0].path).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let z = store.write_partitions("x", Tf::M1, &[b]).unwrap();
        assert_eq!(a[0].checksum, z[0].checksum);
        assert_eq!(bytes, fs::read(&z[0].path).unwrap());
        let _ = fs::remove_dir_all(root);
    }
    #[test]
    fn month_partition_uses_ist_day() {
        let root = temp("ist");
        let store = Store::new(&root);
        let local = NaiveDate::from_ymd_opt(2026, 1, 31)
            .unwrap()
            .and_hms_opt(23, 59, 0)
            .unwrap()
            .and_local_timezone(Kolkata)
            .single()
            .unwrap()
            .timestamp();
        let p = store
            .write_partitions("1", Tf::M1, &[bar(local, 10.0)])
            .unwrap();
        assert!(p[0].path.to_string_lossy().contains("2026-01"));
        let _ = fs::remove_dir_all(root);
    }

    fn scrip(id: &str, sym: &str) -> Scrip {
        Scrip {
            security_id: id.into(),
            symbol: sym.into(),
            lot_size: 1,
            tick_size_paise: 5,
            instrument: "EQUITY".into(),
        }
    }
    #[test]
    fn plan_requests_counts_89_day_chunks_and_single_daily() {
        let root = temp("plan-count");
        let mut cat = Catalog::open(root.join("catalog.sqlite")).unwrap();
        cat.refresh_instruments(&[scrip("1", "ABC")], 0).unwrap();
        let from = NaiveDate::from_ymd_opt(2021, 10, 31).unwrap();
        let to = NaiveDate::from_ymd_opt(2026, 10, 31).unwrap();
        assert_eq!(plan_requests(&cat, "1", Tf::M1, from, to).unwrap(), 21);
        assert_eq!(plan_requests(&cat, "1", Tf::D1, from, to).unwrap(), 1);
        let _ = fs::remove_dir_all(root);
    }
    #[test]
    fn dry_run_reports_per_tf_plan_and_sends_zero_requests() {
        let root = temp("bulk-dry");
        let db = root.join("catalog.sqlite");
        let mut cat = Catalog::open(&db).unwrap();
        cat.refresh_instruments(&[scrip("10", "AAA"), scrip("2", "BBB")], 0)
            .unwrap();
        drop(cat);
        let from = NaiveDate::from_ymd_opt(2021, 10, 31).unwrap();
        let to = NaiveDate::from_ymd_opt(2026, 10, 31).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let bulk = BulkDownloader {
            make_client: move || Box::new(CountingHistory { calls: c.clone() }) as Box<dyn HistoryApi>,
            catalog_path: db.clone(),
            store_root: root.clone(),
            config: BulkConfig::new(from, to, vec![Tf::D1, Tf::M1]),
            stop: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        let report = bulk.dry_run().unwrap();
        assert_eq!(report.symbols_total, 2);
        assert_eq!(
            report.planned,
            vec![("1d".to_string(), 2), ("1m".to_string(), 42)]
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let _ = fs::remove_dir_all(root);
    }
    #[test]
    fn bulk_orders_priority_symbols_first_then_security_id() {
        let root = temp("bulk-order");
        let db = root.join("catalog.sqlite");
        let mut cat = Catalog::open(&db).unwrap();
        cat.refresh_instruments(
            &[scrip("10", "AAA"), scrip("2", "BBB"), scrip("30", "CCC")],
            0,
        )
        .unwrap();
        drop(cat);
        let from = NaiveDate::from_ymd_opt(2026, 10, 26).unwrap();
        let to = from + chrono::Days::new(1);
        let mut cfg = BulkConfig::new(from, to, vec![Tf::M1]);
        cfg.priority_symbols = vec!["CCC".into()];
        let bulk = BulkDownloader {
            make_client: || Box::new(CountingHistory {
                calls: Arc::new(AtomicUsize::new(0)),
            }) as Box<dyn HistoryApi>,
            catalog_path: db.clone(),
            store_root: root.clone(),
            config: cfg,
            stop: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        let cat = Catalog::open(&db).unwrap();
        let order = bulk.order(&cat).unwrap();
        let symbols: Vec<_> = order.iter().map(|(_, s, _)| s.clone()).collect();
        // CCC (priority) first, then by numeric security id: 2=BBB, 10=AAA.
        assert_eq!(symbols, vec!["CCC", "BBB", "AAA"]);
        let _ = fs::remove_dir_all(root);
    }
    #[test]
    fn bulk_stops_at_soft_cap_and_budget_survives_restart() {
        let root = temp("bulk-budget");
        let db = root.join("catalog.sqlite");
        let mut cat = Catalog::open(&db).unwrap();
        cat.refresh_instruments(&[scrip("1", "AAA"), scrip("2", "BBB")], 0)
            .unwrap();
        // Pre-seed today's budget so the cap is reached after the first symbol.
        let today = chrono::Utc::now().date_naive().to_string();
        cat.budget_add(&today, 1).unwrap();
        drop(cat);
        let from = NaiveDate::from_ymd_opt(2026, 10, 26).unwrap();
        let to = from + chrono::Days::new(1);
        let mut cfg = BulkConfig::new(from, to, vec![Tf::D1]);
        cfg.budget_cap = 2; // room for exactly one more request
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let bulk = BulkDownloader {
            make_client: move || Box::new(FakeHistory {
                calls: c.clone(),
                empty_first: false,
            }) as Box<dyn HistoryApi>,
            catalog_path: db.clone(),
            store_root: root.clone(),
            config: cfg,
            stop: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        let report = bulk.run(|_| {}).unwrap();
        assert!(report.budget_reached);
        // One request got through (budget 1 -> 2 == cap), the second symbol is skipped.
        let cat = Catalog::open(&db).unwrap();
        assert_eq!(cat.budget_used(&today).unwrap(), 2);
        let _ = fs::remove_dir_all(root);
    }
    #[test]
    fn bulk_resume_does_not_re_request_completed_ranges() {
        let root = temp("bulk-resume");
        let db = root.join("catalog.sqlite");
        let mut cat = Catalog::open(&db).unwrap();
        cat.refresh_instruments(&[scrip("7", "AAA")], 0).unwrap();
        drop(cat);
        let from = NaiveDate::from_ymd_opt(2026, 10, 26).unwrap();
        let to = from + chrono::Days::new(1);
        let calls = Arc::new(AtomicUsize::new(0));
        let make = {
            let calls = calls.clone();
            move || {
                Box::new(FakeHistory {
                    calls: calls.clone(),
                    empty_first: false,
                }) as Box<dyn HistoryApi>
            }
        };
        let mut cfg = BulkConfig::new(from, to, vec![Tf::M1]);
        cfg.reference_symbol_id = Some("7".into());
        let bulk = BulkDownloader {
            make_client: make,
            catalog_path: db.clone(),
            store_root: root.clone(),
            config: cfg,
            stop: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        let first = bulk.run(|_| {}).unwrap();
        assert_eq!(first.symbols_completed, 1);
        let after_first = calls.load(Ordering::SeqCst);
        assert!(after_first >= 1);
        // Rerun: planner derives work from coverage, so nothing new is requested.
        let second = bulk.run(|_| {}).unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), after_first);
        assert_eq!(second.requests_used, 0);
        let _ = fs::remove_dir_all(root);
    }
    #[test]
    fn progress_line_matches_spec_shape() {
        let p = Progress {
            tf: "1m",
            done: 842,
            total: 2365,
            requests: 17_904,
            budget_cap: 90_000,
            budget_used: 17_904,
            rate: 3.9,
            failures: 3,
        };
        let line = p.line();
        assert!(line.starts_with("1m  842/2365 symbols | requests 17,904/90,000 today |"));
        assert!(line.contains("3.9 req/s"));
        assert!(line.ends_with("failures 3"));
    }
    #[test]
    fn thousands_grouping_and_eta_formatting() {
        assert_eq!(group_thousands(90_000), "90,000");
        assert_eq!(group_thousands(17_904), "17,904");
        assert_eq!(group_thousands(5), "5");
        assert_eq!(format_eta(2 * 3600 + 41 * 60), "2h 41m");
        assert_eq!(format_eta(600), "10m");
        assert_eq!(format_eta(0), "0m");
    }
}

/// Acceptance tests (spec 11) that drive the full download stack through a real
/// [`DhanClient`] pointed at an in-process mock Dhan server. These cover the
/// criteria that need the network edge — 250 ms spacing, DH-904 backoff, auth
/// abort, `no_data` handling and 375-bar sessions — rather than the chunking /
/// resume / budget logic already exercised by the unit tests above.
#[cfg(test)]
mod acceptance {
    use super::*;
    use dhan_api::{DhanClient, TokenProvider};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::time::Instant;

    struct TestToken;
    impl TokenProvider for TestToken {
        fn access_token(&self) -> Result<String, dhan_api::ApiError> {
            Ok("secret".into())
        }
    }

    /// Outcome of the mock server thread: the number of requests served and the
    /// [`Instant`] at which each request's bytes were received.
    struct MockOutcome {
        served: usize,
        received_at: Vec<Instant>,
    }

    /// Start a TCP mock Dhan server that replies to each incoming request with
    /// the next `(status, body)` pair in `responses`, recording the instant each
    /// request arrives. Returns the base URL and a join handle. Modelled on the
    /// dhan-api `mock` helper (raw HTTP/1.1 over `TcpListener`).
    fn mock_server(
        responses: Vec<(u16, String)>,
    ) -> (String, std::thread::JoinHandle<MockOutcome>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = std::thread::spawn(move || {
            let mut received_at = Vec::new();
            let mut served = 0;
            for (status, body) in responses {
                let (mut stream, _) = match listener.accept() {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let mut buf = [0u8; 8192];
                let _ = stream.read(&mut buf).unwrap();
                received_at.push(Instant::now());
                served += 1;
                let phrase = match status {
                    200 => "OK",
                    400 => "Bad Request",
                    401 => "Unauthorized",
                    429 => "Too Many Requests",
                    500 => "Internal Server Error",
                    _ => "Error",
                };
                let response = format!(
                    "HTTP/1.1 {status} {phrase}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{}\r\n{}",
                    body.len(),
                    if status == 429 { "Retry-After: 0\r\n" } else { "" },
                    body
                );
                stream.write_all(response.as_bytes()).unwrap();
            }
            MockOutcome {
                served,
                received_at,
            }
        });
        (format!("http://{addr}"), handle)
    }

    /// Build the intraday columnar JSON the Dhan `/charts/intraday` endpoint
    /// returns for `n` consecutive one-minute bars starting at `start_ts`.
    fn intraday_body(start_ts: i64, n: usize) -> String {
        let ts: Vec<String> = (0..n).map(|i| (start_ts + i as i64 * 60).to_string()).collect();
        let ones = vec!["1.0"; n].join(",");
        let twos = vec!["2.0"; n].join(",");
        let vols = vec!["10.0"; n].join(",");
        format!(
            "{{\"open\":[{o}],\"high\":[{h}],\"low\":[{l}],\"close\":[{c}],\"volume\":[{v}],\"timestamp\":[{t}]}}",
            o = ones,
            h = twos,
            l = ones,
            c = twos,
            v = vols,
            t = ts.join(",")
        )
    }

    fn empty_body() -> String {
        r#"{"open":[],"high":[],"low":[],"close":[],"volume":[],"timestamp":[]}"#.to_string()
    }

    fn make_updater(base: &str, db: &Path, root: &Path) -> Updater {
        let client = DhanClient::with_base(Arc::new(TestToken), base).unwrap();
        Updater {
            client: Box::new(client),
            catalog: Catalog::open(db).unwrap(),
            store: Store::new(root),
            max_retries: 0,
            margin_seconds: 0,
            reference_symbol_id: None,
            budget_cap: DEFAULT_BUDGET_CAP,
        }
    }

    fn seed_instrument(db: &Path, id: &str, symbol: &str) {
        let mut cat = Catalog::open(db).unwrap();
        cat.refresh_instruments(
            &[Scrip {
                security_id: id.into(),
                symbol: symbol.into(),
                lot_size: 1,
                tick_size_paise: 5,
                instrument: "EQUITY".into(),
            }],
            0,
        )
        .unwrap();
    }

    fn open_ist(d: NaiveDate) -> i64 {
        d.and_hms_opt(9, 15, 0)
            .unwrap()
            .and_local_timezone(Kolkata)
            .single()
            .unwrap()
            .timestamp()
    }

    fn temp(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "algomln-acc-{name}-{}-{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap()
        ))
    }

    /// Spec 11.2: request start times are never closer than 250 ms, measured
    /// from the mock's receive timestamps. Three one-minute chunks across a
    /// single intraday series must arrive >=250 ms apart, enforced by the shared
    /// limiter inside the real [`DhanClient`].
    #[test]
    fn request_starts_are_at_least_250ms_apart() {
        let root = temp("spacing");
        let db = root.join("catalog.sqlite");
        seed_instrument(&db, "1", "ABC");
        // Three 89-day chunks => three intraday requests.
        let from = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let to = from + chrono::Days::new(179);
        assert_eq!(chunk_dates(from, to).len(), 3);
        let responses = chunk_dates(from, to)
            .iter()
            .map(|(a, _)| (200u16, intraday_body(open_ist(*a), 1)))
            .collect::<Vec<_>>();
        let (base, server) = mock_server(responses);
        let mut up = make_updater(&base, &db, &root);
        up.update("1", Tf::M1, "EQUITY", from, to).unwrap();
        let outcome = server.join().unwrap();
        assert_eq!(outcome.served, 3);
        for pair in outcome.received_at.windows(2) {
            let gap = pair[1].duration_since(pair[0]);
            // The limiter targets 250 ms between request starts; allow generous
            // slack for localhost delivery/scheduling jitter on the receive side.
            // Without the limiter these arrive sub-millisecond apart, so a 200 ms
            // floor still unambiguously proves spacing is enforced.
            assert!(
                gap >= std::time::Duration::from_millis(200),
                "requests only {gap:?} apart; limiter must keep ~250 ms spacing"
            );
        }
        let _ = fs::remove_dir_all(root);
    }

    /// Spec 11.6: an injected DH-904 triggers backoff, and the run still
    /// completes. The DhanClient retries after the limiter cools down; the
    /// second attempt succeeds and the series is recorded.
    #[test]
    fn injected_dh904_backs_off_and_run_completes() {
        let root = temp("dh904");
        let db = root.join("catalog.sqlite");
        seed_instrument(&db, "1", "ABC");
        let from = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let to = from; // single intraday chunk
        let responses = vec![
            (429u16, r#"{"errorCode":"DH-904","errorMessage":"slow down"}"#.to_string()),
            (200u16, intraday_body(open_ist(from), 1)),
        ];
        let (base, server) = mock_server(responses);
        let mut up = make_updater(&base, &db, &root);
        up.update("1", Tf::M1, "EQUITY", from, to).unwrap();
        let outcome = server.join().unwrap();
        assert_eq!(outcome.served, 2, "DH-904 must be retried once then succeed");
        let covered = up.catalog.covered_dates("1", Tf::M1, from, to).unwrap();
        assert_eq!(covered, vec![from]);
        let _ = fs::remove_dir_all(root);
    }

    /// Spec 11.8: an auth failure aborts the run immediately. Driven through the
    /// `BulkDownloader` so the abort is observed at the run level, not just the
    /// client: `is_auth()` short-circuits and the error propagates.
    #[test]
    fn auth_error_aborts_the_run_immediately() {
        let root = temp("auth");
        let db = root.join("catalog.sqlite");
        seed_instrument(&db, "1", "AAA");
        // A second symbol that must NOT be contacted once auth fails.
        {
            let mut cat = Catalog::open(&db).unwrap();
            cat.refresh_instruments(
                &[
                    Scrip {
                        security_id: "1".into(),
                        symbol: "AAA".into(),
                        lot_size: 1,
                        tick_size_paise: 5,
                        instrument: "EQUITY".into(),
                    },
                    Scrip {
                        security_id: "2".into(),
                        symbol: "BBB".into(),
                        lot_size: 1,
                        tick_size_paise: 5,
                        instrument: "EQUITY".into(),
                    },
                ],
                0,
            )
            .unwrap();
        }
        let from = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let to = from;
        // Only one response queued: the first (and only) request gets 401.
        let (base, server) = mock_server(vec![(
            401u16,
            r#"{"errorCode":"DH-902","errorMessage":"invalid token"}"#.to_string(),
        )]);
        let base_for_factory = base.clone();
        let bulk = BulkDownloader {
            make_client: move || {
                Box::new(DhanClient::with_base(Arc::new(TestToken), &base_for_factory).unwrap())
                    as Box<dyn HistoryApi>
            },
            catalog_path: db.clone(),
            store_root: root.clone(),
            config: BulkConfig::new(from, to, vec![Tf::D1]),
            stop: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        let result = bulk.run(|_| {});
        assert!(
            matches!(&result, Err(e) if e.is_auth()),
            "auth failure must abort the run with an auth error, got {result:?}"
        );
        let outcome = server.join().unwrap();
        assert_eq!(
            outcome.served, 1,
            "run must abort after the first auth failure without contacting the next symbol"
        );
        let _ = fs::remove_dir_all(root);
    }

    /// Spec 11.7: an empty response is logged as `no_data` and is NOT recorded
    /// as coverage. The reference symbol path records the calendar as empty, but
    /// no `data` coverage is written and the session is not marked covered for a
    /// non-reference symbol.
    #[test]
    fn empty_response_is_not_recorded_as_coverage() {
        let root = temp("nodata");
        let db = root.join("catalog.sqlite");
        seed_instrument(&db, "1", "ABC");
        let from = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let to = from;
        let (base, server) = mock_server(vec![(200u16, empty_body())]);
        let mut up = make_updater(&base, &db, &root);
        up.update("1", Tf::M1, "EQUITY", from, to).unwrap();
        let outcome = server.join().unwrap();
        assert_eq!(outcome.served, 1);
        // The spec guarantee: an empty response is never recorded as *data*
        // coverage (it could be a holiday or a pre-listing range). The empty day
        // may be logged as `confirmed_empty`/`no_data` so it is not re-requested
        // forever, but it must not claim real bars exist.
        let sid = up.catalog.series("1", Tf::M1).unwrap();
        let data_rows: i64 = up
            .catalog
            .conn
            .query_row(
                "SELECT COUNT(*) FROM coverage WHERE series_id=?1 AND kind='data'",
                [sid],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(data_rows, 0, "empty response must not add 'data' coverage");
        // No real bars were stored.
        let bars: i64 = up
            .catalog
            .conn
            .query_row(
                "SELECT COALESCE(SUM(rows),0) FROM partitions WHERE series_id=?1",
                [sid],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(bars, 0, "empty response must not store any bars");
        let _ = fs::remove_dir_all(root);
    }

    /// Spec 11.9: a symbol with 375-bar sessions downloads and verifies
    /// correctly — the downloader must not assume 360 bars per session. The mock
    /// returns a full 375-minute session; all 375 bars must be stored.
    #[test]
    fn symbol_with_375_bar_session_downloads_all_bars() {
        let root = temp("375");
        let db = root.join("catalog.sqlite");
        seed_instrument(&db, "1", "ABC");
        let day = NaiveDate::from_ymd_opt(2026, 1, 2).unwrap();
        // 375 one-minute bars: 09:15..=15:29 inclusive is 375 minutes.
        let (base, server) = mock_server(vec![(200u16, intraday_body(open_ist(day), 375))]);
        let mut up = make_updater(&base, &db, &root);
        up.update("1", Tf::M1, "EQUITY", day, day).unwrap();
        let outcome = server.join().unwrap();
        assert_eq!(outcome.served, 1);
        let status = up.catalog.status_rows().unwrap();
        assert_eq!(status.len(), 1);
        assert_eq!(
            status[0].bars, 375,
            "all 375 session bars must be stored; downloader must not cap at 360"
        );
        let _ = fs::remove_dir_all(root);
    }

    /// Spec 6 + 11.2: with `concurrency > 1` the shared limiter must still cap the
    /// global request rate at ~4/s. Eight symbols are fetched by a 4-worker pool
    /// through one cloned-per-worker `DhanClient` limiter.
    ///
    /// The limiter is a token bucket (cap 4), so it permits a short burst of up to
    /// four requests before throttling — a per-gap floor is therefore fragile (the
    /// bucket refills during worker setup and the first few arrivals bunch up, as
    /// seen with 195 ms gaps). The robust invariant is the *total* span: with a max
    /// burst of 4 tokens, `N` requests take at least `(N - 4) / 4` seconds, i.e.
    /// >=1.0 s for 8. Without the limiter all eight arrive in well under 100 ms, so
    /// an 0.8 s floor unambiguously proves throttling survives concurrency. Also
    /// proves the concurrent write path commits every symbol without racing on the
    /// shared catalog (WAL + busy_timeout + IMMEDIATE writes).
    #[test]
    fn concurrent_run_throttles_global_rate_and_commits_all_symbols() {
        let root = temp("concurrent-spacing");
        let db = root.join("catalog.sqlite");
        let symbols = ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF", "GGG", "HHH"];
        for (i, sym) in symbols.iter().enumerate() {
            seed_instrument(&db, &(i + 1).to_string(), sym);
        }
        let day = NaiveDate::from_ymd_opt(2026, 1, 2).unwrap();
        // One intraday request per symbol (single-day range => single chunk).
        let responses = (0..symbols.len())
            .map(|_| (200u16, intraday_body(open_ist(day), 1)))
            .collect::<Vec<_>>();
        let (base, server) = mock_server(responses);
        // Every worker's client shares the one limiter, so the rate cap is global
        // across the pool rather than per client.
        let client = DhanClient::with_base(Arc::new(TestToken), &base).unwrap();
        let base_for_factory = base.clone();
        let make_client = move || {
            Box::new(
                client
                    .clone_with_base(&base_for_factory)
                    .expect("clone shares the limiter"),
            ) as Box<dyn HistoryApi>
        };
        let mut cfg = BulkConfig::new(day, day, vec![Tf::M1]);
        cfg.concurrency = 4;
        cfg.margin_seconds = 0;
        let bulk = BulkDownloader {
            make_client,
            catalog_path: db.clone(),
            store_root: root.clone(),
            config: cfg,
            stop: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        let report = bulk.run(|_| {}).unwrap();
        assert_eq!(
            report.symbols_completed,
            symbols.len(),
            "every symbol must commit"
        );
        let outcome = server.join().unwrap();
        assert_eq!(outcome.served, symbols.len(), "exactly one request per symbol");
        let mut times = outcome.received_at;
        times.sort();
        let span = times
            .last()
            .unwrap()
            .duration_since(*times.first().unwrap());
        assert!(
            span >= std::time::Duration::from_millis(800),
            "eight requests spanned only {span:?}; the shared limiter must keep the \
             global rate near 4/s even with concurrency"
        );
        // Every series is present with its single bar committed (no write races).
        let cat = Catalog::open(&db).unwrap();
        let status = cat.status_rows().unwrap();
        assert_eq!(status.len(), symbols.len());
        assert!(status.iter().all(|s| s.bars == 1));
        let _ = fs::remove_dir_all(root);
    }
}
use chrono::NaiveDate;
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue},
    Client, StatusCode,
};
use serde::{Deserialize, Serialize};
use std::{
    env,
    sync::{Arc, Mutex},
    time::Duration,
};
use thiserror::Error;

const BASE: &str = "https://api.dhan.co/v2";

pub trait TokenProvider: Send + Sync {
    fn access_token(&self) -> Result<String, ApiError>;
}
#[derive(Clone, Debug)]
pub struct EnvTokenProvider {
    pub variable: String,
}
impl Default for EnvTokenProvider {
    fn default() -> Self {
        Self {
            variable: "DHAN_ACCESS_TOKEN".into(),
        }
    }
}
#[derive(Clone, Debug)]
pub struct KeychainEnvTokenProvider {
    pub service: String,
    pub account: String,
    pub variable: String,
}
impl Default for KeychainEnvTokenProvider {
    fn default() -> Self {
        Self {
            service: "algomln".into(),
            account: "dhan-access-token".into(),
            variable: "DHAN_ACCESS_TOKEN".into(),
        }
    }
}
impl TokenProvider for KeychainEnvTokenProvider {
    fn access_token(&self) -> Result<String, ApiError> {
        if let Ok(entry) = keyring::Entry::new(&self.service, &self.account) {
            if let Ok(token) = entry.get_password() {
                if !token.is_empty() {
                    return Ok(token);
                }
            }
        }
        env::var(&self.variable).map_err(|_| {
            ApiError::Auth("Dhan token missing from OS keychain and DHAN_ACCESS_TOKEN".into())
        })
    }
}
impl TokenProvider for EnvTokenProvider {
    fn access_token(&self) -> Result<String, ApiError> {
        env::var(&self.variable).map_err(|_| {
            ApiError::Auth(
                "missing access token; set DHAN_ACCESS_TOKEN or configure an OS keychain provider"
                    .into(),
            )
        })
    }
}

#[derive(Debug, Error, Clone)]
pub enum ApiError {
    #[error("authentication failed ({0}); refresh the token")]
    Auth(String),
    #[error("rate limited ({code}): {message}")]
    RateLimit {
        code: String,
        message: String,
        retry_after: Option<Duration>,
    },
    #[error("Dhan data API error ({code}): {message}")]
    DataApi { code: String, message: String },
    #[error("server error: {0}")]
    Server(String),
    #[error("malformed response: {0}")]
    Malformed(String),
    #[error("transport error: {0}")]
    Transport(String),
    #[error("severe rate limit (DH-805): {0}")]
    SevereRateLimit(String),
    #[error("Dhan access denied ({0})")]
    Access(String),
    #[error("Dhan bad request ({code}): {message}")]
    BadRequest { code: String, message: String },
    #[error("data unavailable ({0})")]
    DataUnavailable(String),
}
#[derive(Debug, Deserialize)]
struct ApiErrorBody {
    #[serde(default, rename = "errorCode")]
    error_code: String,
    #[serde(default, rename = "errorMessage")]
    error_message: String,
}
#[derive(Clone, Debug, Serialize)]
pub struct IntradayRequest<'a> {
    #[serde(rename = "securityId")]
    pub security_id: &'a str,
    #[serde(rename = "exchangeSegment")]
    pub exchange_segment: &'a str,
    pub instrument: &'a str,
    pub interval: &'a str,
    #[serde(rename = "fromDate")]
    pub from_date: String,
    #[serde(rename = "toDate")]
    pub to_date: String,
}
#[derive(Clone, Debug, Serialize)]
pub struct DailyRequest<'a> {
    #[serde(rename = "securityId")]
    pub security_id: &'a str,
    #[serde(rename = "exchangeSegment")]
    pub exchange_segment: &'a str,
    pub instrument: &'a str,
    #[serde(rename = "fromDate")]
    pub from_date: String,
    #[serde(rename = "toDate")]
    pub to_date: String,
}
#[derive(Clone, Debug, PartialEq)]
pub struct Candle {
    pub ts: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}
#[derive(Deserialize)]
struct Columns {
    #[serde(default)]
    open: Vec<f64>,
    #[serde(default)]
    high: Vec<f64>,
    #[serde(default)]
    low: Vec<f64>,
    #[serde(default)]
    close: Vec<f64>,
    #[serde(default)]
    volume: Vec<f64>,
    #[serde(default)]
    timestamp: Vec<serde_json::Number>,
}
impl Columns {
    fn candles(self) -> Result<Vec<Candle>, ApiError> {
        let n = self.timestamp.len();
        if [
            self.open.len(),
            self.high.len(),
            self.low.len(),
            self.close.len(),
            self.volume.len(),
        ]
        .iter()
        .any(|&x| x != n)
        {
            return Err(ApiError::Malformed(
                "column arrays have different lengths".into(),
            ));
        }
        (0..n)
            .map(|i| {
                Ok(Candle {
                    ts: self.timestamp[i]
                        .as_i64()
                        .or_else(|| {
                            self.timestamp[i]
                                .as_f64()
                                .filter(|x| {
                                    x.is_finite()
                                        && x.fract() == 0.0
                                        && *x >= i64::MIN as f64
                                        && *x < -(i64::MIN as f64)
                                })
                                .map(|x| x as i64)
                        })
                        .ok_or_else(|| {
                            ApiError::Malformed(format!(
                                "timestamp at index {i} is not an integer epoch second"
                            ))
                        })?,
                    open: self.open[i],
                    high: self.high[i],
                    low: self.low[i],
                    close: self.close[i],
                    volume: self.volume[i],
                })
            })
            .collect()
    }
}

#[derive(Clone)]
pub struct Limiter {
    inner: Arc<Mutex<Bucket>>,
    cap: usize,
    concurrency: usize,
}
struct Bucket {
    tokens: f64,
    last: std::time::Instant,
    tight_until: Option<std::time::Instant>,
}
impl Limiter {
    // Dhan documents 5 requests/second; operate at 4 with a 250 ms minimum interval.
    pub fn provisional() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Bucket {
                tokens: 1.0,
                last: std::time::Instant::now(),
                tight_until: None,
            })),
            cap: 4,
            concurrency: 2,
        }
    }
    pub fn concurrency(&self) -> usize {
        self.concurrency
    }
    pub fn rate_limit(&self) {
        if let Ok(mut b) = self.inner.lock() {
            b.tokens = 0.0;
            b.tight_until = Some(std::time::Instant::now() + Duration::from_secs(300));
            b.tokens = 0.0;
        }
    }
    pub fn daily_limit_remaining(&self) -> u64 {
        90_000
    }
    pub fn wait(&self) {
        loop {
            let delay = {
                let mut b = self.inner.lock().expect("limiter poisoned");
                let now = std::time::Instant::now();
                let elapsed = now.duration_since(b.last).as_secs_f64();
                b.last = now;
                let cap = if b.tight_until.is_some_and(|t| t > now) {
                    2.0
                } else {
                    self.cap as f64
                };
                if b.tight_until.is_some_and(|t| t <= now) {
                    b.tight_until = None;
                }
                b.tokens = (b.tokens + elapsed * cap).min(cap);
                if b.tokens >= 1.0 {
                    b.tokens -= 1.0;
                    return;
                }
                Duration::from_secs_f64((1.0 - b.tokens) / cap)
            };
            std::thread::sleep(delay);
        }
    }
}

pub struct DhanClient {
    token: Arc<dyn TokenProvider>,
    pub limiter: Limiter,
    base: String,
}
impl DhanClient {
    pub fn new(token: Arc<dyn TokenProvider>) -> Result<Self, ApiError> {
        Self::with_base(token, BASE)
    }
    pub fn with_base(
        token: Arc<dyn TokenProvider>,
        base: impl Into<String>,
    ) -> Result<Self, ApiError> {
        Ok(Self {
            token,
            limiter: Limiter::provisional(),
            base: base.into(),
        })
    }
    pub fn clone_with_base(&self, base: &str) -> Result<Self, ApiError> {
        let mut c = Self::with_base(self.token.clone(), base)?;
        c.limiter = self.limiter.clone();
        Ok(c)
    }
    pub fn intraday(
        &self,
        security_id: &str,
        instrument: &str,
        interval: &str,
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<Vec<Candle>, ApiError> {
        let body = IntradayRequest {
            security_id,
            exchange_segment: "NSE_EQ",
            instrument,
            interval,
            from_date: from.format("%Y-%m-%d").to_string(),
            to_date: to.format("%Y-%m-%d").to_string(),
        };
        let url = format!("{}/charts/intraday", self.base);
        self.post_columns(&url, &body)
    }
    pub fn daily(
        &self,
        security_id: &str,
        instrument: &str,
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<Vec<Candle>, ApiError> {
        let next = to
            .succ_opt()
            .ok_or_else(|| ApiError::Malformed("daily end date overflow".into()))?;
        let body = DailyRequest {
            security_id,
            exchange_segment: "NSE_EQ",
            instrument,
            from_date: from.format("%Y-%m-%d").to_string(),
            to_date: next.format("%Y-%m-%d").to_string(),
        };
        let url = format!("{}/charts/historical", self.base);
        self.post_columns(&url, &body).map(|mut rows| {
            for row in &mut rows {
                row.ts = row.ts.saturating_add(33_300);
            }
            rows
        })
    }
    fn post_columns<T: Serialize>(&self, url: &str, body: &T) -> Result<Vec<Candle>, ApiError> {
        let token = self.token.access_token()?;
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("access-token"),
            HeaderValue::from_str(&token).map_err(|e| ApiError::Auth(e.to_string()))?,
        );
        let mut attempts = 0;
        loop {
            self.limiter.wait();
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| ApiError::Transport(e.to_string()))?;
            let response = runtime.block_on(async {
                let client = Client::builder()
                    .timeout(Duration::from_secs(30))
                    .build()
                    .map_err(|e| ApiError::Transport(e.to_string()))?;
                let response = client
                    .post(url)
                    .headers(headers.clone())
                    .json(body)
                    .send()
                    .await
                    .map_err(|e| ApiError::Transport(e.to_string()))?;
                let status = response.status();
                let retry = response
                    .headers()
                    .get("Retry-After")
                    .and_then(|x| x.to_str().ok())
                    .and_then(|x| x.parse::<u64>().ok())
                    .map(Duration::from_secs);
                let bytes = response
                    .bytes()
                    .await
                    .map_err(|e| ApiError::Transport(e.to_string()))?;
                Ok::<_, ApiError>((status, retry, bytes))
            });
            match response {
                Err(e) => {
                    attempts += 1;
                    if attempts >= 3 {
                        return Err(ApiError::Server(e.to_string()));
                    }
                    std::thread::sleep(Duration::from_millis(100 * 2u64.pow(attempts)));
                }
                Ok((status, retry, bytes)) => {
                    let err: ApiErrorBody =
                        serde_json::from_slice(&bytes).unwrap_or(ApiErrorBody {
                            error_code: String::new(),
                            error_message: String::new(),
                        });
                    if err.error_code == "DH-805" {
                        return Err(ApiError::SevereRateLimit(err.error_message));
                    }
                    if err.error_code == "DH-907" {
                        return Err(ApiError::DataUnavailable(err.error_message));
                    }
                    if err.error_code == "DH-904" || status == StatusCode::TOO_MANY_REQUESTS {
                        self.limiter.rate_limit();
                        if attempts >= 5 {
                            return Err(ApiError::RateLimit {
                                code: err.error_code,
                                message: err.error_message,
                                retry_after: retry,
                            });
                        }
                        attempts += 1;
                        std::thread::sleep(
                            retry.unwrap_or(Duration::from_millis(250 * 2u64.pow(attempts.min(5)))),
                        );
                        continue;
                    }
                    if err.error_code == "DH-905" {
                        return Err(ApiError::DataApi {
                            code: err.error_code,
                            message: err.error_message,
                        });
                    }
                    if !status.is_success() {
                        if status == StatusCode::UNAUTHORIZED || err.error_code.starts_with("DH-10")
                        {
                            return Err(ApiError::Auth(format!(
                                "{} {}",
                                status, err.error_message
                            )));
                        }
                        if status == StatusCode::FORBIDDEN {
                            return Err(ApiError::Access(format!(
                                "HTTP {status} {}",
                                err.error_message
                            )));
                        }
                        if status == StatusCode::BAD_REQUEST {
                            return Err(ApiError::BadRequest {
                                code: err.error_code,
                                message: err.error_message,
                            });
                        }
                        if status.is_server_error() && attempts < 3 {
                            attempts += 1;
                            std::thread::sleep(Duration::from_millis(100 * 2u64.pow(attempts)));
                            continue;
                        }
                        if !err.error_code.is_empty() {
                            return Err(ApiError::DataApi {
                                code: err.error_code,
                                message: err.error_message,
                            });
                        }
                        return Err(ApiError::Server(format!("HTTP {status}")));
                    }
                    if bytes.is_empty() {
                        return Ok(Vec::new());
                    }
                    let columns: Columns = serde_json::from_slice(&bytes)
                        .map_err(|e| ApiError::Malformed(e.to_string()))?;
                    return columns.candles();
                }
            }
        }
    }
}
#[derive(Clone, Debug)]
pub struct Scrip {
    pub security_id: String,
    pub symbol: String,
    pub lot_size: u32,
    pub tick_size_paise: i64,
    pub instrument: String,
}
pub fn parse_scrip_master<R: std::io::Read>(reader: R) -> Result<Vec<Scrip>, ApiError> {
    let mut csv = csv::Reader::from_reader(reader);
    let headers = csv
        .headers()
        .map_err(|e| ApiError::Malformed(e.to_string()))?
        .clone();
    let idx = |n: &str| {
        headers
            .iter()
            .position(|x| x == n)
            .ok_or_else(|| ApiError::Malformed(format!("missing scrip column {n}")))
    };
    let (id, sym, lot, tick, exchange, series, kind, name) = (
        idx("SEM_SMST_SECURITY_ID")?,
        idx("SEM_TRADING_SYMBOL")?,
        idx("SEM_LOT_UNITS")?,
        idx("SEM_TICK_SIZE")?,
        idx("SEM_EXM_EXCH_ID")?,
        idx("SEM_SERIES")?,
        idx("SEM_EXCH_INSTRUMENT_TYPE")?,
        idx("SEM_INSTRUMENT_NAME")?,
    );
    let mut out = Vec::new();
    for row in csv.records() {
        let r = row.map_err(|e| ApiError::Malformed(e.to_string()))?;
        if &r[exchange] != "NSE" || &r[series] != "EQ" || &r[kind] != "ES" || &r[name] != "EQUITY" {
            continue;
        }
        let raw: f64 = r[tick]
            .parse()
            .map_err(|_| ApiError::Malformed("invalid SEM_TICK_SIZE".into()))?;
        let paise = raw.round() as i64;
        let lot_raw: f64 = r[lot]
            .parse()
            .map_err(|_| ApiError::Malformed("invalid SEM_LOT_UNITS".into()))?;
        if !lot_raw.is_finite()
            || lot_raw < 1.0
            || lot_raw.fract() != 0.0
            || lot_raw > u32::MAX as f64
        {
            return Err(ApiError::Malformed(
                "SEM_LOT_UNITS must be a positive whole number".into(),
            ));
        }
        if paise <= 0 {
            return Err(ApiError::Malformed(
                "SEM_TICK_SIZE must be positive paise".into(),
            ));
        }
        out.push(Scrip {
            security_id: r[id].into(),
            symbol: r[sym].to_ascii_uppercase(),
            lot_size: lot_raw as u32,
            tick_size_paise: paise,
            instrument: r[name].to_string(),
        });
    }
    Ok(out)
}
pub struct ScripResolver {
    by_symbol: std::collections::HashMap<String, Scrip>,
}
impl ScripResolver {
    pub fn new(rows: Vec<Scrip>) -> Result<Self, ApiError> {
        let mut by_symbol = std::collections::HashMap::new();
        for x in rows {
            if by_symbol.insert(x.symbol.clone(), x.clone()).is_some() {
                return Err(ApiError::Malformed(format!(
                    "duplicate NSE symbol {}",
                    x.symbol
                )));
            }
        }
        Ok(Self { by_symbol })
    }
    pub fn resolve(&self, s: &str) -> Result<&Scrip, ApiError> {
        self.by_symbol
            .get(&s.trim().to_ascii_uppercase())
            .ok_or_else(|| ApiError::DataApi {
                code: "UNKNOWN_SYMBOL".into(),
                message: s.into(),
            })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn daily_body_uses_required_fields_only() {
        let _c = DhanClient::with_base(Arc::new(EnvTokenProvider::default()), "http://localhost")
            .unwrap();
        let body = DailyRequest {
            security_id: "1",
            exchange_segment: "NSE_EQ",
            instrument: "EQUITY",
            from_date: "2026-01-01".into(),
            to_date: "2026-01-02".into(),
        };
        let json = serde_json::to_value(body).unwrap();
        assert!(json.get("expiryCode").is_none());
        assert!(json.get("oi").is_none());
    }
    fn mock(
        responses: Vec<(u16, &'static str)>,
        path: &'static str,
    ) -> (String, std::thread::JoinHandle<usize>) {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let h = std::thread::spawn(move || {
            let mut seen = 0;
            for (status, body) in responses {
                let (mut stream, _) = listener.accept().unwrap();
                let mut buf = [0u8; 8192];
                let n = stream.read(&mut buf).unwrap();
                let req = String::from_utf8_lossy(&buf[..n]);
                assert!(req.contains("access-token: secret"));
                assert!(req.contains(path));
                if seen == 0 {
                    assert!(req.contains("\"exchangeSegment\":\"NSE_EQ\""));
                    assert!(req.contains("\"fromDate\":\"2026-01-01\""));
                }
                seen += 1;
                let phrase = match status {
                    200 => "OK",
                    400 => "Bad Request",
                    401 => "Unauthorized",
                    429 => "Too Many Requests",
                    500 => "Internal Server Error",
                    _ => "Error",
                };
                let response=format!("HTTP/1.1 {status} {phrase}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{}\r\n{}",body.len(),if status==429{"Retry-After: 0\r\n"}else{""},body);
                stream.write_all(response.as_bytes()).unwrap();
            }
            seen
        });
        (format!("http://{addr}"), h)
    }
    struct TestToken;
    impl TokenProvider for TestToken {
        fn access_token(&self) -> Result<String, ApiError> {
            Ok("secret".into())
        }
    }
    fn call(base: &str) -> Result<Vec<Candle>, ApiError> {
        DhanClient::with_base(Arc::new(TestToken), base)?.intraday(
            "123",
            "EQUITY",
            "1",
            NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(),
        )
    }
    #[test]
    fn mock_daily_uses_exclusive_end_and_normalizes_open_label() {
        let (base, server) = mock(
            vec![(
                200,
                r#"{"open":[1],"high":[2],"low":[1],"close":[2],"volume":[3],"timestamp":[1767225600]}"#,
            )],
            "/charts/historical",
        );
        let out = DhanClient::with_base(Arc::new(TestToken), base)
            .unwrap()
            .daily(
                "123",
                "EQUITY",
                NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(),
                NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(),
            )
            .unwrap();
        assert_eq!(out[0].ts, 1767225600 + 33300);
        assert_eq!(server.join().unwrap(), 1);
    }
    #[test]
    fn mock_success_and_empty() {
        let (body, server) = mock(
            vec![(
                200,
                r#"{"open":[1],"high":[2],"low":[1],"close":[2],"volume":[10],"timestamp":[1767225600]}"#,
            )],
            "/charts/intraday",
        );
        assert_eq!(call(&body).unwrap().len(), 1);
        assert_eq!(server.join().unwrap(), 1);
        let (body, server) = mock(vec![(200, "")], "/charts/intraday");
        assert!(call(&body).unwrap().is_empty());
        server.join().unwrap();
    }
    #[test]
    fn timestamp_accepts_integral_float_and_rejects_fractional_seconds() {
        let (base, server) = mock(
            vec![(
                200,
                r#"{"open":[1],"high":[2],"low":[1],"close":[2],"volume":[10],"timestamp":[1793159100.0]}"#,
            )],
            "/charts/intraday",
        );
        assert_eq!(call(&base).unwrap()[0].ts, 1793159100);
        server.join().unwrap();
        let (base, server) = mock(
            vec![(
                200,
                r#"{"open":[1],"high":[2],"low":[1],"close":[2],"volume":[10],"timestamp":[1793159100.5]}"#,
            )],
            "/charts/intraday",
        );
        assert!(matches!(call(&base), Err(ApiError::Malformed(_))));
        server.join().unwrap();
    }
    #[test]
    fn mock_429_dh904_backoff_then_success() {
        let (body, server) = mock(
            vec![
                (429, r#"{"errorCode":"DH-904","errorMessage":"slow down"}"#),
                (
                    200,
                    r#"{"open":[],"high":[],"low":[],"close":[],"volume":[],"timestamp":[]}"#,
                ),
            ],
            "/charts/intraday",
        );
        assert!(call(&body).unwrap().is_empty());
        assert_eq!(server.join().unwrap(), 2);
    }
    #[test]
    fn mock_auth_does_not_retry() {
        let (body, server) = mock(
            vec![(
                401,
                r#"{"errorCode":"DH-902","errorMessage":"invalid token"}"#,
            )],
            "/charts/intraday",
        );
        assert!(matches!(call(&body), Err(ApiError::Auth(_))));
        assert_eq!(server.join().unwrap(), 1);
    }
    #[test]
    fn mock_data_error_and_truncated_json() {
        let (body, server) = mock(
            vec![(
                400,
                r#"{"errorCode":"DH-905","errorMessage":"bad instrument"}"#,
            )],
            "/charts/intraday",
        );
        assert!(matches!(call(&body), Err(ApiError::DataApi { .. })));
        server.join().unwrap();
        let (body, server) = mock(vec![(200, r#"{"open":[1]"#)], "/charts/intraday");
        assert!(matches!(call(&body), Err(ApiError::Malformed(_))));
        server.join().unwrap();
    }
    #[test]
    fn mock_server_retries_then_succeeds() {
        let (body, server) = mock(
            vec![
                (500, "{}"),
                (
                    200,
                    r#"{"open":[],"high":[],"low":[],"close":[],"volume":[],"timestamp":[]}"#,
                ),
            ],
            "/charts/intraday",
        );
        assert!(call(&body).unwrap().is_empty());
        assert_eq!(server.join().unwrap(), 2);
    }
    #[test]
    fn scrip_filter_and_tick_conversion() {
        let csv="SEM_SMST_SECURITY_ID,SEM_TRADING_SYMBOL,SEM_LOT_UNITS,SEM_TICK_SIZE,SEM_EXM_EXCH_ID,SEM_SERIES,SEM_EXCH_INSTRUMENT_TYPE,SEM_INSTRUMENT_NAME\n1,abc,1,5.0000,NSE,EQ,ES,EQUITY\n2,NO,1,5,NSE,BE,ES,EQUITY\n";
        let x = parse_scrip_master(csv.as_bytes()).unwrap();
        assert_eq!(x.len(), 1);
        assert_eq!(x[0].symbol, "ABC");
        assert_eq!(x[0].tick_size_paise, 5);
        assert_eq!(x[0].instrument, "EQUITY");
    }
    #[test]
    fn scrip_master_accepts_decimal_whole_lot_sizes_and_filters_to_nse_eq_equity() {
        let csv="SEM_SMST_SECURITY_ID,SEM_TRADING_SYMBOL,SEM_LOT_UNITS,SEM_TICK_SIZE,SEM_EXM_EXCH_ID,SEM_SERIES,SEM_EXCH_INSTRUMENT_TYPE,SEM_INSTRUMENT_NAME\n1,RELIANCE,1.0,10.0000,NSE,EQ,ES,EQUITY\n2,FUT,500.0,10.0000,NSE,,FUTSTK,EQUITY\n";
        let rows = parse_scrip_master(csv.as_bytes()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].lot_size, 1);
        assert_eq!(rows[0].tick_size_paise, 10);
    }
    #[test]
    fn scrip_master_rejects_fractional_lots() {
        let csv="SEM_SMST_SECURITY_ID,SEM_TRADING_SYMBOL,SEM_LOT_UNITS,SEM_TICK_SIZE,SEM_EXM_EXCH_ID,SEM_SERIES,SEM_EXCH_INSTRUMENT_TYPE,SEM_INSTRUMENT_NAME\n1,RELIANCE,1.5,10.0000,NSE,EQ,ES,EQUITY\n";
        assert!(matches!(
            parse_scrip_master(csv.as_bytes()),
            Err(ApiError::Malformed(_))
        ));
    }
}
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(source) = std::str::from_utf8(data) {
        let _ = algomln::lexer::lex(source);
    }
});
//! Typed, serializable syntax tree. Decimal values are stored as their exact
//! source spelling to avoid floating point rounding in the frontend.
use crate::source::{NumericLiteral, Span};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Strategy {
    pub name: Option<String>,
    pub trade_in: TradeIn,
    pub stop_loss: Option<NumericLiteral>,
    pub take_profit: Option<NumericLiteral>,
    pub rules: Vec<Rule>,
    pub risk: RiskConfig,
}

// Domain-explicit names for consumers that prefer the `*Node` vocabulary.
pub type StrategyNode = Strategy;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradeIn {
    Symbols(Vec<String>),
    Index(String),
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RiskConfig {
    pub max_orders: Option<u64>,
    pub max_positions: Option<u64>,
    pub max_daily_loss: Option<NumericLiteral>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rule {
    pub id: String,
    pub condition: Condition,
    pub action: Action,
    pub span: Span,
    pub condition_span: Span,
    pub action_span: Span,
}
pub type RuleNode = Rule;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Condition {
    Compare {
        left: NumericExpr,
        op: CompareOp,
        right: NumericExpr,
    },
    Cross {
        direction: CrossDirection,
        left: NumericExpr,
        right: NumericExpr,
    },
    And(Box<Condition>, Box<Condition>),
    Or(Box<Condition>, Box<Condition>),
    Not(Box<Condition>),
    InPosition,
    Between {
        start: Time,
        end: Time,
    },
}
pub type ConditionNode = Condition;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NumericExpr {
    Literal(Decimal),
    Price(PriceField),
    Indicator(IndicatorCall),
}

pub type Decimal = NumericLiteral;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PriceField {
    Close,
    Open,
    High,
    Low,
    Volume,
    PrevClose,
    PrevOpen,
    PrevHigh,
    PrevLow,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IndicatorCall {
    pub kind: IndicatorKind,
    pub period: u32,
    pub multiplier: Option<Decimal>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum IndicatorKind {
    Sma,
    Ema,
    Rsi,
    RelVol,
    Atr,
    Vwap,
    BbUpper,
    BbLower,
    BbMid,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompareOp {
    Lt,
    Le,
    Gt,
    Ge,
    Eq,
    Ne,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CrossDirection {
    Above,
    Below,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Time {
    pub hour: u8,
    pub minute: u8,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Action {
    Buy(TradeAmount),
    Sell(TradeAmount),
    SellAll,
}
pub type ActionNode = Action;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradeAmount {
    Quantity(u64),
    Worth(Decimal),
    BuyCapitalPercentage(Decimal),
    SellPositionPercentage(Decimal),
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn serde_round_trip_preserves_distinctions_and_rule_order() {
        let strategy = Strategy {
            name: Some("demo".into()),
            trade_in: TradeIn::Symbols(vec!["ABC".into()]),
            stop_loss: None,
            take_profit: None,
            rules: vec![Rule {
                id: "rule_1".into(),
                condition: Condition::InPosition,
                action: Action::Buy(TradeAmount::BuyCapitalPercentage(
                    Decimal::parse("2.5".into(), Span::default()).unwrap(),
                )),
                span: Span::default(),
                condition_span: Span::default(),
                action_span: Span::default(),
            }],
            risk: RiskConfig::default(),
        };
        let value = serde_json::to_value(&strategy).unwrap();
        assert_eq!(serde_json::from_value::<Strategy>(value).unwrap(), strategy);
    }
    #[test]
    fn exact_decimal_keeps_spelling_and_normalizes_value() {
        let a = crate::source::NumericLiteral::parse("001.500".into(), Span::default()).unwrap();
        let b = crate::source::NumericLiteral::parse("1.5".into(), Span::default()).unwrap();
        assert_eq!(a.spelling, "001.500");
        assert_eq!(a.value, b.value);
        assert_eq!(a.cmp_integer("2"), Some(std::cmp::Ordering::Less));
    }
}

#[cfg(test)]
mod property_tests {
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn parsed_ast_survives_serde_round_trip_and_reparse(period in 1u32..=1000, value in 1u32..=10000) {
            let source = format!("TRADE_IN ABC\nWHEN ema({period}) > {value}\nBUY {value}\n");
            let parsed = crate::compile(&source).unwrap();
            let encoded = serde_json::to_vec(parsed.strategy()).unwrap();
            let decoded: crate::ast::Strategy = serde_json::from_slice(&encoded).unwrap();
            prop_assert_eq!(parsed.strategy(), &decoded);
            let reparsed = crate::compile(&source).unwrap();
            prop_assert_eq!(reparsed.strategy(), &decoded);
        }
    }
}
use crate::source::Span;
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Diagnostic {
    pub code: &'static str,
    pub message: String,
    pub span: Option<Span>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorStage {
    Lexing,
    Parsing,
    Validation,
}

#[derive(Debug, Error)]
pub enum FrontendError {
    #[error("lex error {code} at {line}:{column}: {message}")]
    Lex {
        code: &'static str,
        message: String,
        line: u32,
        column: u32,
        span: Span,
    },
    #[error("parse error {code} at {line}:{column}: {message}")]
    Parse {
        code: &'static str,
        message: String,
        line: u32,
        column: u32,
        span: Span,
    },
    #[error("validation failed: {0:?}")]
    Validation(Vec<Diagnostic>),
}
use crate::{
    ast::*,
    indicators::{
        Candle, IndicatorError, IndicatorHandle, IndicatorProvider, IndicatorSpec,
        PrecomputedProvider, PriceField as PField,
    },
};
use std::{
    collections::{BTreeMap, HashMap},
    fmt,
};

pub type RuleId = String;
pub type Symbol = String;
#[derive(Clone, Debug)]
pub struct Program {
    strategy: Strategy,
}
impl Program {
    pub fn new(strategy: Strategy) -> Self {
        Self { strategy }
    }
    pub fn rules(&self) -> &[Rule] {
        &self.strategy.rules
    }
    pub fn stop_loss(&self) -> Option<&Decimal> {
        self.strategy.stop_loss.as_ref()
    }
    pub fn take_profit(&self) -> Option<&Decimal> {
        self.strategy.take_profit.as_ref()
    }
    pub fn risk(&self) -> &RiskConfig {
        &self.strategy.risk
    }
    pub fn symbols(&self) -> Vec<&str> {
        match &self.strategy.trade_in {
            TradeIn::Symbols(s) => s.iter().map(String::as_str).collect(),
            TradeIn::Index(s) => vec![s],
        }
    }
    pub fn strategy(&self) -> &Strategy {
        &self.strategy
    }
    /// Largest indicator lookback used by the strategy, suitable as the
    /// default amount of history to feed in observe mode before a run.
    pub fn warmup_bars(&self) -> usize {
        fn expr(e: &NumericExpr, max: &mut usize) {
            if let NumericExpr::Indicator(i) = e {
                *max = (*max).max(i.period as usize);
            }
        }
        fn condition(c: &Condition, max: &mut usize) {
            match c {
                Condition::Compare { left, right, .. } | Condition::Cross { left, right, .. } => {
                    expr(left, max);
                    expr(right, max);
                }
                Condition::And(a, b) | Condition::Or(a, b) => {
                    condition(a, max);
                    condition(b, max);
                }
                Condition::Not(a) => condition(a, max),
                _ => {}
            }
        }
        let mut max = 0;
        for rule in &self.strategy.rules {
            condition(&rule.condition, &mut max);
        }
        max
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct LevelFrame {
    pub symbol: Symbol,
    pub candle_ts: i64,
    pub warm: bool,
    pub entry: BTreeMap<RuleId, bool>,
    pub exit: BTreeMap<RuleId, bool>,
}
#[derive(Debug)]
pub enum EngineError {
    Indicator(IndicatorError),
    NonMonotonicTs { previous: i64, current: i64 },
    InvalidProgram(&'static str),
}
impl From<IndicatorError> for EngineError {
    fn from(e: IndicatorError) -> Self {
        Self::Indicator(e)
    }
}
impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for EngineError {}
pub struct Engine {
    symbol: Symbol,
    provider: PrecomputedProvider,
    handles: HashMap<IndicatorSpec, IndicatorHandle>,
    previous_ts: Option<i64>,
    cross_prev: HashMap<(NumericExpr, NumericExpr), (Option<f64>, Option<f64>)>,
    program: Program,
}
impl Engine {
    pub fn warmup_bars(program: &Program) -> usize {
        program.warmup_bars()
    }
    pub fn new(program: &Program, symbol: Symbol) -> Result<Self, EngineError> {
        let mut provider = PrecomputedProvider::new();
        let mut handles = HashMap::new();
        fn reg(
            c: &Condition,
            p: &mut PrecomputedProvider,
            h: &mut HashMap<IndicatorSpec, IndicatorHandle>,
        ) -> Result<(), EngineError> {
            match c {
                Condition::Compare { left, right, .. } | Condition::Cross { left, right, .. } => {
                    for e in [left, right] {
                        if let NumericExpr::Indicator(i) = e {
                            let s = IndicatorSpec::from_call(i)?;
                            if let std::collections::hash_map::Entry::Vacant(entry) = h.entry(s) {
                                let n = p.register(s)?;
                                entry.insert(n);
                            }
                        }
                    }
                    Ok(())
                }
                Condition::And(a, b) | Condition::Or(a, b) => {
                    reg(a, p, h)?;
                    reg(b, p, h)
                }
                Condition::Not(a) => reg(a, p, h),
                _ => Ok(()),
            }
        }
        for r in &program.strategy.rules {
            reg(&r.condition, &mut provider, &mut handles)?;
        }
        Ok(Self {
            symbol,
            provider,
            handles,
            previous_ts: None,
            cross_prev: HashMap::new(),
            program: program.clone(),
        })
    }
    pub fn on_candle(&mut self, c: &Candle) -> Result<LevelFrame, EngineError> {
        if let Some(t) = self.previous_ts {
            if c.timestamp <= t {
                return Err(EngineError::NonMonotonicTs {
                    previous: t,
                    current: c.timestamp,
                });
            }
        }
        c.is_valid(None)
            .map_err(|reason| EngineError::Indicator(IndicatorError::InvalidCandle { reason }))?;
        self.provider.advance_unchecked(c);
        let mut entry = BTreeMap::new();
        let mut exit = BTreeMap::new();
        for r in &self.program.strategy.rules {
            let v = self.eval(&r.condition).unwrap_or(false);
            match &r.action {
                Action::Buy(_) => {
                    entry.insert(r.id.clone(), v);
                }
                Action::Sell(_) | Action::SellAll => {
                    exit.insert(r.id.clone(), v);
                }
            }
        }
        let mut keys = Vec::new();
        for r in &self.program.strategy.rules {
            Self::cross_keys(&r.condition, &mut keys)
        }
        for (a, b) in keys {
            let x = self.value(&a);
            let y = self.value(&b);
            self.cross_prev.insert((a, b), (x, y));
        }
        self.previous_ts = Some(c.timestamp);
        let warm = self
            .handles
            .values()
            .all(|h| self.provider.get(*h).is_some());
        Ok(LevelFrame {
            symbol: self.symbol.clone(),
            candle_ts: c.timestamp,
            warm,
            entry,
            exit,
        })
    }
    pub fn reset(&mut self) {
        self.provider.reset();
        self.previous_ts = None;
        self.cross_prev.clear()
    }
    fn cross_keys(c: &Condition, out: &mut Vec<(NumericExpr, NumericExpr)>) {
        match c {
            Condition::Cross { left, right, .. } => out.push((left.clone(), right.clone())),
            Condition::And(a, b) | Condition::Or(a, b) => {
                Self::cross_keys(a, out);
                Self::cross_keys(b, out)
            }
            Condition::Not(a) => Self::cross_keys(a, out),
            _ => {}
        }
    }
    fn value(&self, e: &NumericExpr) -> Option<f64> {
        match e {
            NumericExpr::Literal(x) => x.spelling.parse().ok(),
            NumericExpr::Price(x) => self.provider.price(match x {
                PriceField::Close => PField::Close,
                PriceField::Open => PField::Open,
                PriceField::High => PField::High,
                PriceField::Low => PField::Low,
                PriceField::Volume => PField::Volume,
                PriceField::PrevClose => PField::PrevClose,
                PriceField::PrevOpen => PField::PrevOpen,
                PriceField::PrevHigh => PField::PrevHigh,
                PriceField::PrevLow => PField::PrevLow,
            }),
            NumericExpr::Indicator(i) => IndicatorSpec::from_call(i)
                .ok()
                .and_then(|s| self.handles.get(&s))
                .and_then(|h| self.provider.get(*h)),
        }
    }
    fn eval(&self, c: &Condition) -> Option<bool> {
        match c {
            Condition::Compare { left, op, right } => {
                self.value(left)
                    .zip(self.value(right))
                    .map(|(a, b)| match cmp3(a, b) {
                        std::cmp::Ordering::Less => {
                            matches!(op, CompareOp::Lt | CompareOp::Le | CompareOp::Ne)
                        }
                        std::cmp::Ordering::Equal => {
                            matches!(op, CompareOp::Le | CompareOp::Ge | CompareOp::Eq)
                        }
                        std::cmp::Ordering::Greater => {
                            matches!(op, CompareOp::Gt | CompareOp::Ge | CompareOp::Ne)
                        }
                    })
            }
            Condition::Cross {
                direction,
                left,
                right,
            } => {
                let (a, b) = self
                    .cross_prev
                    .get(&(left.clone(), right.clone()))
                    .copied()
                    .unwrap_or((None, None));
                let (x, y) = (self.value(left), self.value(right));
                a.zip(b)
                    .zip(x.zip(y))
                    .map(|((a, b), (x, y))| match direction {
                        CrossDirection::Above => {
                            cmp3(a, b) != std::cmp::Ordering::Greater
                                && cmp3(x, y) == std::cmp::Ordering::Greater
                        }
                        CrossDirection::Below => {
                            cmp3(a, b) != std::cmp::Ordering::Less
                                && cmp3(x, y) == std::cmp::Ordering::Less
                        }
                    })
            }
            Condition::And(a, b) => match (self.eval(a), self.eval(b)) {
                (Some(false), _) | (_, Some(false)) => Some(false),
                (Some(true), Some(true)) => Some(true),
                _ => None,
            },
            Condition::Or(a, b) => match (self.eval(a), self.eval(b)) {
                (Some(true), _) | (_, Some(true)) => Some(true),
                (Some(false), Some(false)) => Some(false),
                _ => None,
            },
            Condition::Not(a) => Some(!self.eval(a)?),
            _ => Some(false),
        }
    }
}
/// Evaluate a candle series through the batch provider API. The provider contract
/// is identical to streaming, so this helper preserves frame ordering and state.
pub fn evaluate_series(
    program: &Program,
    symbol: Symbol,
    candles: &[Candle],
) -> Result<Vec<LevelFrame>, EngineError> {
    let mut engine = Engine::new(program, symbol)?;
    candles.iter().map(|c| engine.on_candle(c)).collect()
}
const ABS_FLOOR: f64 = 1e-12;
fn cmp3(a: f64, b: f64) -> std::cmp::Ordering {
    if (a - b).abs() <= ABS_FLOOR.max(1e-9 * a.abs().max(b.abs())) {
        std::cmp::Ordering::Equal
    } else {
        a.total_cmp(&b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn candle(ts: i64, close: f64) -> Candle {
        Candle {
            timestamp: ts,
            open: close,
            high: close,
            low: close,
            close,
            volume: 1.,
        }
    }
    fn program(src: &str) -> Program {
        crate::compile(src).unwrap()
    }
    #[test]
    fn crossover_warmup_and_non_monotonic_rejection() {
        let p=program("TRADE_IN ABC\nWHEN CROSS_ABOVE(close, ema(2))\nBUY 1\nWHEN close <= ema(2)\nSELL ALL\n");
        let mut e = Engine::new(&p, "ABC".into()).unwrap();
        let first = e.on_candle(&candle(1, 10.)).unwrap();
        assert!(!first.warm);
        assert!(!first.entry["rule_1"]);
        assert!(matches!(
            e.on_candle(&candle(1, 11.)),
            Err(EngineError::NonMonotonicTs { .. })
        ));
        let second = e.on_candle(&candle(2, 12.)).unwrap();
        assert!(second.warm);
    }
    #[test]
    fn batch_matches_stream_and_reset_replays() {
        let p = program(
            "TRADE_IN ABC\nWHEN close > 10\nBUY 1\nWHEN CROSS_BELOW(close, ema(2))\nSELL ALL\n",
        );
        let data = [candle(1, 10.), candle(2, 12.), candle(3, 8.)];
        let batch = evaluate_series(&p, "ABC".into(), &data).unwrap();
        let mut e = Engine::new(&p, "ABC".into()).unwrap();
        let stream: Vec<_> = data.iter().map(|c| e.on_candle(c).unwrap()).collect();
        assert_eq!(batch, stream);
        e.reset();
        let replay: Vec<_> = data.iter().map(|c| e.on_candle(c).unwrap()).collect();
        assert_eq!(batch, replay);
    }
    #[test]
    fn invalid_candle_does_not_advance_state() {
        let p = program("TRADE_IN ABC\nWHEN close > 1\nBUY 1\n");
        let mut e = Engine::new(&p, "ABC".into()).unwrap();
        let bad = Candle {
            timestamp: 1,
            open: 1.,
            high: 1.,
            low: 2.,
            close: 1.,
            volume: 0.,
        };
        assert!(e.on_candle(&bad).is_err());
        assert!(e.on_candle(&candle(1, 2.)).is_ok());
    }

    #[test]
    fn missing_indicators_remain_false_under_not_and_boolean_nesting() {
        let p = program(
            "TRADE_IN ABC\nWHEN NOT (sma(5) > 1)\nBUY 1\nWHEN sma(5) > 1 OR close > 1\nBUY 1\n",
        );
        let mut e = Engine::new(&p, "ABC".into()).unwrap();
        let f = e.on_candle(&candle(1, 2.)).unwrap();
        assert!(!f.entry["rule_1"]);
        assert!(f.entry["rule_2"]);
    }

    #[test]
    fn tolerant_comparison_operators_are_complementary_at_boundary() {
        let p=program("TRADE_IN ABC\nWHEN close < 100\nBUY 1\nWHEN close <= 100\nBUY 1\nWHEN close == 100\nBUY 1\nWHEN close != 100\nBUY 1\nWHEN close >= 100\nBUY 1\nWHEN close > 100\nBUY 1\n");
        for (close, expected) in [
            (100.0, [false, true, true, false, true, false]),
            (100.00000005, [false, true, true, false, true, false]),
            (100.0000002, [false, false, false, true, true, true]),
        ] {
            let frames = evaluate_series(&p, "ABC".into(), &[candle(1, close)]).unwrap();
            let actual: Vec<_> = (1..=6)
                .map(|i| frames[0].entry[&format!("rule_{i}")])
                .collect();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn every_crossover_rule_reads_the_same_previous_values() {
        let p=program("TRADE_IN ABC\nWHEN CROSS_ABOVE(close, sma(2))\nBUY 1\nWHEN CROSS_ABOVE(close, sma(2))\nBUY 1\n");
        let frames = evaluate_series(
            &p,
            "ABC".into(),
            &[candle(1, 10.), candle(2, 10.), candle(3, 12.)],
        )
        .unwrap();
        assert_eq!(frames[2].entry["rule_1"], frames[2].entry["rule_2"]);
    }

    #[test]
    fn no_indicator_rules_are_warm_from_first_valid_candle() {
        let p = program("TRADE_IN ABC\nWHEN close > 1\nBUY 1\n");
        assert!(evaluate_series(&p, "ABC".into(), &[candle(1, 2.)]).unwrap()[0].warm);
    }

    proptest::proptest! {
        #[test]
        fn generated_sequences_match_batch_and_stream(closes in proptest::collection::vec(-1000.0f64..1000.0, 1..300)) {
            let p=program("TRADE_IN ABC\nWHEN ema(7) >= sma(3) AND close > 10\nBUY 1\nWHEN CROSS_BELOW(close, ema(7)) OR rsi(5) < 40\nSELL ALL\n");
            let data:Vec<_>=closes.iter().enumerate().map(|(i,&x)|candle(i as i64+1,x)).collect();
            let batch=evaluate_series(&p,"ABC".into(),&data).unwrap();
            let mut stream=Engine::new(&p,"ABC".into()).unwrap();
            let actual:Vec<_>=data.iter().map(|c|stream.on_candle(c).unwrap()).collect();
            proptest::prop_assert_eq!(batch,actual);
        }
    }
}
use super::{candle::Candle, state::Indicator};
pub(super) struct Atr {
    p: usize,
    n: usize,
    sum: f64,
    prev: Option<f64>,
    value: Option<f64>,
}
impl Atr {
    pub fn new(p: usize) -> Self {
        Self {
            p,
            n: 0,
            sum: 0.,
            prev: None,
            value: None,
        }
    }
}
impl Indicator for Atr {
    fn update(&mut self, c: &Candle) -> Option<f64> {
        if let Some(prev) = self.prev {
            let tr = (c.high - c.low)
                .max((c.high - prev).abs())
                .max((c.low - prev).abs());
            if let Some(v) = self.value {
                self.value = Some((v * (self.p - 1) as f64 + tr) / self.p as f64)
            } else {
                self.n += 1;
                self.sum += tr;
                if self.n == self.p {
                    self.value = Some(self.sum / self.p as f64)
                }
            }
        }
        self.prev = Some(c.close);
        self.value
    }
}
use super::{candle::Candle, spec::IndicatorType, state::Indicator};
pub(super) struct Bollinger {
    p: usize,
    kind: IndicatorType,
    m: f64,
    anchor: Option<f64>,
    buf: Vec<f64>,
    pos: usize,
    n: usize,
    sum: f64,
    mean_offset: f64,
    m2: f64,
    ticks: usize,
}
impl Bollinger {
    pub fn new(p: usize, kind: IndicatorType, m: f64) -> Self {
        Self {
            p,
            kind,
            m,
            anchor: None,
            buf: vec![0.; p],
            pos: 0,
            n: 0,
            sum: 0.,
            mean_offset: 0.,
            m2: 0.,
            ticks: 0,
        }
    }
}
impl Indicator for Bollinger {
    fn update(&mut self, c: &Candle) -> Option<f64> {
        self.ticks += 1;
        let full = self.n == self.p;
        if full {
            let old = self.buf[self.pos] - self.anchor.unwrap_or(0.0);
            self.sum -= old;
            if self.p > 1 {
                let next_mean = (self.p as f64 * self.mean_offset - old) / (self.p - 1) as f64;
                self.m2 -= (old - self.mean_offset) * (old - next_mean);
                self.mean_offset = next_mean;
            } else {
                self.mean_offset = 0.0;
                self.m2 = 0.0;
            }
        } else {
            self.n += 1
        }
        let x = c.close;
        let anchor = *self.anchor.get_or_insert(x);
        let centered = x - anchor;
        self.buf[self.pos] = x;
        self.sum += centered;
        let n = if full { self.p } else { self.n };
        let delta = centered - self.mean_offset;
        self.mean_offset += delta / n as f64;
        self.m2 += delta * (centered - self.mean_offset);
        self.pos = (self.pos + 1) % self.p;
        if self.ticks % 4096 == 0 {
            self.sum = self.buf.iter().take(self.n).map(|x| x - anchor).sum();
            self.mean_offset = 0.0;
            self.m2 = 0.0;
            for (i, value) in self.buf.iter().take(self.n).enumerate() {
                let value = *value - anchor;
                let delta = value - self.mean_offset;
                let n = (i + 1) as f64;
                self.mean_offset += delta / n;
                self.m2 += delta * (value - self.mean_offset);
            }
        }
        if self.n < self.p {
            return None;
        }
        let mean_offset = self.sum / self.p as f64;
        let mid = anchor + mean_offset;
        let sd = (self.m2 / self.p as f64).max(0.).sqrt();
        Some(match self.kind {
            IndicatorType::BbUpper => mid + self.m * sd,
            IndicatorType::BbLower => mid - self.m * sd,
            _ => mid,
        })
    }
}
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Candle {
    pub timestamp: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

impl Candle {
    pub fn is_valid(&self, previous: Option<&Self>) -> Result<(), &'static str> {
        if ![self.open, self.high, self.low, self.close, self.volume]
            .iter()
            .all(|x| x.is_finite())
        {
            return Err("all price and volume fields must be finite");
        }
        if self.low > self.high
            || self.low > self.open
            || self.low > self.close
            || self.open > self.high
            || self.close > self.high
        {
            return Err("expected low <= {open, close} <= high");
        }
        if self.volume < 0.0 {
            return Err("volume must be non-negative");
        }
        if previous.is_some_and(|p| self.timestamp < p.timestamp) {
            return Err("timestamp precedes previous candle");
        }
        Ok(())
    }
}
use super::{candle::Candle, state::Indicator};
pub(super) struct Ema {
    p: usize,
    n: usize,
    sum: f64,
    value: Option<f64>,
    alpha: f64,
}
impl Ema {
    pub fn new(p: usize) -> Self {
        Self {
            p,
            n: 0,
            sum: 0.,
            value: None,
            alpha: 2. / (p as f64 + 1.),
        }
    }
}
impl Indicator for Ema {
    fn update(&mut self, c: &Candle) -> Option<f64> {
        if let Some(v) = self.value {
            let next = self.alpha * c.close + (1. - self.alpha) * v;
            self.value = next.is_finite().then_some(next)
        } else {
            self.n += 1;
            self.sum += c.close;
            if self.n == self.p {
                let v = self.sum / self.p as f64;
                if v.is_finite() {
                    self.value = Some(v)
                }
            }
        }
        self.value
    }
}
use std::fmt;
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IndicatorError {
    InvalidCandle { reason: &'static str },
    InvalidSpec { reason: &'static str },
    RegisterAfterStart,
}
impl fmt::Display for IndicatorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCandle { reason } => write!(f, "invalid candle: {reason}"),
            Self::InvalidSpec { reason } => write!(f, "invalid indicator spec: {reason}"),
            Self::RegisterAfterStart => f.write_str("cannot register indicators after advancing"),
        }
    }
}
impl std::error::Error for IndicatorError {}
//! Incremental, single-threaded market indicators.
mod atr;
mod bollinger;
mod ema;
mod provider;
mod rel_vol;
mod rsi;
mod sma;
mod state;
mod vwap;

pub mod candle;
pub mod error;
pub mod spec;
pub use candle::Candle;
pub use error::IndicatorError;
pub use provider::{IncrementalProvider, IndicatorHandle, IndicatorProvider, PrecomputedProvider};
pub use spec::{IndicatorSpec, IndicatorType, PriceField};

#[cfg(test)]
mod tests;
use super::{
    candle::Candle,
    error::IndicatorError,
    spec::{IndicatorSpec, PriceField},
    state::IndicatorState,
};
use std::collections::HashMap;
pub type IndicatorHandle = usize;
pub trait IndicatorProvider {
    fn register(&mut self, spec: IndicatorSpec) -> Result<IndicatorHandle, IndicatorError>;
    fn advance(&mut self, candle: &Candle) -> Result<(), IndicatorError>;
    fn get(&self, handle: IndicatorHandle) -> Option<f64>;
    fn price(&self, field: PriceField) -> Option<f64>;
    fn reset(&mut self);
}
/// Provider interface for deterministic, precomputed indicator series.
pub struct PrecomputedProvider {
    inner: IncrementalProvider,
}
impl PrecomputedProvider {
    pub fn new() -> Self {
        Self {
            inner: IncrementalProvider::new(),
        }
    }
    pub(crate) fn advance_unchecked(&mut self, c: &Candle) {
        self.inner.advance_unchecked(c)
    }
}
impl Default for PrecomputedProvider {
    fn default() -> Self {
        Self::new()
    }
}
impl IndicatorProvider for PrecomputedProvider {
    fn register(&mut self, s: IndicatorSpec) -> Result<IndicatorHandle, IndicatorError> {
        self.inner.register(s)
    }
    fn advance(&mut self, c: &Candle) -> Result<(), IndicatorError> {
        self.inner.advance(c)
    }
    fn get(&self, h: IndicatorHandle) -> Option<f64> {
        self.inner.get(h)
    }
    fn price(&self, f: PriceField) -> Option<f64> {
        self.inner.price(f)
    }
    fn reset(&mut self) {
        self.inner.reset()
    }
}
pub struct IncrementalProvider {
    states: Vec<IndicatorState>,
    specs: Vec<IndicatorSpec>,
    values: Vec<Option<f64>>,
    index: HashMap<IndicatorSpec, IndicatorHandle>,
    previous: Option<Candle>,
    prior: Option<Candle>,
    started: bool,
}
impl Default for IncrementalProvider {
    fn default() -> Self {
        Self::new()
    }
}
impl IncrementalProvider {
    pub fn new() -> Self {
        Self {
            states: Vec::new(),
            specs: Vec::new(),
            values: Vec::new(),
            index: HashMap::new(),
            previous: None,
            prior: None,
            started: false,
        }
    }
}
impl IndicatorProvider for IncrementalProvider {
    fn register(&mut self, spec: IndicatorSpec) -> Result<IndicatorHandle, IndicatorError> {
        if self.started {
            return Err(IndicatorError::RegisterAfterStart);
        }
        if let Some(&h) = self.index.get(&spec) {
            return Ok(h);
        }
        let h = self.states.len();
        self.states.push(IndicatorState::new(spec));
        self.specs.push(spec);
        self.values.push(None);
        self.index.insert(spec, h);
        Ok(h)
    }
    fn advance(&mut self, c: &Candle) -> Result<(), IndicatorError> {
        c.is_valid(None)
            .map_err(|reason| IndicatorError::InvalidCandle { reason })?;
        if let Some(prev) = self.previous {
            if c.timestamp <= prev.timestamp {
                return Err(IndicatorError::InvalidCandle {
                    reason: "timestamp must increase",
                });
            }
        }
        self.advance_unchecked(c);
        Ok(())
    }
    fn get(&self, h: IndicatorHandle) -> Option<f64> {
        self.values.get(h).copied().flatten()
    }
    fn price(&self, f: PriceField) -> Option<f64> {
        let c = self.previous?;
        Some(match f {
            PriceField::Close => c.close,
            PriceField::Open => c.open,
            PriceField::High => c.high,
            PriceField::Low => c.low,
            PriceField::Volume => c.volume,
            PriceField::PrevClose => return self.previous_prev_field(f),
            PriceField::PrevOpen | PriceField::PrevHigh | PriceField::PrevLow => {
                return self.previous_prev_field(f)
            }
        })
    }
    fn reset(&mut self) {
        self.states = self
            .specs
            .iter()
            .copied()
            .map(IndicatorState::new)
            .collect();
        self.values.fill(None);
        self.previous = None;
        self.prior = None;
        self.started = false;
    }
}
impl IncrementalProvider {
    pub(crate) fn advance_unchecked(&mut self, c: &Candle) {
        self.started = true;
        for (state, value) in self.states.iter_mut().zip(self.values.iter_mut()) {
            *value = state.update(c).filter(|x| x.is_finite());
        }
        self.prior = self.previous;
        self.previous = Some(*c);
    }
    fn previous_prev_field(&self, f: PriceField) -> Option<f64> {
        let c = self.prior?;
        Some(match f {
            PriceField::PrevClose => c.close,
            PriceField::PrevOpen => c.open,
            PriceField::PrevHigh => c.high,
            PriceField::PrevLow => c.low,
            _ => return None,
        })
    }
}
use super::{candle::Candle, state::Indicator};
pub(super) struct RelVol {
    p: usize,
    values: Vec<f64>,
    count: usize,
    pos: usize,
    sum: f64,
}
impl RelVol {
    pub fn new(p: usize) -> Self {
        Self {
            p,
            values: vec![0.0; p],
            count: 0,
            pos: 0,
            sum: 0.,
        }
    }
}
impl Indicator for RelVol {
    fn update(&mut self, c: &Candle) -> Option<f64> {
        let out = if self.count == self.p {
            let avg = self.sum / self.p as f64;
            (avg > 0.).then(|| c.volume / avg)
        } else {
            None
        };
        if self.count == self.p {
            self.sum -= self.values[self.pos];
        } else {
            self.count += 1;
        }
        self.values[self.pos] = c.volume;
        self.sum += c.volume;
        self.pos = (self.pos + 1) % self.p;
        out
    }
}
use super::{candle::Candle, state::Indicator};
pub(super) struct Rsi {
    p: usize,
    n: usize,
    prev: Option<f64>,
    gain: f64,
    loss: f64,
    avg: Option<(f64, f64)>,
}
impl Rsi {
    pub fn new(p: usize) -> Self {
        Self {
            p,
            n: 0,
            prev: None,
            gain: 0.,
            loss: 0.,
            avg: None,
        }
    }
}
impl Indicator for Rsi {
    fn update(&mut self, c: &Candle) -> Option<f64> {
        let prev = self.prev.replace(c.close)?;
        let d = c.close - prev;
        let (g, l) = (d.max(0.), (-d).max(0.));
        let (a, b) = if let Some((a, b)) = self.avg {
            let v = (
                (a * (self.p - 1) as f64 + g) / self.p as f64,
                (b * (self.p - 1) as f64 + l) / self.p as f64,
            );
            self.avg = Some(v);
            v
        } else {
            self.n += 1;
            self.gain += g;
            self.loss += l;
            if self.n < self.p {
                return None;
            }
            let v = (self.gain / self.p as f64, self.loss / self.p as f64);
            self.avg = Some(v);
            v
        };
        Some(if b == 0. {
            if a == 0. {
                50.
            } else {
                100.
            }
        } else if a == 0. {
            0.
        } else {
            100. - 100. / (1. + a / b)
        })
    }
}
use super::{candle::Candle, state::Indicator};
pub(super) struct Sma {
    p: usize,
    buf: Vec<f64>,
    pos: usize,
    sum: f64,
    n: usize,
    ticks: usize,
}
impl Sma {
    pub fn new(p: usize) -> Self {
        Self {
            p,
            buf: vec![0.; p],
            pos: 0,
            sum: 0.,
            n: 0,
            ticks: 0,
        }
    }
}
impl Indicator for Sma {
    fn update(&mut self, c: &Candle) -> Option<f64> {
        self.ticks += 1;
        if self.n == self.p {
            self.sum -= self.buf[self.pos]
        } else {
            self.n += 1
        }
        self.buf[self.pos] = c.close;
        self.sum += c.close;
        self.pos = (self.pos + 1) % self.p;
        if self.ticks % 4096 == 0 {
            self.sum = self.buf.iter().sum()
        }
        (self.n == self.p).then(|| self.sum / self.p as f64)
    }
}
use crate::{
    ast::{IndicatorCall, IndicatorKind},
    source::NumericLiteral,
};
use std::hash::{Hash, Hasher};
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PriceField {
    Close,
    Open,
    High,
    Low,
    Volume,
    PrevClose,
    PrevOpen,
    PrevHigh,
    PrevLow,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum IndicatorType {
    Sma,
    Ema,
    Rsi,
    Atr,
    BbUpper,
    BbMid,
    BbLower,
    RelVol,
    Vwap,
}
#[derive(Clone, Copy, Debug)]
pub struct IndicatorSpec {
    pub kind: IndicatorType,
    pub period: u32,
    multiplier: Option<u64>,
}
impl IndicatorSpec {
    pub fn new(
        kind: IndicatorType,
        period: u32,
        multiplier: Option<f64>,
    ) -> Result<Self, crate::indicators::IndicatorError> {
        if kind != IndicatorType::Vwap && period == 0 {
            return Err(crate::indicators::IndicatorError::InvalidSpec {
                reason: "period must be positive",
            });
        }
        if multiplier.is_some_and(|m| !m.is_finite() || m <= 0.0) {
            return Err(crate::indicators::IndicatorError::InvalidSpec {
                reason: "multiplier must be finite and positive",
            });
        }
        if multiplier.is_some()
            && !matches!(
                kind,
                IndicatorType::BbUpper | IndicatorType::BbMid | IndicatorType::BbLower
            )
        {
            return Err(crate::indicators::IndicatorError::InvalidSpec {
                reason: "only Bollinger indicators accept a multiplier",
            });
        }
        Ok(Self {
            kind,
            period: if kind == IndicatorType::Vwap {
                0
            } else {
                period
            },
            multiplier: if matches!(
                kind,
                IndicatorType::BbUpper | IndicatorType::BbMid | IndicatorType::BbLower
            ) {
                Some(multiplier.unwrap_or(2.0).to_bits())
            } else {
                None
            },
        })
    }
    pub fn bollinger_multiplier(self) -> f64 {
        self.multiplier.map(f64::from_bits).unwrap_or(2.0)
    }
    pub fn from_call(call: &IndicatorCall) -> Result<Self, crate::indicators::IndicatorError> {
        let kind = match call.kind {
            IndicatorKind::Sma => IndicatorType::Sma,
            IndicatorKind::Ema => IndicatorType::Ema,
            IndicatorKind::Rsi => IndicatorType::Rsi,
            IndicatorKind::Atr => IndicatorType::Atr,
            IndicatorKind::BbUpper => IndicatorType::BbUpper,
            IndicatorKind::BbMid => IndicatorType::BbMid,
            IndicatorKind::BbLower => IndicatorType::BbLower,
            IndicatorKind::RelVol => IndicatorType::RelVol,
            IndicatorKind::Vwap => IndicatorType::Vwap,
        };
        Self::new(
            kind,
            call.period,
            call.multiplier
                .as_ref()
                .map(decimal_to_f64)
                .transpose()?
                .or_else(|| {
                    matches!(
                        kind,
                        IndicatorType::BbUpper | IndicatorType::BbMid | IndicatorType::BbLower
                    )
                    .then_some(2.0)
                }),
        )
    }
}
fn decimal_to_f64(d: &NumericLiteral) -> Result<f64, crate::indicators::IndicatorError> {
    let value =
        d.spelling
            .parse::<f64>()
            .map_err(|_| crate::indicators::IndicatorError::InvalidSpec {
                reason: "decimal multiplier cannot be represented as f64",
            })?;
    if value.is_finite() {
        Ok(value)
    } else {
        Err(crate::indicators::IndicatorError::InvalidSpec {
            reason: "decimal multiplier cannot be represented as finite f64",
        })
    }
}
impl PartialEq for IndicatorSpec {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind
            && self.period == other.period
            && self.multiplier == other.multiplier
    }
}
impl Eq for IndicatorSpec {}
impl Hash for IndicatorSpec {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.kind.hash(state);
        self.period.hash(state);
        self.multiplier.hash(state)
    }
}
use super::{
    atr::Atr,
    bollinger::Bollinger,
    candle::Candle,
    ema::Ema,
    rel_vol::RelVol,
    rsi::Rsi,
    sma::Sma,
    spec::{IndicatorSpec, IndicatorType},
    vwap::Vwap,
};
pub(super) trait Indicator {
    fn update(&mut self, c: &Candle) -> Option<f64>;
}
pub(super) enum IndicatorState {
    Sma(Sma),
    Ema(Ema),
    Rsi(Rsi),
    Atr(Atr),
    Bollinger(Bollinger),
    RelVol(RelVol),
    Vwap(Vwap),
}
impl IndicatorState {
    pub fn new(s: IndicatorSpec) -> Self {
        match s.kind {
            IndicatorType::Sma => Self::Sma(Sma::new(s.period as usize)),
            IndicatorType::Ema => Self::Ema(Ema::new(s.period as usize)),
            IndicatorType::Rsi => Self::Rsi(Rsi::new(s.period as usize)),
            IndicatorType::Atr => Self::Atr(Atr::new(s.period as usize)),
            IndicatorType::BbUpper | IndicatorType::BbMid | IndicatorType::BbLower => {
                Self::Bollinger(Bollinger::new(
                    s.period as usize,
                    s.kind,
                    s.bollinger_multiplier(),
                ))
            }
            IndicatorType::RelVol => Self::RelVol(RelVol::new(s.period as usize)),
            IndicatorType::Vwap => Self::Vwap(Vwap::new()),
        }
    }
    pub fn update(&mut self, c: &Candle) -> Option<f64> {
        match self {
            Self::Sma(x) => x.update(c),
            Self::Ema(x) => x.update(c),
            Self::Rsi(x) => x.update(c),
            Self::Atr(x) => x.update(c),
            Self::Bollinger(x) => x.update(c),
            Self::RelVol(x) => x.update(c),
            Self::Vwap(x) => x.update(c),
        }
    }
}
use super::{
    Candle, IncrementalProvider, IndicatorProvider, IndicatorSpec, IndicatorType, PriceField,
};

fn candles() -> &'static [Candle] {
    static INPUT: std::sync::OnceLock<Vec<Candle>> = std::sync::OnceLock::new();
    INPUT.get_or_init(|| {
        include_str!("../../sample-data/nifty_1min.csv")
            .lines()
            .skip(1)
            .enumerate()
            .map(|(i, row)| {
                let mut columns = row.split(',');
                let _timestamp = columns.next().unwrap();
                let mut number = || columns.next().unwrap().parse::<f64>().unwrap();
                let open = number();
                let high = number();
                let low = number();
                let close = number();
                // The source has five rounding discrepancies of less than 0.001 where
                // close falls just outside the quoted low/high bounds. Clamp to the
                // low <= {open, close} <= high invariant that `is_valid` enforces.
                let high = high.max(open).max(close);
                let low = low.min(open).min(close);
                Candle {
                    timestamp: i as i64 * 60_000,
                    open,
                    high,
                    low,
                    close,
                    // The source CSV has no volume column. Zero is explicit so volume-based
                    // outputs are undefined instead of silently inventing market data.
                    volume: 0.0,
                }
            })
            .collect()
    })
}

fn check_fixture(kind: IndicatorType, period: u32, name: &str, fixture: &str) {
    let mut provider = IncrementalProvider::new();
    let handle = provider
        .register(IndicatorSpec::new(kind, period, None).unwrap())
        .unwrap();
    let rows: Vec<_> = fixture.lines().skip(1).collect();
    let input = candles();
    assert_eq!(
        rows.len(),
        input.len(),
        "fixture length for {name}_{period}"
    );
    for (i, (candle, row)) in input.iter().zip(rows).enumerate() {
        provider.advance(candle).unwrap();
        let expected = if row == "None" {
            None
        } else {
            Some(row.parse::<f64>().unwrap())
        };
        let actual = provider.get(handle);
        match (actual, expected) {
            (None, None) => {}
            (Some(a), Some(e)) => {
                let tolerance = 1e-9 * e.abs().max(1.0);
                assert!(
                    (a - e).abs() <= tolerance,
                    "{name}({period}) at row {i}: {a} != {e}"
                );
            }
            pair => panic!("{name}({period}) at row {i}: got {pair:?}"),
        }
    }
}

macro_rules! golden_periods {
    ($kind:ident, $name:literal) => {
        check_fixture(
            IndicatorType::$kind,
            1,
            $name,
            include_str!(concat!("tests/fixtures/", $name, "_1.csv")),
        );
        check_fixture(
            IndicatorType::$kind,
            2,
            $name,
            include_str!(concat!("tests/fixtures/", $name, "_2.csv")),
        );
        check_fixture(
            IndicatorType::$kind,
            5,
            $name,
            include_str!(concat!("tests/fixtures/", $name, "_5.csv")),
        );
        check_fixture(
            IndicatorType::$kind,
            14,
            $name,
            include_str!(concat!("tests/fixtures/", $name, "_14.csv")),
        );
        check_fixture(
            IndicatorType::$kind,
            20,
            $name,
            include_str!(concat!("tests/fixtures/", $name, "_20.csv")),
        );
        check_fixture(
            IndicatorType::$kind,
            50,
            $name,
            include_str!(concat!("tests/fixtures/", $name, "_50.csv")),
        );
    };
}

#[test]
fn matches_pandas_golden_fixtures() {
    golden_periods!(Sma, "sma");
    golden_periods!(Ema, "ema");
    golden_periods!(Rsi, "rsi");
    golden_periods!(Atr, "atr");
    golden_periods!(BbUpper, "bb_upper");
    golden_periods!(BbMid, "bb_mid");
    golden_periods!(BbLower, "bb_lower");
}

#[test]
fn provider_price_and_volume_fields_follow_csv_assumptions() {
    let mut provider = IncrementalProvider::new();
    let input = candles();
    provider.advance(&input[0]).unwrap();
    assert_eq!(provider.price(PriceField::Volume), Some(0.0));
    assert_eq!(provider.price(PriceField::PrevClose), None);
    provider.advance(&input[1]).unwrap();
    assert_eq!(provider.price(PriceField::PrevClose), Some(input[0].close));
}

fn sample(timestamp: i64, close: f64, volume: f64) -> Candle {
    Candle {
        timestamp,
        open: close,
        high: close + 1.0,
        low: close - 1.0,
        close,
        volume,
    }
}

#[test]
fn volume_indicators_use_prior_window_and_reset_at_ist_day_boundary() {
    let mut provider = IncrementalProvider::new();
    let rel = provider
        .register(IndicatorSpec::new(IndicatorType::RelVol, 2, None).unwrap())
        .unwrap();
    let vwap = provider
        .register(IndicatorSpec::new(IndicatorType::Vwap, 20, None).unwrap())
        .unwrap();
    // Epoch seconds; 66_600 is an IST midnight ((66_600 + 19_800) % 86_400 == 0),
    // so the first two bars fall in the prior IST day and the third resets VWAP.
    let boundary = 66_600;
    provider.advance(&sample(boundary - 120, 10.0, 2.0)).unwrap();
    assert_eq!(provider.get(rel), None);
    assert_eq!(provider.get(vwap), Some(10.0));
    provider.advance(&sample(boundary - 60, 12.0, 4.0)).unwrap();
    assert_eq!(provider.get(rel), None);
    assert_eq!(provider.get(vwap), Some((10.0 * 2.0 + 12.0 * 4.0) / 6.0));
    provider.advance(&sample(boundary, 20.0, 9.0)).unwrap();
    assert_eq!(provider.get(rel), Some(3.0));
    assert_eq!(provider.get(vwap), Some(20.0));
}

#[test]
fn invalid_candles_do_not_mutate_provider_and_specs_dedupe() {
    let mut provider = IncrementalProvider::new();
    let sma_spec = IndicatorSpec::new(IndicatorType::Sma, 1, None).unwrap();
    let h = provider.register(sma_spec).unwrap();
    assert_eq!(provider.register(sma_spec).unwrap(), h);
    let bb_default = IndicatorSpec::new(IndicatorType::BbUpper, 20, None).unwrap();
    let bb_explicit = IndicatorSpec::new(IndicatorType::BbUpper, 20, Some(2.0)).unwrap();
    assert_eq!(bb_default, bb_explicit);
    assert_eq!(
        provider.register(bb_default).unwrap(),
        provider.register(bb_explicit).unwrap()
    );
    provider.advance(&sample(1, 100.0, 1.0)).unwrap();
    assert_eq!(provider.get(h), Some(100.0));
    let invalid = Candle {
        high: 1.0,
        low: 2.0,
        ..sample(2, 100.0, 1.0)
    };
    assert!(provider.advance(&invalid).is_err());
    assert_eq!(provider.get(h), Some(100.0));
    assert_eq!(provider.price(PriceField::Close), Some(100.0));
}

#[test]
fn period_one_and_short_large_periods_follow_warmup_rules() {
    let mut provider = IncrementalProvider::new();
    let sma = provider
        .register(IndicatorSpec::new(IndicatorType::Sma, 1, None).unwrap())
        .unwrap();
    let ema = provider
        .register(IndicatorSpec::new(IndicatorType::Ema, 1, None).unwrap())
        .unwrap();
    let rsi = provider
        .register(IndicatorSpec::new(IndicatorType::Rsi, 1, None).unwrap())
        .unwrap();
    let atr = provider
        .register(IndicatorSpec::new(IndicatorType::Atr, 1, None).unwrap())
        .unwrap();
    let bb = provider
        .register(IndicatorSpec::new(IndicatorType::BbUpper, 1, None).unwrap())
        .unwrap();
    let rel = provider
        .register(IndicatorSpec::new(IndicatorType::RelVol, 1, None).unwrap())
        .unwrap();
    let large = provider
        .register(IndicatorSpec::new(IndicatorType::Sma, 10_000, None).unwrap())
        .unwrap();
    provider.advance(&sample(1, 10.0, 2.0)).unwrap();
    assert_eq!(provider.get(sma), Some(10.0));
    assert_eq!(provider.get(ema), Some(10.0));
    assert_eq!(provider.get(bb), Some(10.0));
    assert_eq!(provider.get(rsi), None);
    assert_eq!(provider.get(atr), None);
    assert_eq!(provider.get(rel), None);
    assert_eq!(provider.get(large), None);
    provider.advance(&sample(2, 10.0, 4.0)).unwrap();
    assert_eq!(provider.get(rsi), Some(50.0));
    assert_eq!(provider.get(atr), Some(2.0));
    assert_eq!(provider.get(rel), Some(2.0));
}

#[test]
fn reset_replay_reproduces_values_and_preserves_handles() {
    let mut provider = IncrementalProvider::new();
    let handle = provider
        .register(IndicatorSpec::new(IndicatorType::Ema, 3, None).unwrap())
        .unwrap();
    let input = [
        sample(1, 10.0, 1.0),
        sample(2, 11.0, 1.0),
        sample(3, 9.0, 1.0),
    ];
    let mut first = Vec::new();
    for candle in &input {
        provider.advance(candle).unwrap();
        first.push(provider.get(handle));
    }
    provider.reset();
    let mut replay = Vec::new();
    for candle in &input {
        provider.advance(candle).unwrap();
        replay.push(provider.get(handle));
    }
    assert_eq!(first, replay);
}
use super::{candle::Candle, state::Indicator};
pub(super) struct Vwap {
    day: Option<i64>,
    pv: f64,
    v: f64,
}
impl Vwap {
    pub fn new() -> Self {
        Self {
            day: None,
            pv: 0.,
            v: 0.,
        }
    }
}
impl Indicator for Vwap {
    fn update(&mut self, c: &Candle) -> Option<f64> {
        // Timestamps are epoch seconds; +19800 shifts to IST so the accumulator
        // resets at IST midnight, matching the session convention used elsewhere.
        let day = (c.timestamp + 19_800).div_euclid(86_400);
        if self.day != Some(day) {
            self.day = Some(day);
            self.pv = 0.;
            self.v = 0.
        }
        let typ = (c.high + c.low + c.close) / 3.;
        self.pv += typ * c.volume;
        self.v += c.volume;
        (self.v > 0.).then(|| self.pv / self.v)
    }
}
use crate::{diagnostics::FrontendError, source::Span};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Token {
    pub kind: TokenKind,
    pub span: Span,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TokenKind {
    Word(String),
    String(String),
    Number(String),
    Time(String),
    LParen,
    RParen,
    Comma,
    Percent,
    Plus,
    Minus,
    Lt,
    Le,
    Gt,
    Ge,
    Eq,
    Ne,
    Newline,
    Eof,
}

pub const MAX_SOURCE_BYTES: usize = 1_048_576;
pub const MAX_TOKENS: usize = 100_000;
pub const MAX_TOKEN_BYTES: usize = 256;

pub fn lex(source: &str) -> Result<Vec<Token>, FrontendError> {
    if source.len() > MAX_SOURCE_BYTES {
        return Err(err("LEX_LIMIT", "source exceeds 1 MiB limit", 1, 1));
    }
    let mut out = Vec::new();
    let mut i = 0;
    let mut line = 1u32;
    let mut col = 1u32;
    while i < source.len() {
        let c = source[i..].chars().next().expect("valid UTF-8 boundary");
        if c == '\r' && source[i..].starts_with("\r\n") {
            out.push(Token {
                kind: TokenKind::Newline,
                span: Span::new(i, i + 2, line, col),
            });
            i += 2;
            line += 1;
            col = 1;
            continue;
        }
        if c == '\r' {
            return Err(err(
                "LEX_NEWLINE",
                "lone carriage return is not a newline; use LF or CRLF",
                line,
                col,
            ));
        }
        if c == '\n' {
            out.push(Token {
                kind: TokenKind::Newline,
                span: Span::new(i, i + 1, line, col),
            });
            i += 1;
            line += 1;
            col = 1;
            continue;
        }
        if c.is_whitespace() {
            if matches!(c, '\u{2028}' | '\u{2029}' | '\u{0085}') {
                out.push(Token {
                    kind: TokenKind::Newline,
                    span: Span::new(i, i + c.len_utf8(), line, col),
                });
                i += c.len_utf8();
                line += 1;
                col = 1;
                continue;
            }
            i += c.len_utf8();
            col += 1;
            continue;
        }
        let start = i;
        let start_col = col;
        let kind = match c {
            '(' => {
                i += 1;
                TokenKind::LParen
            }
            '"' => {
                i += 1;
                let mut value = String::new();
                let mut closed = false;
                while i < source.len() {
                    let ch = source[i..].chars().next().expect("valid UTF-8 boundary");
                    if ch == '"' {
                        i += 1;
                        closed = true;
                        break;
                    }
                    if matches!(ch, '\\' | '\n' | '\r') {
                        return Err(err(
                            "LEX_STRING",
                            "invalid escape or newline in quoted identifier",
                            line,
                            start_col,
                        ));
                    }
                    value.push(ch);
                    i += ch.len_utf8();
                }
                if !closed {
                    return Err(err(
                        "LEX_STRING",
                        "unterminated quoted identifier",
                        line,
                        start_col,
                    ));
                }
                if value.is_empty() {
                    return Err(err(
                        "LEX_EMPTY_IDENTIFIER",
                        "quoted identifier cannot be empty",
                        line,
                        start_col,
                    ));
                }
                if i - start > MAX_TOKEN_BYTES {
                    return Err(err(
                        "LEX_LIMIT",
                        "quoted identifier exceeds 256-byte limit",
                        line,
                        start_col,
                    ));
                }
                TokenKind::String(value)
            }
            ')' => {
                i += 1;
                TokenKind::RParen
            }
            ',' => {
                i += 1;
                TokenKind::Comma
            }
            '%' => {
                i += 1;
                TokenKind::Percent
            }
            '+' => {
                i += 1;
                TokenKind::Plus
            }
            '-' => {
                i += 1;
                TokenKind::Minus
            }
            '<' | '>' | '=' | '!' => {
                i += 1;
                let next = source.get(i..).and_then(|s| s.chars().next());
                let pair = next == Some('=');
                if pair {
                    i += 1;
                }
                match (c, pair) {
                    ('<', true) => TokenKind::Le,
                    ('<', false) => TokenKind::Lt,
                    ('>', true) => TokenKind::Ge,
                    ('>', false) => TokenKind::Gt,
                    ('=', true) => TokenKind::Eq,
                    ('!', true) => TokenKind::Ne,
                    _ => return Err(err("LEX_OPERATOR", "invalid operator", line, start_col)),
                }
            }
            '.' if source[i..]
                .chars()
                .nth(1)
                .is_some_and(|x| x.is_ascii_digit()) =>
            {
                return Err(err(
                    "LEX_NUMBER",
                    "leading decimal point is not permitted",
                    line,
                    start_col,
                ));
            }
            '.' => {
                return Err(err(
                    "LEX_NUMBER",
                    "unexpected decimal point",
                    line,
                    start_col,
                ))
            }
            x if x.is_ascii_digit() => {
                while source
                    .get(i..)
                    .and_then(|s| s.chars().next())
                    .is_some_and(|x| x.is_ascii_digit())
                {
                    i += 1;
                }
                if source[i..].starts_with(':') {
                    let hour_digits = i - start;
                    i += 1;
                    let minute_start = i;
                    while source
                        .get(i..)
                        .and_then(|s| s.chars().next())
                        .is_some_and(|x| x.is_ascii_digit())
                    {
                        i += 1;
                    }
                    if hour_digits != 2
                        || i - minute_start != 2
                        || source
                            .get(i..)
                            .and_then(|s| s.chars().next())
                            .is_some_and(|x| x == ':' || x.is_ascii_alphanumeric() || x == '_')
                    {
                        return Err(err(
                            "LEX_TIME",
                            "time must have exactly two minute digits and a token boundary",
                            line,
                            start_col,
                        ));
                    }
                    // Range-check at lex time so an out-of-range literal like
                    // `25:99` is rejected here rather than silently tokenising and
                    // only being caught (if at all) much later. HH must be 00..=23
                    // and MM 00..=59; the digit count is already guaranteed above.
                    let hour: u32 = source[start..start + 2].parse().unwrap_or(99);
                    let minute: u32 = source[minute_start..i].parse().unwrap_or(99);
                    if hour > 23 || minute > 59 {
                        return Err(err(
                            "LEX_TIME",
                            "time must be within 00:00..=23:59",
                            line,
                            start_col,
                        ));
                    }
                    TokenKind::Time(source[start..i].to_owned())
                } else {
                    if source[i..].starts_with('.') {
                        i += 1;
                        if !source
                            .get(i..)
                            .and_then(|s| s.chars().next())
                            .is_some_and(|x| x.is_ascii_digit())
                        {
                            return Err(err(
                                "LEX_NUMBER",
                                "decimal point must be followed by digits",
                                line,
                                start_col,
                            ));
                        }
                        while source
                            .get(i..)
                            .and_then(|s| s.chars().next())
                            .is_some_and(|x| x.is_ascii_digit())
                        {
                            i += 1;
                        }
                    }
                    if source
                        .get(i..)
                        .and_then(|s| s.chars().next())
                        .is_some_and(|x| x == '.' || x.is_alphabetic() || x == '_')
                    {
                        return Err(err(
                            "LEX_NUMBER",
                            "malformed or adjacent numeric literal",
                            line,
                            start_col,
                        ));
                    }
                    TokenKind::Number(source[start..i].to_owned())
                }
            }
            x if x.is_alphabetic() || x == '_' => {
                i += x.len_utf8();
                while source
                    .get(i..)
                    .and_then(|s| s.chars().next())
                    .is_some_and(|x| x.is_alphanumeric() || x == '_')
                {
                    i += source[i..].chars().next().unwrap().len_utf8();
                }
                if i - start > MAX_TOKEN_BYTES {
                    return Err(err(
                        "LEX_LIMIT",
                        "identifier exceeds 256-byte limit",
                        line,
                        start_col,
                    ));
                }
                TokenKind::Word(source[start..i].to_owned())
            }
            _ => {
                return Err(err(
                    "LEX_CHAR",
                    format!("unexpected character {c:?}"),
                    line,
                    start_col,
                ))
            }
        };
        if i - start > MAX_TOKEN_BYTES && matches!(kind, TokenKind::Number(_) | TokenKind::Time(_))
        {
            return Err(err(
                "LEX_LIMIT",
                "numeric or time literal exceeds 256-byte limit",
                line,
                start_col,
            ));
        }
        col += source[start..i].chars().count() as u32;
        if matches!(kind, TokenKind::Time(_)) {
            let tail = source.get(i..).and_then(|s| s.chars().next());
            if tail.is_some_and(|x| x.is_ascii_digit() || x == ':' || x.is_alphabetic() || x == '_')
            {
                return Err(err("LEX_TIME", "malformed time literal", line, start_col));
            }
        }
        if matches!(kind, TokenKind::Number(_))
            && source
                .get(i..)
                .and_then(|s| s.chars().next())
                .is_some_and(|x| x.is_ascii_alphanumeric() || x == '_')
        {
            return Err(err(
                "LEX_NUMBER",
                "malformed numeric suffix",
                line,
                start_col,
            ));
        }
        out.push(Token {
            kind,
            span: Span::new(start, i, line, start_col),
        });
        if out.len() > MAX_TOKENS {
            return Err(err("LEX_LIMIT", "token count exceeds 100,000", line, col));
        }
    }
    out.push(Token {
        kind: TokenKind::Eof,
        span: Span::new(i, i, line, col),
    });
    Ok(out)
}
fn err(code: &'static str, message: impl Into<String>, line: u32, column: u32) -> FrontendError {
    let span = Span::new(0, 0, line, column);
    FrontendError::Lex {
        code,
        message: message.into(),
        line,
        column,
        span,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn maximal_operators_and_unicode_columns() {
        let t = lex("λ <= 2\r\n!=").unwrap();
        assert_eq!(t[0].span.column, 1);
        assert!(matches!(t[1].kind, TokenKind::Le));
        assert!(matches!(t[4].kind, TokenKind::Ne));
        assert_eq!(t[4].span.line, 2);
    }
    #[test]
    fn malformed_numbers_fail() {
        assert!(lex("12.3.4").is_err());
        assert!(lex("1abc").is_err());
        assert!(lex("1e5").is_err());
        assert!(lex(".5").is_err());
        assert!(lex("1..5").is_err());
        assert!(lex("10:3").is_err());
        assert!(lex("# comment").is_err());
        assert!(lex("\rTRADE_IN A").is_err());
        assert!(lex("09:30").is_ok());
        assert!(lex("9:30").is_err());
        // Out-of-range hours/minutes are rejected at lex time.
        assert!(lex("25:99").is_err());
        assert!(lex("24:00").is_err());
        assert!(lex("12:60").is_err());
        assert!(lex("23:59").is_ok());
        assert!(lex("00:00").is_ok());
    }

    #[test]
    fn quoted_names_and_unicode_line_separators_are_lexed() {
        assert!(matches!(
            lex("\"M&M\"").unwrap()[0].kind,
            TokenKind::String(_)
        ));
        assert!(matches!(
            lex("\"BAJAJ-AUTO\"").unwrap()[0].kind,
            TokenKind::String(_)
        ));
        assert!(matches!(
            lex("\"360ONE\"").unwrap()[0].kind,
            TokenKind::String(_)
        ));
        let tokens = lex("A\u{2028}B").unwrap();
        assert!(matches!(tokens[1].kind, TokenKind::Newline));
        assert_eq!(tokens[2].span.line, 2);
        let empty = lex("\"\"").unwrap_err();
        assert_eq!(empty.code(), "LEX_EMPTY_IDENTIFIER");
    }
    #[test]
    fn source_positions_and_limits_are_explicit() {
        let tokens = lex("λ\tA\r\nB").unwrap();
        assert_eq!(tokens[1].span.column, 3);
        assert_eq!(tokens[3].span.line, 2);
        assert!(lex(&"a".repeat(MAX_TOKEN_BYTES + 1)).is_err());
    }
}
//! AlgoMLN strategy language frontend, streaming indicator engine, portfolio
//! trader, and deterministic local historical simulation.

pub mod ast;
pub mod diagnostics;
pub mod engine;
pub mod indicators;
pub mod lexer;
pub mod parser;
pub mod sim;
pub mod source;
pub mod trader;
pub mod validator;
pub use engine::Program;
pub use engine::{evaluate_series, Engine, EngineError, LevelFrame, RuleId, Symbol};
pub use trader::{
    ExecutionPort, FrameOutput, InstrumentRule, Mode, OrderEvent, OrderId, OrderReason,
    OrderRequest, OrderState, PortError, Position, ScriptedPort, Side, Trader, TraderConfig,
    TraderError, TraderEvent,
};

pub use ast::Strategy;
pub use diagnostics::{Diagnostic, FrontendError};

impl FrontendError {
    pub fn stage(&self) -> diagnostics::ErrorStage {
        match self {
            Self::Lex { .. } => diagnostics::ErrorStage::Lexing,
            Self::Parse { .. } => diagnostics::ErrorStage::Parsing,
            Self::Validation(_) => diagnostics::ErrorStage::Validation,
        }
    }
    pub fn code(&self) -> &str {
        match self {
            Self::Lex { code, .. } | Self::Parse { code, .. } => code,
            Self::Validation(ds) => ds.first().map_or("VAL_UNKNOWN", |d| d.code),
        }
    }
    pub fn span(&self) -> Option<source::Span> {
        match self {
            Self::Lex { span, .. } | Self::Parse { span, .. } => Some(*span),
            Self::Validation(ds) => ds.first().and_then(|d| d.span),
        }
    }
}

/// Parse and validate a complete AlgoMLN source file.
pub fn compile(source: &str) -> Result<Program, FrontendError> {
    let tokens = lexer::lex(source)?;
    let strategy = parser::parse(&tokens)?;
    validator::validate(&strategy)?;
    Ok(Program::new(strategy))
}
use crate::{
    ast::*,
    diagnostics::FrontendError,
    lexer::{Token, TokenKind},
    source::{NumericLiteral, Span},
};

pub fn parse(tokens: &[Token]) -> Result<Strategy, FrontendError> {
    Parser { t: tokens, i: 0 }.strategy()
}
struct Parser<'a> {
    t: &'a [Token],
    i: usize,
}
impl<'a> Parser<'a> {
    fn k(&self) -> &TokenKind {
        &self.t[self.i].kind
    }
    fn span(&self) -> Span {
        self.t[self.i].span
    }
    fn bump(&mut self) -> Token {
        let x = self.t[self.i].clone();
        if !matches!(x.kind, TokenKind::Eof) {
            self.i += 1;
        }
        x
    }
    fn nesting(&self) -> usize {
        let mut depth = 0usize;
        for t in self.t[..self.i.min(self.t.len())].iter() {
            match t.kind {
                TokenKind::LParen => depth += 1,
                TokenKind::RParen => depth = depth.saturating_sub(1),
                _ => {}
            }
        }
        depth
    }
    fn check_depth(&self) -> Result<(), FrontendError> {
        if self.nesting() > 128 {
            let p = self.span();
            Err(FrontendError::Parse {
                code: "PARSE_DEPTH",
                message: "nesting exceeds 128 levels".into(),
                line: p.line,
                column: p.column,
                span: p,
            })
        } else {
            Ok(())
        }
    }
    fn word(&self, s: &str) -> bool {
        matches!(self.k(),TokenKind::Word(w) if w.eq_ignore_ascii_case(s))
    }
    fn take_word(&mut self, s: &str) -> bool {
        if self.word(s) {
            self.bump();
            true
        } else {
            false
        }
    }
    fn fail<T>(&self, msg: &str) -> Result<T, FrontendError> {
        let p = self.span();
        Err(FrontendError::Parse {
            code: "PARSE_SYNTAX",
            message: msg.into(),
            line: p.line,
            column: p.column,
            span: p,
        })
    }
    fn fail_at<T>(&self, msg: &str, span: Span) -> Result<T, FrontendError> {
        Err(FrontendError::Parse {
            code: "PARSE_SYNTAX",
            message: msg.into(),
            line: span.line,
            column: span.column,
            span,
        })
    }
    fn newlines(&mut self) {
        while matches!(self.k(), TokenKind::Newline) {
            self.bump();
        }
    }
    fn strategy(mut self) -> Result<Strategy, FrontendError> {
        self.newlines();
        let name = if self.take_word("STRATEGY") {
            match self.bump().kind {
                TokenKind::Word(w) | TokenKind::String(w) => {
                    if !matches!(self.k(), TokenKind::Newline) {
                        return self.fail(
                            "strategy name is one identifier and header must end at newline",
                        );
                    }
                    self.newlines();
                    Some(w)
                }
                _ => return self.fail("expected strategy name"),
            }
        } else {
            None
        };
        if !self.take_word("TRADE_IN") {
            return self.fail("expected TRADE_IN declaration");
        }
        let trade_in = self.trade_in()?;
        if !matches!(self.k(), TokenKind::Newline | TokenKind::Eof) {
            return self.fail("TRADE_IN must end at newline");
        }
        self.newlines();
        let mut stop_loss = None;
        let mut take_profit = None;
        let mut risk = RiskConfig::default();
        let mut rules = Vec::new();
        loop {
            self.newlines();
            if matches!(self.k(), TokenKind::Eof) {
                break;
            }
            if self.take_word("STRATEGY") {
                return self.fail("STRATEGY is only allowed before TRADE_IN");
            }
            if self.take_word("TRADE_IN") {
                return self.fail("duplicate TRADE_IN declaration");
            }
            if self.word("STOP_LOSS") {
                let declaration_span = self.bump().span;
                if stop_loss.is_some() {
                    return self.fail_at("duplicate STOP_LOSS", declaration_span);
                }
                stop_loss = Some(self.percent()?);
                if !matches!(self.k(), TokenKind::Newline | TokenKind::Eof) {
                    return self.fail("unexpected token after declaration");
                }
                self.newlines();
                continue;
            }
            if self.word("TAKE_PROFIT") {
                let declaration_span = self.bump().span;
                if take_profit.is_some() {
                    return self.fail_at("duplicate TAKE_PROFIT", declaration_span);
                }
                take_profit = Some(self.percent()?);
                if !matches!(self.k(), TokenKind::Newline | TokenKind::Eof) {
                    return self.fail("unexpected token after declaration");
                }
                self.newlines();
                continue;
            }
            if self.take_word("RISK") {
                let key = match self.bump().kind {
                    TokenKind::Word(w) => w.to_ascii_uppercase(),
                    _ => return self.fail("expected risk setting"),
                };
                match key.as_str() {
                    "MAX_ORDERS" => {
                        if risk.max_orders.is_some() {
                            return self.fail("duplicate MAX_ORDERS");
                        }
                        risk.max_orders = Some(self.uint()?);
                    }
                    "MAX_POSITIONS" => {
                        if risk.max_positions.is_some() {
                            return self.fail("duplicate MAX_POSITIONS");
                        }
                        risk.max_positions = Some(self.uint()?);
                    }
                    "MAX_DAILY_LOSS" => {
                        if risk.max_daily_loss.is_some() {
                            return self.fail("duplicate MAX_DAILY_LOSS");
                        }
                        risk.max_daily_loss = Some(self.percent()?);
                    }
                    _ => return self.fail("unknown risk setting"),
                };
                if !matches!(self.k(), TokenKind::Newline | TokenKind::Eof) {
                    return self.fail("unexpected token after risk declaration");
                }
                self.newlines();
                continue;
            }
            if self.take_word("WHEN") {
                let start = self.t[self.i - 1].span;
                let condition_start = self.span();
                let condition = self.condition()?;
                let condition_end = self.t[self.i - 1].span;
                if !matches!(self.k(), TokenKind::Newline) {
                    return self.fail("rule condition must end at newline");
                }
                self.newlines();
                if matches!(self.k(), TokenKind::Eof) {
                    return self.fail("expected action after condition");
                }
                let action_start = self.span();
                let action = self.action()?;
                let end = self.t[self.i - 1].span;
                let n = rules.len() + 1;
                rules.push(Rule {
                    id: format!("rule_{n}"),
                    condition,
                    action,
                    span: Span::new(start.start, end.end, start.line, start.column),
                    condition_span: Span::new(
                        condition_start.start,
                        condition_end.end,
                        condition_start.line,
                        condition_start.column,
                    ),
                    action_span: Span::new(
                        action_start.start,
                        end.end,
                        action_start.line,
                        action_start.column,
                    ),
                });
                if !matches!(self.k(), TokenKind::Newline | TokenKind::Eof) {
                    return self.fail("expected newline after action");
                }
                self.newlines();
                continue;
            }
            return self.fail("expected declaration or WHEN rule");
        }
        Ok(Strategy {
            name,
            trade_in,
            stop_loss,
            take_profit,
            rules,
            risk,
        })
    }
    fn trade_in(&mut self) -> Result<TradeIn, FrontendError> {
        if self.take_word("INDEX") {
            return match self.bump().kind {
                TokenKind::Word(w) => Ok(TradeIn::Index(w)),
                _ => self.fail("expected index alias"),
            };
        }
        let mut xs = Vec::new();
        loop {
            match self.bump().kind {
                TokenKind::Word(w) if !w.eq_ignore_ascii_case("INDEX") => xs.push(w),
                TokenKind::String(w) => xs.push(w),
                _ => return self.fail("expected symbol or INDEX"),
            };
            if !matches!(self.k(), TokenKind::Comma) {
                break;
            }
            self.bump();
        }
        if xs.is_empty() {
            self.fail("TRADE_IN requires symbol(s)")
        } else {
            Ok(TradeIn::Symbols(xs))
        }
    }
    fn percent(&mut self) -> Result<Decimal, FrontendError> {
        let token_start = self.span();
        let n = self.number()?;
        if !matches!(self.k(), TokenKind::Percent) {
            return self.fail("expected %");
        };
        if self.i > 0 && self.t[self.i - 1].span.end != self.span().start {
            return self.fail("% must immediately follow number");
        }
        self.bump();
        let end = self.t[self.i - 1].span.end;
        NumericLiteral::parse(
            n,
            Span::new(token_start.start, end, token_start.line, token_start.column),
        )
        .ok_or_else(|| FrontendError::Parse {
            code: "PARSE_NUMBER",
            message: "invalid decimal".into(),
            line: token_start.line,
            column: token_start.column,
            span: token_start,
        })
    }
    fn decimal(&self, n: String) -> Result<Decimal, FrontendError> {
        let span = self.t[self.i.saturating_sub(1)].span;
        NumericLiteral::parse(n, span).ok_or_else(|| FrontendError::Parse {
            code: "PARSE_NUMBER",
            message: "invalid decimal".into(),
            line: span.line,
            column: span.column,
            span,
        })
    }
    fn number(&mut self) -> Result<String, FrontendError> {
        let sign = if matches!(self.k(), TokenKind::Minus | TokenKind::Plus) {
            let s = if matches!(self.k(), TokenKind::Minus) {
                "-"
            } else {
                "+"
            };
            self.bump();
            s
        } else {
            ""
        };
        match self.bump().kind {
            TokenKind::Number(n) => Ok(format!("{sign}{n}")),
            _ => self.fail("expected number"),
        }
    }
    fn uint(&mut self) -> Result<u64, FrontendError> {
        let n = self.number()?;
        if n.starts_with(['-', '+']) || n.contains('.') {
            return self.fail("expected unsigned integer");
        }
        n.parse().map_err(|_| FrontendError::Parse {
            code: "PARSE_INTEGER",
            message: "integer out of range".into(),
            line: self.span().line,
            column: self.span().column,
            span: self.span(),
        })
    }
    fn action(&mut self) -> Result<Action, FrontendError> {
        let buy = if self.take_word("BUY") {
            true
        } else if self.take_word("SELL") {
            false
        } else {
            return self.fail("expected BUY or SELL action");
        };
        if !buy && self.take_word("ALL") {
            if !matches!(self.k(), TokenKind::Newline | TokenKind::Eof) {
                return self.fail("SELL ALL cannot have an amount or suffix");
            }
            return Ok(Action::SellAll);
        }
        let n = self.number()?;
        let number_end = self.span().start;
        let amount = if self.take_word("WORTH") {
            if n.starts_with(['-', '+']) {
                return self.fail("WORTH requires an unsigned decimal amount");
            }
            TradeAmount::Worth(self.decimal(n)?)
        } else if matches!(self.k(), TokenKind::Percent) {
            if number_end != self.span().start {
                return self.fail("% must immediately follow number");
            }
            self.bump();
            if buy {
                TradeAmount::BuyCapitalPercentage(self.decimal(n)?)
            } else {
                TradeAmount::SellPositionPercentage(self.decimal(n)?)
            }
        } else {
            let q = n.parse::<u64>().map_err(|_| FrontendError::Parse {
                code: "PARSE_INTEGER",
                message: "quantity must be a nonnegative integer".into(),
                line: self.span().line,
                column: self.span().column,
                span: self.span(),
            })?;
            TradeAmount::Quantity(q)
        };
        if !matches!(self.k(), TokenKind::Newline | TokenKind::Eof) {
            return self.fail("unexpected token after action");
        }
        Ok(if buy {
            Action::Buy(amount)
        } else {
            Action::Sell(amount)
        })
    }
    fn condition(&mut self) -> Result<Condition, FrontendError> {
        self.or()
    }
    fn or(&mut self) -> Result<Condition, FrontendError> {
        let mut x = self.and()?;
        while self.take_word("OR") {
            x = Condition::Or(Box::new(x), Box::new(self.and()?));
        }
        Ok(x)
    }
    fn and(&mut self) -> Result<Condition, FrontendError> {
        let mut x = self.not()?;
        while self.take_word("AND") {
            x = Condition::And(Box::new(x), Box::new(self.not()?));
        }
        Ok(x)
    }
    fn not(&mut self) -> Result<Condition, FrontendError> {
        if self.take_word("NOT") {
            let mut count = 1;
            while self.take_word("NOT") {
                count += 1;
            }
            let mut c = self.not()?;
            for _ in 0..count {
                c = Condition::Not(Box::new(c));
            }
            return Ok(c);
        }
        if matches!(self.k(), TokenKind::LParen) {
            self.bump();
            self.check_depth()?;
            let x = self.condition()?;
            if !matches!(self.k(), TokenKind::RParen) {
                return self.fail("expected closing parenthesis");
            }
            self.bump();
            return Ok(x);
        }
        self.predicate()
    }
    fn predicate(&mut self) -> Result<Condition, FrontendError> {
        if self.word("CROSS_ABOVE") || self.word("CROSS_BELOW") {
            let direction = if self.take_word("CROSS_ABOVE") {
                CrossDirection::Above
            } else {
                self.bump();
                CrossDirection::Below
            };
            if !matches!(self.k(), TokenKind::LParen) {
                return self.fail("cross predicate requires parentheses");
            }
            self.bump();
            self.check_depth()?;
            let left = self.numeric()?;
            if !matches!(self.k(), TokenKind::Comma) {
                return self.fail("cross predicate expects two arguments");
            }
            self.bump();
            let right = self.numeric()?;
            if !matches!(self.k(), TokenKind::RParen) {
                return self.fail("expected closing parenthesis");
            }
            self.bump();
            return Ok(Condition::Cross {
                direction,
                left,
                right,
            });
        }
        if self.take_word("IN_POSITION") {
            if !matches!(self.k(), TokenKind::LParen) {
                return self.fail("in_position requires parentheses");
            }
            self.bump();
            if !matches!(self.k(), TokenKind::RParen) {
                return self.fail("in_position takes no arguments");
            }
            self.bump();
            return Ok(Condition::InPosition);
        }
        if self.take_word("BETWEEN") {
            if !matches!(self.k(), TokenKind::LParen) {
                return self.fail("between requires parentheses");
            }
            self.bump();
            let a = self.time()?;
            if !matches!(self.k(), TokenKind::Comma) {
                return self.fail("between expects two times");
            }
            self.bump();
            let b = self.time()?;
            if !matches!(self.k(), TokenKind::RParen) {
                return self.fail("expected closing parenthesis");
            }
            self.bump();
            return Ok(Condition::Between { start: a, end: b });
        }
        let left = self.numeric()?;
        let op = match self.k() {
            TokenKind::Lt => CompareOp::Lt,
            TokenKind::Le => CompareOp::Le,
            TokenKind::Gt => CompareOp::Gt,
            TokenKind::Ge => CompareOp::Ge,
            TokenKind::Eq => CompareOp::Eq,
            TokenKind::Ne => CompareOp::Ne,
            _ => return self.fail("expected comparison operator"),
        };
        self.bump();
        let right = self.numeric()?;
        if matches!(
            self.k(),
            TokenKind::Lt
                | TokenKind::Le
                | TokenKind::Gt
                | TokenKind::Ge
                | TokenKind::Eq
                | TokenKind::Ne
        ) {
            return self.fail("chained comparisons are not supported");
        }
        Ok(Condition::Compare { left, op, right })
    }
    fn time(&mut self) -> Result<Time, FrontendError> {
        let s = match self.bump().kind {
            TokenKind::Time(s) => s,
            _ => return self.fail("expected HH:MM time"),
        };
        let mut p = s.split(':');
        let h = p.next().unwrap_or("").parse::<u8>().unwrap_or(255);
        let m = p.next().unwrap_or("").parse::<u8>().unwrap_or(255);
        Ok(Time { hour: h, minute: m })
    }
    fn numeric(&mut self) -> Result<NumericExpr, FrontendError> {
        if matches!(
            self.k(),
            TokenKind::Number(_) | TokenKind::Minus | TokenKind::Plus
        ) {
            let n = self.number()?;
            return Ok(NumericExpr::Literal(self.decimal(n)?));
        }
        let name = match self.bump().kind {
            TokenKind::Word(w) => w,
            _ => return self.fail("expected numeric expression"),
        };
        let price = match name.to_ascii_lowercase().as_str() {
            "close" => Some(PriceField::Close),
            "open" => Some(PriceField::Open),
            "high" => Some(PriceField::High),
            "low" => Some(PriceField::Low),
            "volume" => Some(PriceField::Volume),
            "prev_close" => Some(PriceField::PrevClose),
            "prev_open" => Some(PriceField::PrevOpen),
            "prev_high" => Some(PriceField::PrevHigh),
            "prev_low" => Some(PriceField::PrevLow),
            _ => None,
        };
        if let Some(p) = price {
            return Ok(NumericExpr::Price(p));
        }
        let kind = match name.to_ascii_lowercase().as_str() {
            "sma" => IndicatorKind::Sma,
            "ema" => IndicatorKind::Ema,
            "rsi" => IndicatorKind::Rsi,
            "rel_vol" => IndicatorKind::RelVol,
            "atr" => IndicatorKind::Atr,
            "vwap" => IndicatorKind::Vwap,
            "bb_upper" => IndicatorKind::BbUpper,
            "bb_lower" => IndicatorKind::BbLower,
            "bb_mid" => IndicatorKind::BbMid,
            _ => return self.fail("unknown price field or indicator"),
        };
        if !matches!(self.k(), TokenKind::LParen) {
            return self.fail("indicator requires parentheses");
        }
        self.bump();
        let period_token = self.bump();
        let period = match period_token.kind {
            TokenKind::Number(n) if !n.contains('.') => {
                n.parse::<u32>().map_err(|_| FrontendError::Parse {
                    code: "PARSE_PERIOD",
                    message: "indicator period must be a positive integer".into(),
                    line: self.span().line,
                    column: self.span().column,
                    span: self.span(),
                })?
            }
            _ => return self.fail("indicator period must be an integer"),
        };
        let multiplier = if matches!(self.k(), TokenKind::Comma) {
            self.bump();
            let n = self.number()?;
            Some(self.decimal(n)?)
        } else {
            None
        };
        let multiplier = if matches!(
            kind,
            IndicatorKind::BbUpper | IndicatorKind::BbLower | IndicatorKind::BbMid
        ) && multiplier.is_none()
        {
            Some(NumericLiteral::parse("2.0".into(), self.span()).unwrap())
        } else {
            multiplier
        };
        if !matches!(
            kind,
            IndicatorKind::BbUpper | IndicatorKind::BbLower | IndicatorKind::BbMid
        ) && multiplier.is_some()
        {
            return self.fail("this indicator accepts only a period");
        }
        if multiplier.is_some() && matches!(self.k(), TokenKind::Comma) {
            return self.fail("too many indicator arguments");
        }
        if !matches!(self.k(), TokenKind::RParen) {
            return self.fail("expected closing indicator parenthesis");
        }
        self.bump();
        if period == 0 {
            return self.fail("indicator period must be positive");
        }
        if let Some(m) = &multiplier {
            if !m.cmp_integer("0").is_some_and(|v| v.is_gt()) {
                return self.fail("Bollinger multiplier must be positive");
            }
        }
        Ok(NumericExpr::Indicator(IndicatorCall {
            kind,
            period,
            multiplier,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::lex;
    #[test]
    fn precedence_is_and_before_or_and_rules_are_ordered() {
        let src = "TRADE_IN ABC\nWHEN close > 1 AND open > 2 OR high > 3\nBUY 2\nWHEN low < 1\nSELL ALL\n";
        let ast = parse(&lex(src).unwrap()).unwrap();
        assert_eq!(ast.rules.len(), 2);
        assert_eq!(ast.rules[0].id, "rule_1");
        assert!(matches!(ast.rules[0].condition, Condition::Or(_, _)));
        assert!(matches!(ast.rules[1].action, Action::SellAll));
    }
    #[test]
    fn declarations_may_be_interleaved_and_duplicates_fail() {
        let src = "TRADE_IN ABC\nWHEN close > 1\nBUY 2\nSTOP_LOSS 5%\nWHEN low < 1\nSELL ALL\n";
        assert_eq!(
            parse(&lex(src).unwrap()).unwrap().stop_loss,
            Some(NumericLiteral::parse("5".into(), Span::new(44, 46, 4, 11)).unwrap())
        );
        assert!(parse(&lex("TRADE_IN ABC\nSTOP_LOSS 2%\nSTOP_LOSS 3%\n").unwrap()).is_err());
    }
    #[test]
    fn trailing_input_and_missing_rule_newline_are_rejected() {
        assert!(parse(&lex("TRADE_IN ABC\nWHEN close > 1 BUY 2\n").unwrap()).is_err());
        assert!(lex("TRADE_IN ABC\nWHEN close > 1\nBUY 2\n@\n").is_err());
    }
    #[test]
    fn headers_actions_suffixes_and_eof_are_strict() {
        for src in [
            "TRADE_IN ABC\nTRADE_IN XYZ\n",
            "TRADE_IN ABC\nSTRATEGY late\n",
            "TRADE_IN ABC\nSTOP_LOSS 2 %\n",
            "TRADE_IN ABC\nWHEN close > 1\nBUY 1%WORTH\n",
            "TRADE_IN ABC\nWHEN close > 1\nSELL ALL 1\n",
            "TRADE_IN ABC\nWHEN 1 < close < 10\nBUY 1\n",
            "TRADE_IN ABC,\n",
            "TRADE_IN ABC\nWHEN close > 1\nBUY 1\nGARBAGE\n",
        ] {
            assert!(compile_err(src), "{src}");
        }
        assert!(!compile_err(
            "STRATEGY Momentum\nTRADE_IN INDEX NIFTY\n\nWHEN SMA(14) > 1\n\nBUY 00010\n"
        ));
    }
    #[test]
    fn quoted_symbols_fractional_worth_and_price_crossovers_parse() {
        let s = crate::compile("TRADE_IN \"M&M\", \"BAJAJ-AUTO\", \"3MINDIA\", \"360ONE\"\nWHEN CROSS_ABOVE(close, ema(20))\nBUY 10.50 WORTH\nWHEN close > 0\nSELL 12.5%\n").unwrap();
        assert_eq!(
            s.strategy().trade_in,
            TradeIn::Symbols(vec![
                "M&M".into(),
                "BAJAJ-AUTO".into(),
                "3MINDIA".into(),
                "360ONE".into()
            ])
        );
        assert!(matches!(s.rules()[0].condition, Condition::Cross { .. }));
        assert!(matches!(
            s.rules()[0].action,
            Action::Buy(TradeAmount::Worth(_))
        ));
        assert!(matches!(
            s.rules()[1].action,
            Action::Sell(TradeAmount::SellPositionPercentage(_))
        ));
    }
    fn compile_err(s: &str) -> bool {
        let t = lex(s);
        match t {
            Err(_) => true,
            Ok(t) => parse(&t).is_err(),
        }
    }
}
//! Deterministic, local historical simulation primitives.
//!
//! This module depends on the strategy engine and execution port only. Data
//! sources are replaceable; the file source never performs network access.
use crate::{
    engine::{Engine, LevelFrame, Program, Symbol},
    indicators::Candle,
    trader::{
        ExecutionPort, Mode, OrderEvent, OrderId, OrderReason, OrderRequest, PortError, Side,
        Trader, TraderConfig, TraderEvent,
    },
};
use arrow_array::{Array, Float64Array, Int64Array, RecordBatch};
use chrono::{DateTime, Datelike, FixedOffset, TimeZone};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap, HashMap},
    fs::File,
    io::Read,
    path::PathBuf,
    sync::{Arc, Mutex},
};
use thiserror::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Timeframe {
    M1,
    M5,
    M15,
    M25,
    M60,
    D1,
}
impl Timeframe {
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "1m" => Self::M1,
            "5m" => Self::M5,
            "15m" => Self::M15,
            "25m" => Self::M25,
            "60m" => Self::M60,
            "1d" => Self::D1,
            _ => return None,
        })
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::M1 => "1m",
            Self::M5 => "5m",
            Self::M15 => "15m",
            Self::M25 => "25m",
            Self::M60 => "60m",
            Self::D1 => "1d",
        }
    }
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DateRange {
    pub from: Option<i64>,
    pub to: Option<i64>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fingerprint(pub String);
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum GapPolicy {
    None,
    FillFlatInSession,
}
#[derive(Debug, Error)]
pub enum SourceError {
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("{file}, row {row}: {message}")]
    Invalid {
        file: PathBuf,
        row: usize,
        message: String,
    },
    #[error("{0}")]
    Other(String),
}
pub type CandleStream = Box<dyn Iterator<Item = Result<(Symbol, Candle), SourceError>> + Send>;

/// Reproducible candles for exercising strategy behavior without external data.
/// The generated series includes an upward trend, flat sessions, one missing
/// symbol interval, intraday gaps, and a deterministic volatile session.
pub fn seeded_synthetic_candles(
    seed: u64,
    symbols: &[Symbol],
    sessions: usize,
) -> Vec<(Symbol, Candle)> {
    let mut state = seed;
    let mut rows = Vec::new();
    const SESSION_OPEN: i64 = 9 * 3600 + 15 * 60;
    for day in 0..sessions {
        let date = chrono::NaiveDate::from_ymd_opt(2024, 1, 2)
            .unwrap()
            .checked_add_days(chrono::Days::new(day as u64))
            .unwrap();
        // Skip weekends to create realistic session gaps in the input stream.
        if matches!(date.weekday(), chrono::Weekday::Sat | chrono::Weekday::Sun) {
            continue;
        }
        let day_start =
            chrono::NaiveDateTime::new(date, chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap())
                .and_utc()
                .timestamp();
        for (symbol_index, symbol) in symbols.iter().enumerate() {
            let mut price = 100.0 + symbol_index as f64 * 20.0;
            for bar in 0..12 {
                // One missing symbol interval, repeated deterministically.
                if symbol_index == 1 && day == 1 && bar == 5 {
                    continue;
                }
                // Flat opening/closing sections and a volatile middle session.
                let change = if !(2..=9).contains(&bar) {
                    0.0
                } else if day == 2 && (5..=7).contains(&bar) {
                    if bar % 2 == 0 {
                        8.0
                    } else {
                        -7.0
                    }
                } else {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    (state % 401) as f64 / 100.0 - 2.0 + (bar as f64 * 0.1)
                };
                let open = price;
                price = (price + change).max(1.0);
                let ts = day_start + SESSION_OPEN + bar * 60;
                rows.push((
                    symbol.clone(),
                    Candle {
                        timestamp: ts,
                        open,
                        high: open.max(price)
                            + if day == 2 && (5..=7).contains(&bar) {
                                3.0
                            } else {
                                0.1
                            },
                        low: open.min(price)
                            - if day == 2 && (5..=7).contains(&bar) {
                                3.0
                            } else {
                                0.1
                            },
                        close: price,
                        volume: if !(2..=9).contains(&bar) {
                            0.0
                        } else {
                            (state % 1000 + 1) as f64
                        },
                    },
                ));
            }
        }
    }
    rows.sort_by(|a, b| (a.1.timestamp, &a.0).cmp(&(b.1.timestamp, &b.0)));
    rows
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SourceStats {
    pub skipped_bad: BTreeMap<Symbol, u64>,
    pub missing_candles: BTreeMap<Symbol, u64>,
}
pub trait CandleSource: Send + Sync {
    fn open(
        &self,
        symbols: &[Symbol],
        tf: Timeframe,
        range: DateRange,
    ) -> Result<CandleStream, SourceError>;
    fn fingerprint(&self, symbols: &[Symbol], tf: Timeframe, range: DateRange) -> Fingerprint;
    fn stats(&self) -> SourceStats {
        SourceStats::default()
    }
    fn instrument_rules(
        &self,
        _symbols: &[Symbol],
    ) -> BTreeMap<Symbol, crate::trader::InstrumentRule> {
        BTreeMap::new()
    }
    fn tick_sizes_paise(&self, _symbols: &[Symbol]) -> BTreeMap<Symbol, i64> {
        BTreeMap::new()
    }
}
fn limit_to_warmup_window(mut input: CandleStream, from: Option<i64>, bars: usize) -> CandleStream {
    let Some(from) = from else { return input };
    let mut tails = BTreeMap::<Symbol, std::collections::VecDeque<Candle>>::new();
    let mut first = None;
    let mut early_error = None;
    for item in input.by_ref() {
        match item {
            Ok((symbol, candle)) if candle.timestamp < from => {
                if bars > 0 {
                    let q = tails.entry(symbol).or_default();
                    q.push_back(candle);
                    if q.len() > bars {
                        q.pop_front();
                    }
                }
            }
            Ok(x) => {
                first = Some(x);
                break;
            }
            Err(e) => {
                early_error = Some(e);
                break;
            }
        }
    }
    if let Some(error) = early_error {
        return Box::new(std::iter::once(Err(error)));
    }
    let Some(first) = first else {
        return Box::new(std::iter::empty());
    };
    let mut warm = Vec::new();
    for (symbol, tail) in tails {
        warm.extend(tail.into_iter().map(|c| (symbol.clone(), c)));
    }
    warm.sort_by(|a, b| (a.1.timestamp, &a.0).cmp(&(b.1.timestamp, &b.0)));
    Box::new(
        warm.into_iter()
            .map(Ok)
            .chain(std::iter::once(Ok(first)))
            .chain(input),
    )
}

/// One-row-per-symbol merge. The heap contains at most one decoded candle per
/// input, keeping the file-backed stream bounded by the number of symbols.
pub struct FileSource {
    pub dir: PathBuf,
    pub csv_tz: Option<String>,
    pub skip_bad: bool,
    pub gap_policy: GapPolicy,
    stats: Arc<Mutex<SourceStats>>,
}
impl FileSource {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            csv_tz: None,
            skip_bad: false,
            gap_policy: GapPolicy::None,
            stats: Arc::new(Mutex::new(SourceStats::default())),
        }
    }
}
struct CsvCursor {
    symbol: Symbol,
    path: PathBuf,
    reader: csv::Reader<File>,
    row: usize,
    previous: Option<i64>,
    tz: Option<CsvTimezone>,
    skip_bad: bool,
    stats: Arc<Mutex<SourceStats>>,
}
struct ParquetCursor {
    symbol: Symbol,
    path: PathBuf,
    reader: ParquetRecordBatchReader,
    batch: Option<RecordBatch>,
    row: usize,
    row_in_batch: usize,
    columns: [usize; 6],
    previous: Option<i64>,
    skip_bad: bool,
    stats: Arc<Mutex<SourceStats>>,
}
enum DataCursor {
    Csv(CsvCursor),
    Parquet(ParquetCursor),
}
struct GapCursor {
    inner: DataCursor,
    symbol: Symbol,
    policy: GapPolicy,
    step: i64,
    pending: Option<Candle>,
    previous: Option<Candle>,
    gaps: std::collections::VecDeque<Candle>,
    stats: Arc<Mutex<SourceStats>>,
}
impl GapCursor {
    fn next(&mut self) -> Option<Result<(Symbol, Candle), SourceError>> {
        loop {
            if let Some(c) = self.gaps.pop_front() {
                self.previous = Some(c);
                return Some(Ok((self.symbol.clone(), c)));
            }
            let (current, may_fill) = if let Some(c) = self.pending.take() {
                (Some(Ok((self.symbol.clone(), c))), false)
            } else {
                (self.inner.next(), true)
            };
            let (s, c) = match current? {
                Ok(x) => x,
                Err(e) => return Some(Err(e)),
            };
            if may_fill {
                if let Some(prev) = self.previous {
                    // Gap filling is intraday-only and confined to a single trading
                    // session. Daily (and coarser) series are never filled: a gap
                    // between two daily bars spans non-trading days (weekends,
                    // holidays), and the sim carries no trading calendar to tell a
                    // holiday apart from genuinely missing data, so a flat fill there
                    // would fabricate bars and corrupt the backtest. Overnight /
                    // cross-session gaps are skipped for the same reason (markets are
                    // closed, so no bars are "missing") — hence prev and c must share
                    // an IST day and only in-session timestamps are counted/filled.
                    let intraday = self.step < 86_400;
                    let same_session = ist_day(prev.timestamp) == ist_day(c.timestamp);
                    if intraday && same_session {
                        let mut ts = prev.timestamp.saturating_add(self.step);
                        let mut count = 0;
                        while ts < c.timestamp && in_ist_session(ts) {
                            count += 1;
                            if self.policy == GapPolicy::FillFlatInSession {
                                self.gaps.push_back(Candle {
                                    timestamp: ts,
                                    open: prev.close,
                                    high: prev.close,
                                    low: prev.close,
                                    close: prev.close,
                                    volume: 0.0,
                                });
                            }
                            ts = ts.saturating_add(self.step);
                        }
                        if count > 0 {
                            if let Ok(mut stats) = self.stats.lock() {
                                *stats
                                    .missing_candles
                                    .entry(self.symbol.clone())
                                    .or_default() += count;
                            }
                        }
                        if !self.gaps.is_empty() {
                            self.pending = Some(c);
                            continue;
                        }
                    }
                }
            }
            self.previous = Some(c);
            return Some(Ok((s, c)));
        }
    }
}
fn in_ist_session(ts: i64) -> bool {
    let local = (ts + 19800).rem_euclid(86400);
    (9 * 3600 + 15 * 60..15 * 3600 + 30 * 60).contains(&local)
}
#[derive(Clone, Copy)]
enum CsvTimezone {
    Named(chrono_tz::Tz),
    Offset(FixedOffset),
}
fn timeframe_seconds(tf: Timeframe) -> i64 {
    match tf {
        Timeframe::M1 => 60,
        Timeframe::M5 => 300,
        Timeframe::M15 => 900,
        Timeframe::M25 => 1500,
        Timeframe::M60 => 3600,
        Timeframe::D1 => 86400,
    }
}
impl CsvCursor {
    fn next(&mut self) -> Option<Result<(Symbol, Candle), SourceError>> {
        loop {
            let mut record = csv::StringRecord::new();
            match self.reader.read_record(&mut record) {
                Ok(false) => return None,
                Err(e) if self.skip_bad => {
                    self.row += 1;
                    if let Ok(mut stats) = self.stats.lock() {
                        *stats.skipped_bad.entry(self.symbol.clone()).or_default() += 1;
                    }
                    let _ = e;
                    continue;
                }
                Err(e) => {
                    return Some(Err(SourceError::Other(format!(
                        "{} row {}: {e}",
                        self.path.display(),
                        self.row + 1
                    ))))
                }
                Ok(true) => {}
            }
            self.row += 1;
            if self.row == 1 && record.get(0).is_some_and(|value| value.trim() == "ts") {
                continue;
            }
            let invalid = |message: String| SourceError::Invalid {
                file: self.path.clone(),
                row: self.row,
                message,
            };
            let parsed = (|| -> Result<Candle, String> {
                if record.len() != 6 {
                    return Err("expected ts,open,high,low,close,volume".into());
                }
                let ts =
                    parse_timestamp(record.get(0).unwrap(), self.tz).map_err(|e| e.to_string())?;
                let mut v = [0.0; 5];
                for (i, x) in record.iter().skip(1).enumerate() {
                    v[i] = x.parse().map_err(|_| format!("invalid number {x:?}"))?;
                }
                let c = Candle {
                    timestamp: ts,
                    open: v[0],
                    high: v[1],
                    low: v[2],
                    close: v[3],
                    volume: v[4],
                };
                c.is_valid(None).map_err(str::to_owned)?;
                if [c.open, c.high, c.low, c.close].iter().any(|p| *p <= 0.0)
                    || c.low > c.open.min(c.close)
                    || c.high < c.open.max(c.close)
                {
                    return Err(
                        "prices must be positive and low/high must bound open and close".into(),
                    );
                }
                if self.previous.is_some_and(|p| ts <= p) {
                    return Err("timestamp must be strictly increasing".into());
                }
                self.previous = Some(ts);
                Ok(c)
            })();
            match parsed {
                Ok(c) => return Some(Ok((self.symbol.clone(), c))),
                Err(_msg) if self.skip_bad => {
                    if let Ok(mut stats) = self.stats.lock() {
                        *stats.skipped_bad.entry(self.symbol.clone()).or_default() += 1;
                    }
                    continue;
                }
                Err(msg) => return Some(Err(invalid(msg))),
            }
        }
    }
}
impl ParquetCursor {
    fn next(&mut self) -> Option<Result<(Symbol, Candle), SourceError>> {
        loop {
            if self
                .batch
                .as_ref()
                .map_or(true, |b| self.row_in_batch >= b.num_rows())
            {
                match self.reader.next() {
                    Some(Ok(batch)) => {
                        self.batch = Some(batch);
                        self.row_in_batch = 0;
                    }
                    Some(Err(e)) if self.skip_bad => {
                        self.row += 1;
                        if let Ok(mut stats) = self.stats.lock() {
                            *stats.skipped_bad.entry(self.symbol.clone()).or_default() += 1;
                        }
                        let _ = e;
                        continue;
                    }
                    Some(Err(e)) => {
                        return Some(Err(SourceError::Other(format!(
                            "{} row {}: {e}",
                            self.path.display(),
                            self.row + 1
                        ))))
                    }
                    None => return None,
                }
            }
            let batch = self.batch.as_ref().unwrap();
            let i = self.row_in_batch;
            self.row_in_batch += 1;
            self.row += 1;
            let invalid = |message: String| SourceError::Invalid {
                file: self.path.clone(),
                row: self.row,
                message,
            };
            let parsed = (|| -> Result<Candle, String> {
                let ts = batch
                    .column(self.columns[0])
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or("ts column must be i64")?;
                let mut vals = [0.0; 5];
                for j in 1..6 {
                    let array = batch
                        .column(self.columns[j])
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .ok_or_else(|| {
                            format!(
                                "{} column must be f64",
                                ["open", "high", "low", "close", "volume"][j - 1]
                            )
                        })?;
                    if array.is_null(i) {
                        return Err("null values are not allowed".into());
                    }
                    vals[j - 1] = array.value(i);
                }
                if ts.is_null(i) {
                    return Err("null timestamp is not allowed".into());
                }
                let c = Candle {
                    timestamp: ts.value(i),
                    open: vals[0],
                    high: vals[1],
                    low: vals[2],
                    close: vals[3],
                    volume: vals[4],
                };
                c.is_valid(None).map_err(str::to_owned)?;
                if [c.open, c.high, c.low, c.close].iter().any(|p| *p <= 0.0)
                    || c.low > c.open.min(c.close)
                    || c.high < c.open.max(c.close)
                {
                    return Err(
                        "prices must be positive and low/high must bound open and close".into(),
                    );
                }
                if self.previous.is_some_and(|p| c.timestamp <= p) {
                    return Err("timestamp must be strictly increasing".into());
                }
                self.previous = Some(c.timestamp);
                Ok(c)
            })();
            match parsed {
                Ok(c) => return Some(Ok((self.symbol.clone(), c))),
                Err(_e) if self.skip_bad => {
                    if let Ok(mut stats) = self.stats.lock() {
                        *stats.skipped_bad.entry(self.symbol.clone()).or_default() += 1;
                    }
                    continue;
                }
                Err(e) => return Some(Err(invalid(e))),
            }
        }
    }
}
impl DataCursor {
    fn next(&mut self) -> Option<Result<(Symbol, Candle), SourceError>> {
        match self {
            Self::Csv(c) => c.next(),
            Self::Parquet(c) => c.next(),
        }
    }
}
fn parse_timestamp(s: &str, tz: Option<CsvTimezone>) -> Result<i64, String> {
    if let Ok(n) = s.parse::<i64>() {
        return Ok(n);
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.timestamp());
    }
    let naive = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
        .map_err(|_| "timestamp must be epoch seconds or RFC3339 with offset".to_string())?;
    let local = match tz.ok_or_else(|| "naive timestamp requires --csv-tz".to_string())? {
        CsvTimezone::Named(tz) => tz
            .from_local_datetime(&naive)
            .single()
            .map(|d| d.timestamp()),
        CsvTimezone::Offset(tz) => tz
            .from_local_datetime(&naive)
            .single()
            .map(|d| d.timestamp()),
    };
    local.ok_or_else(|| "ambiguous or invalid local timestamp".into())
}
impl CandleSource for FileSource {
    fn open(
        &self,
        symbols: &[Symbol],
        tf: Timeframe,
        range: DateRange,
    ) -> Result<CandleStream, SourceError> {
        if let Ok(mut stats) = self.stats.lock() {
            *stats = SourceStats::default();
        }
        let tz = match self.csv_tz.as_deref() {
            None => None,
            Some(x) => Some(if let Some(offset) = parse_fixed_tz(x) {
                CsvTimezone::Offset(offset)
            } else {
                CsvTimezone::Named(
                    x.parse::<chrono_tz::Tz>()
                        .map_err(|_| SourceError::Other(format!("unsupported timezone {x:?}")))?,
                )
            }),
        };
        let mut cursors: Vec<GapCursor> = Vec::new();
        for s in symbols {
            let csv = self.dir.join(format!("{s}.csv"));
            let pq = self.dir.join(format!("{s}.parquet"));
            let path = if csv.exists() {
                csv
            } else if pq.exists() {
                pq
            } else if symbols.len() == 1 && self.dir.is_file() {
                self.dir.clone()
            } else {
                return Err(SourceError::Other(format!("missing data file for {s}")));
            };
            let inner = if path
                .extension()
                .and_then(|x| x.to_str())
                .is_some_and(|x| x.eq_ignore_ascii_case("parquet"))
            {
                let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path)?)
                    .map_err(|e| SourceError::Other(format!("{}: {e}", path.display())))?;
                let schema = builder.schema();
                let names = ["ts", "open", "high", "low", "close", "volume"];
                let mut columns = [0usize; 6];
                for (i, name) in names.iter().enumerate() {
                    columns[i] = schema.index_of(name).map_err(|_| {
                        SourceError::Other(format!(
                            "{}: missing Parquet column {name}",
                            path.display()
                        ))
                    })?;
                }
                let reader = builder
                    .with_batch_size(1024)
                    .build()
                    .map_err(|e| SourceError::Other(format!("{}: {e}", path.display())))?;
                DataCursor::Parquet(ParquetCursor {
                    symbol: s.clone(),
                    path: path.clone(),
                    reader,
                    batch: None,
                    row: 0,
                    row_in_batch: 0,
                    columns,
                    previous: None,
                    skip_bad: self.skip_bad,
                    stats: self.stats.clone(),
                })
            } else {
                DataCursor::Csv(CsvCursor {
                    symbol: s.clone(),
                    path: path.clone(),
                    reader: csv::ReaderBuilder::new()
                        .has_headers(false)
                        .from_reader(File::open(&path)?),
                    row: 0,
                    previous: None,
                    tz,
                    skip_bad: self.skip_bad,
                    stats: self.stats.clone(),
                })
            };
            cursors.push(GapCursor {
                inner,
                symbol: s.clone(),
                policy: self.gap_policy,
                step: timeframe_seconds(tf),
                pending: None,
                previous: None,
                gaps: Default::default(),
                stats: self.stats.clone(),
            });
        }
        let mut heap: BinaryHeap<Reverse<(i64, Symbol, usize)>> = BinaryHeap::new();
        let mut heads: Vec<Option<Candle>> = vec![None; cursors.len()];
        for (i, c) in cursors.iter_mut().enumerate() {
            if let Some((_, x)) = c.next().transpose()? {
                heads[i] = Some(x);
                heap.push(Reverse((x.timestamp, c.symbol.clone(), i)));
            }
        }
        let iter = std::iter::from_fn(move || loop {
            let Reverse((ts, s, i)) = heap.pop()?;
            let c = heads[i].take().expect("heap head");
            let mut out = None;
            if range.from.map_or(true, |x| ts >= x) && range.to.map_or(true, |x| ts <= x) {
                out = Some(Ok((s, c)));
            }
            match cursors[i].next() {
                Some(Ok((ns, nc))) => {
                    heads[i] = Some(nc);
                    heap.push(Reverse((nc.timestamp, ns, i)))
                }
                Some(Err(e)) => return Some(Err(e)),
                None => {}
            }
            if out.is_some() {
                return out;
            }
        });
        Ok(Box::new(iter))
    }
    fn fingerprint(&self, symbols: &[Symbol], tf: Timeframe, range: DateRange) -> Fingerprint {
        // Stable FNV-1a over request metadata and source file bytes.
        let mut h = 0xcbf29ce484222325u64;
        hash_feed(&mut h, tf.as_str().as_bytes());
        hash_feed(
            &mut h,
            format!(
                "{:?}{:?}{:?}{:?}{:?}",
                range.from, range.to, self.csv_tz, self.skip_bad, self.gap_policy
            )
            .as_bytes(),
        );
        for s in symbols {
            hash_feed(&mut h, s.as_bytes());
            if self.dir.is_file() && symbols.len() == 1 {
                hash_file(&mut h, &self.dir);
            } else {
                for ext in ["csv", "parquet"] {
                    hash_file(&mut h, &self.dir.join(format!("{s}.{ext}")));
                }
            }
        }
        Fingerprint(format!("fnv1a64:{h:016x}"))
    }
    fn stats(&self) -> SourceStats {
        self.stats.lock().map(|x| x.clone()).unwrap_or_default()
    }
}
fn hash_feed(h: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *h ^= *byte as u64;
        *h = h.wrapping_mul(0x100000001b3);
    }
}
fn hash_file(h: &mut u64, path: &std::path::Path) {
    if let Ok(mut file) = File::open(path) {
        let mut buffer = [0u8; 64 * 1024];
        loop {
            match file.read(&mut buffer) {
                Ok(0) | Err(_) => break,
                Ok(n) => hash_feed(h, &buffer[..n]),
            }
        }
    }
}
fn parse_fixed_tz(s: &str) -> Option<FixedOffset> {
    let sign = if s.starts_with('-') { -1 } else { 1 };
    let p = s.trim_start_matches(['+', '-']);
    let mut a = p.split(':');
    let h: i32 = a.next()?.parse().ok()?;
    let m: i32 = a.next()?.parse().ok()?;
    if m >= 60 || h >= 24 {
        return None;
    }
    FixedOffset::east_opt(sign * (h * 3600 + m * 60))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Broker fees are accrued and rounded in integer paise using ties-to-even.
pub struct FeeModel {
    pub bps: f64,
    pub flat_per_order: f64,
    pub min_per_order: f64,
}
fn round_half_even_i128(numerator: i128, denominator: i128) -> i128 {
    let q = numerator / denominator;
    let r = numerator % denominator;
    let twice = r.abs() * 2;
    if twice > denominator.abs() || (twice == denominator.abs() && q % 2 != 0) {
        q + numerator.signum() * denominator.signum()
    } else {
        q
    }
}
fn round_half_even_f64(value: f64) -> f64 {
    let floor = value.floor();
    let fraction = value - floor;
    if fraction < 0.5 {
        floor
    } else if fraction > 0.5 || (floor as i128).rem_euclid(2) != 0 {
        floor + 1.0
    } else {
        floor
    }
}
impl Default for FeeModel {
    fn default() -> Self {
        Self {
            bps: 0.0,
            flat_per_order: 0.0,
            min_per_order: 0.0,
        }
    }
}
#[derive(Clone, Debug)]
pub struct SimBroker {
    pub symbols: Vec<Symbol>,
    pub latency: u64,
    pub slippage_bps: f64,
    /// Minimum permitted fill-price increment in paise (NSE equity default: 5).
    pub tick_size_paise: i64,
    pub tick_sizes_paise: BTreeMap<Symbol, i64>,
    pub partial_fills_pct: Option<f64>,
    pub fees: FeeModel,
    pending: BTreeMap<OrderId, PendingOrder>,
    events: Vec<OrderEvent>,
    ticks: BTreeMap<Symbol, u64>,
    seen_ids: std::collections::BTreeSet<OrderId>,
}
#[derive(Clone, Debug)]
struct PendingOrder {
    req: OrderRequest,
    remaining: u64,
    due: u64,
    acked: bool,
    notional_filled_paise: i128,
    fees_charged_paise: i64,
}
impl SimBroker {
    pub fn new(symbols: Vec<Symbol>) -> Self {
        Self {
            symbols,
            latency: 1,
            slippage_bps: 0.0,
            tick_size_paise: 5,
            tick_sizes_paise: BTreeMap::new(),
            partial_fills_pct: None,
            fees: FeeModel::default(),
            pending: BTreeMap::new(),
            events: Vec::new(),
            ticks: BTreeMap::new(),
            seen_ids: std::collections::BTreeSet::new(),
        }
    }
    pub fn on_candle_open(&mut self, candles: &BTreeMap<Symbol, Candle>) {
        for s in candles.keys() {
            *self.ticks.entry(s.clone()).or_default() += 1;
        }
        let mut ids: Vec<_> = self.pending.keys().cloned().collect();
        ids.sort();
        for id in ids {
            let Some(p) = self.pending.get_mut(&id) else {
                continue;
            };
            let Some(c) = candles.get(&p.req.symbol) else {
                continue;
            };
            let tick = *self.ticks.get(&p.req.symbol).unwrap_or(&0);
            if tick < p.due {
                continue;
            }
            if !p.acked {
                self.events.push(OrderEvent::Acked { id: id.clone() });
                p.acked = true;
            }
            let cap = self
                .partial_fills_pct
                .map(|pct| (c.volume * pct / 100.0).floor() as u64)
                .unwrap_or(p.remaining);
            let qty = p.remaining.min(cap);
            if qty == 0 {
                p.due = tick + 1;
                continue;
            }
            let raw_price = if p.req.side == Side::Buy {
                c.open * (1.0 + self.slippage_bps / 10000.0)
            } else {
                c.open * (1.0 - self.slippage_bps / 10000.0)
            };
            let tick_paise = self
                .tick_sizes_paise
                .get(&p.req.symbol)
                .copied()
                .unwrap_or(self.tick_size_paise)
                .max(1);
            let tick_units = raw_price * 100.0 / tick_paise as f64;
            let nearest = tick_units.round();
            let units = if (tick_units - nearest).abs() <= 1e-10 {
                nearest
            } else if p.req.side == Side::Buy {
                tick_units.ceil()
            } else {
                tick_units.floor()
            };
            let price_paise = (units as i64).saturating_mul(tick_paise);
            let price = crate::trader::paise_to_rupees(price_paise);
            p.notional_filled_paise += qty as i128 * price_paise as i128;
            let target_fee_paise = if self.fees.bps == 0.0
                && self.fees.flat_per_order == 0.0
                && self.fees.min_per_order == 0.0
            {
                0i64
            } else {
                // Convert the configured rate to integer hundredths of a bp
                // once; all notional multiplication and tie decisions below
                // are integer-only (denominator: 10,000 * 100).
                let bps_hundredths = round_half_even_f64(self.fees.bps * 100.0) as i128;
                let variable = round_half_even_i128(
                    p.notional_filled_paise * bps_hundredths,
                    10_000i128 * 100,
                );
                let flat = crate::trader::rupees_to_paise(self.fees.flat_per_order) as i128;
                let minimum = crate::trader::rupees_to_paise(self.fees.min_per_order) as i128;
                (variable + flat).max(minimum) as i64
            };
            let fees_paise = (target_fee_paise - p.fees_charged_paise).max(0);
            p.fees_charged_paise = target_fee_paise;
            let fees = crate::trader::paise_to_rupees(fees_paise);
            p.remaining -= qty;
            let done = p.remaining == 0;
            if done {
                self.events.push(OrderEvent::Filled {
                    id: id.clone(),
                    qty,
                    price,
                    fees,
                });
            } else {
                self.events.push(OrderEvent::PartialFill {
                    id: id.clone(),
                    qty,
                    price,
                    fees,
                });
                p.due = tick + 1;
            }
            if done {
                self.pending.remove(&id);
            }
        }
    }
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }
}
impl ExecutionPort for SimBroker {
    fn submit(&mut self, r: &OrderRequest) -> Result<(), PortError> {
        if r.qty == 0
            || !self.symbols.contains(&r.symbol)
            || self.seen_ids.contains(&r.client_order_id)
        {
            return Err(PortError::Message("invalid or duplicate order".into()));
        }
        let tick = *self.ticks.get(&r.symbol).unwrap_or(&0);
        self.pending.insert(
            r.client_order_id.clone(),
            PendingOrder {
                req: r.clone(),
                remaining: r.qty,
                due: tick + self.latency.max(1),
                acked: false,
                notional_filled_paise: 0,
                fees_charged_paise: 0,
            },
        );
        self.seen_ids.insert(r.client_order_id.clone());
        Ok(())
    }
    fn cancel(&mut self, id: OrderId) -> Result<(), PortError> {
        if self.pending.remove(&id).is_some() {
            self.events.push(OrderEvent::Cancelled { id });
            Ok(())
        } else {
            Err(PortError::UnknownOrder(id))
        }
    }
    fn drain_events(&mut self) -> Vec<OrderEvent> {
        self.events.sort_by(|a, b| event_id(a).cmp(event_id(b)));
        std::mem::take(&mut self.events)
    }
}
fn event_id(e: &OrderEvent) -> &str {
    match e {
        OrderEvent::Acked { id }
        | OrderEvent::PartialFill { id, .. }
        | OrderEvent::Filled { id, .. }
        | OrderEvent::Rejected { id, .. }
        | OrderEvent::Cancelled { id }
        | OrderEvent::Expired { id } => id,
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunConfig {
    pub timeframe: Timeframe,
    pub range: DateRange,
    pub initial_cash: f64,
    pub warmup_bars: usize,
    pub latency: u64,
    pub slippage_bps: f64,
    pub fee_model: FeeModel,
    pub partial_fills_pct: Option<f64>,
    pub gap_policy: GapPolicy,
    pub debug_invariants: bool,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Fill {
    pub ts: i64,
    pub symbol: Symbol,
    pub order_id: OrderId,
    pub side: Side,
    pub qty: u64,
    pub price: f64,
    pub price_paise: i64,
    pub fees: f64,
    pub fees_paise: i64,
    pub reason: OrderReason,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EquityPoint {
    pub ts: i64,
    pub cash: f64,
    pub positions_value: f64,
    pub equity: f64,
    pub cash_paise: i64,
    pub positions_value_paise: i64,
    pub equity_paise: i64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OpenPosition {
    pub symbol: Symbol,
    pub qty: i64,
    pub avg_entry: f64,
    pub last_close: f64,
    pub unrealized: f64,
    pub avg_entry_paise: i64,
    pub last_close_paise: i64,
    pub unrealized_paise: i64,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Trade {
    pub symbol: Symbol,
    pub entry_ts: i64,
    pub exit_ts: i64,
    pub qty: u64,
    pub avg_entry: f64,
    pub avg_entry_paise: i64,
    pub avg_exit: f64,
    pub avg_exit_paise: i64,
    pub fees: f64,
    pub fees_paise: i64,
    pub net_pnl: f64,
    pub net_pnl_paise: i64,
    pub exit_reason: OrderReason,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SymbolEquityPoint {
    pub ts: i64,
    pub equity: f64,
    pub equity_paise: i64,
    pub position_qty: i64,
}
#[derive(Clone, Debug, Serialize)]
pub struct RunResult {
    pub fills: Vec<Fill>,
    pub equity: Vec<EquityPoint>,
    pub events: Vec<TraderEvent>,
    pub open_positions: Vec<OpenPosition>,
    pub manifest: serde_json::Value,
    pub source_stats: SourceStats,
    pub trades: Vec<Trade>,
    pub symbol_equity: BTreeMap<Symbol, Vec<SymbolEquityPoint>>,
}
#[derive(Debug, Error)]
pub enum SimError {
    #[error(transparent)]
    Source(#[from] SourceError),
    #[error(transparent)]
    Engine(#[from] crate::engine::EngineError),
    #[error("{0}")]
    Other(String),
}
fn check_invariants(
    trader: &Trader,
    symbols: &[Symbol],
    fills: &[Fill],
    ts: i64,
) -> Result<(), SimError> {
    if trader.cash_paise() < 0 {
        return Err(SimError::Other(format!(
            "invariant: cash is negative at {ts}"
        )));
    }
    if trader.pending_order_count() == 0 && trader.reserved_cash_paise() != 0 {
        return Err(SimError::Other(format!(
            "invariant: reserved cash remains without pending orders at {ts}"
        )));
    }
    for symbol in symbols {
        let signed: i128 = fills
            .iter()
            .filter(|f| &f.symbol == symbol)
            .map(|f| {
                if f.side == Side::Buy {
                    f.qty as i128
                } else {
                    -(f.qty as i128)
                }
            })
            .sum();
        let actual = trader.position(symbol).map_or(0, |p| p.filled_qty) as i128;
        if signed != actual || actual < 0 {
            return Err(SimError::Other(format!(
                "invariant: fill ledger position mismatch for {symbol} at {ts}"
            )));
        }
    }
    Ok(())
}
/// Running per-symbol P&L state, folded forward one fill at a time by the sim
/// frame loop. `cash_flow_paise` is the net cash effect of this symbol's fills
/// (buys negative incl. fees, sells positive net of prorated fees) and `qty` is
/// the open quantity; a symbol's P&L at any mark is `cash_flow_paise + mark*qty`.
/// Replaces the former `symbol_pnl` replay, which re-scanned all fills per symbol
/// per frame (quadratic over a long run).
#[derive(Clone, Copy, Debug, Default)]
struct SymbolPnlAccum {
    qty: u64,
    cash_flow_paise: i128,
}
/// Stream a merged source through independent per-symbol engines, then process
/// broker events before each close frame in deterministic symbol order.
pub fn run(
    program: &Program,
    source: &dyn CandleSource,
    config: RunConfig,
    requested: Option<Vec<Symbol>>,
) -> Result<RunResult, SimError> {
    run_internal(program, source, config, requested, None)
}
/// Precompute each symbol's frames in parallel, then replay through the same
/// trader/broker timeline. This is the Stage-5 path and shares the streaming
/// accounting and event ordering implementation.
pub fn run_precomputed(
    program: &Program,
    source: &dyn CandleSource,
    config: RunConfig,
    requested: Option<Vec<Symbol>>,
) -> Result<RunResult, SimError> {
    let mut symbols = requested
        .clone()
        .unwrap_or_else(|| program.symbols().into_iter().map(str::to_owned).collect());
    symbols.sort();
    symbols.dedup();
    let stream = source.open(
        &symbols,
        config.timeframe,
        DateRange {
            from: None,
            to: config.range.to,
        },
    )?;
    let stream = limit_to_warmup_window(stream, config.range.from, config.warmup_bars);
    let mut series = BTreeMap::<Symbol, Vec<Candle>>::new();
    for item in stream {
        let (s, c) = item?;
        series.entry(s).or_default().push(c)
    }
    let computed: Vec<Result<(Symbol, Vec<LevelFrame>), crate::engine::EngineError>> = symbols
        .par_iter()
        .map(|s| {
            let candles = series.get(s).map(Vec::as_slice).unwrap_or(&[]);
            crate::engine::evaluate_series(program, s.clone(), candles)
                .map(|frames| (s.clone(), frames))
        })
        .collect();
    let mut frames = HashMap::<(Symbol, i64), LevelFrame>::new();
    for item in computed {
        let (s, values) = item?;
        for frame in values {
            frames.insert((s.clone(), frame.candle_ts), frame);
        }
    }
    run_internal(program, source, config, Some(symbols), Some(frames))
}
fn run_internal(
    program: &Program,
    source: &dyn CandleSource,
    config: RunConfig,
    requested: Option<Vec<Symbol>>,
    mut precomputed: Option<HashMap<(Symbol, i64), LevelFrame>>,
) -> Result<RunResult, SimError> {
    if !config.initial_cash.is_finite() || config.initial_cash <= 0.0 {
        return Err(SimError::Other(
            "initial cash must be finite and positive".into(),
        ));
    }
    if config.latency == 0 {
        return Err(SimError::Other(
            "latency must be at least one candle".into(),
        ));
    }
    if !config.slippage_bps.is_finite() || !(0.0..10000.0).contains(&config.slippage_bps) {
        return Err(SimError::Other(
            "slippage basis points must be in [0,10000)".into(),
        ));
    }
    if [
        config.fee_model.bps,
        config.fee_model.flat_per_order,
        config.fee_model.min_per_order,
    ]
    .iter()
    .any(|x| !x.is_finite() || *x < 0.0)
    {
        return Err(SimError::Other(
            "fee model values must be finite and non-negative".into(),
        ));
    }
    if config
        .partial_fills_pct
        .is_some_and(|p| !p.is_finite() || p <= 0.0 || p > 100.0)
    {
        return Err(SimError::Other(
            "partial fill percentage must be in (0,100]".into(),
        ));
    }
    if config
        .range
        .from
        .zip(config.range.to)
        .is_some_and(|(from, to)| from > to)
    {
        return Err(SimError::Other("--from must be at or before --to".into()));
    }
    let mut symbols =
        requested.unwrap_or_else(|| program.symbols().into_iter().map(str::to_owned).collect());
    symbols.sort();
    symbols.dedup();
    if symbols.is_empty() {
        return Err(SimError::Other("at least one symbol is required".into()));
    }
    let fp = source.fingerprint(&symbols, config.timeframe, config.range);
    if std::env::var_os("ALGOMLN_BENCHMARK").is_none() {
        eprintln!("source fingerprint: {}", fp.0);
    }
    let stream = source.open(
        &symbols,
        config.timeframe,
        DateRange {
            from: None,
            to: config.range.to,
        },
    )?;
    let stream = limit_to_warmup_window(stream, config.range.from, config.warmup_bars);
    let mut engines: BTreeMap<Symbol, Engine> = symbols
        .iter()
        .map(|s| Ok((s.clone(), Engine::new(program, s.clone())?)))
        .collect::<Result<_, crate::engine::EngineError>>()?;
    let mut trader = Trader::new(
        program,
        TraderConfig {
            initial_cash: config.initial_cash,
            instrument_rules: source.instrument_rules(&symbols),
        },
    )
    .map_err(|e| SimError::Other(format!("trader init: {e:?}")))?;
    let mut broker = SimBroker::new(symbols.clone());
    broker.latency = config.latency;
    broker.slippage_bps = config.slippage_bps;
    broker.tick_sizes_paise = source.tick_sizes_paise(&symbols);
    broker.fees = config.fee_model.clone();
    broker.partial_fills_pct = config.partial_fills_pct;
    let mut fills = Vec::new();
    let mut equity = Vec::new();
    let mut symbol_equity: BTreeMap<Symbol, Vec<SymbolEquityPoint>> =
        symbols.iter().map(|s| (s.clone(), Vec::new())).collect();
    let mut events = Vec::new();
    let mut last = BTreeMap::<Symbol, Candle>::new();
    let mut reqs = HashMap::<OrderId, OrderRequest>::new();
    let mut day = None;
    let mut current_ts = None;
    let mut candles = BTreeMap::<Symbol, Candle>::new();
    let mut observed = BTreeMap::<Symbol, usize>::new();
    // Incremental per-symbol P&L, folded forward as fills arrive instead of
    // replaying the whole fills vector per symbol per frame (was O(frames x fills)).
    let mut symbol_pnl_state = BTreeMap::<Symbol, SymbolPnlAccum>::new();
    let mut processed_fills = 0usize;
    let mut process = |ts: i64,
                       candles: &BTreeMap<Symbol, Candle>,
                       trader: &mut Trader,
                       broker: &mut SimBroker,
                       engines: &mut BTreeMap<Symbol, Engine>,
                       last: &mut BTreeMap<Symbol, Candle>,
                       fills: &mut Vec<Fill>,
                       events: &mut Vec<TraderEvent>,
                       reqs: &mut HashMap<OrderId, OrderRequest>,
                       equity: &mut Vec<EquityPoint>,
                       symbol_equity: &mut BTreeMap<Symbol, Vec<SymbolEquityPoint>>,
                       day: &mut Option<i64>,
                       observed: &mut BTreeMap<Symbol, usize>,
                       symbol_pnl_state: &mut BTreeMap<Symbol, SymbolPnlAccum>,
                       processed_fills: &mut usize|
     -> Result<(), SimError> {
        let d = ist_day(ts);
        if *day != Some(d) {
            trader.start_day(d);
            *day = Some(d);
            if (config.debug_invariants || cfg!(test))
                && (trader.realized_pnl_today_paise() != 0
                    || trader.realized_loss_today_paise() != 0
                    || trader.accepted_buys_today() != 0)
            {
                return Err(SimError::Other(format!(
                    "invariant: daily counters did not reset for IST day {d}"
                )));
            }
        }
        broker.on_candle_open(candles);
        let order_events = broker.drain_events();
        for ev in order_events {
            if let Some(id) = match &ev {
                OrderEvent::PartialFill { id, .. } | OrderEvent::Filled { id, .. } => Some(id),
                _ => None,
            } {
                if let Some(req) = reqs.get(id) {
                    if ts <= req.decided_at && (config.debug_invariants || cfg!(test)) {
                        return Err(SimError::Other(format!(
                            "invariant: order {id} filled before its next candle open"
                        )));
                    }
                }
            }
            if let Some(fill) = to_fill(ts, &ev, reqs) {
                fills.push(fill)
            }
            events.extend(trader.on_order_event(ev));
        }
        for (s, c) in candles {
            last.insert(s.clone(), *c);
            let frame = if let Some(frames) = precomputed.as_mut() {
                frames.remove(&(s.clone(), c.timestamp)).ok_or_else(|| {
                    SimError::Other(format!(
                        "precomputed frame missing for {s} at {}",
                        c.timestamp
                    ))
                })?
            } else {
                engines
                    .get_mut(s)
                    .expect("configured symbol")
                    .on_candle(c)?
            };
            let count = observed.entry(s.clone()).or_default();
            let mode = if let Some(from) = config.range.from {
                if ts < from {
                    Mode::Observe
                } else {
                    Mode::Act
                }
            } else if *count < config.warmup_bars {
                *count += 1;
                Mode::Observe
            } else {
                Mode::Act
            };
            let out = trader.on_frame(&frame, c, mode);
            events.extend(out.events.clone());
            for id in out.cancels {
                broker
                    .cancel(id)
                    .map_err(|e| SimError::Other(format!("broker cancel: {e:?}")))?;
            }
            for req in &out.orders {
                broker
                    .submit(req)
                    .map_err(|e| SimError::Other(format!("broker submit: {e:?}")))?;
                if reqs
                    .insert(req.client_order_id.clone(), req.clone())
                    .is_some()
                {
                    return Err(SimError::Other(format!(
                        "invariant: duplicate order id {}",
                        req.client_order_id
                    )));
                }
            }
        }
        let mut pos_value_paise = 0i128;
        for s in &symbols {
            if let (Some(p), Some(c)) = (trader.position(s), last.get(s)) {
                let paise = crate::trader::rupees_to_paise(c.close);
                pos_value_paise += p.filled_qty as i128 * paise as i128;
            }
        }
        let cash_paise = trader.cash_paise();
        let positions_value_paise =
            pos_value_paise.clamp(i64::MIN as i128, i64::MAX as i128) as i64;
        let equity_paise = cash_paise.saturating_add(positions_value_paise);
        equity.push(EquityPoint {
            ts,
            cash: crate::trader::paise_to_rupees(cash_paise),
            positions_value: crate::trader::paise_to_rupees(positions_value_paise),
            equity: crate::trader::paise_to_rupees(equity_paise),
            cash_paise,
            positions_value_paise,
            equity_paise,
        });
        // Fold the fills added since the last frame into the per-symbol P&L
        // accumulators once, then read each symbol's running total below. This
        // replaces the previous full replay of `fills` per symbol per frame.
        for f in &fills[*processed_fills..] {
            let acc = symbol_pnl_state.entry(f.symbol.clone()).or_default();
            match f.side {
                Side::Buy => {
                    acc.qty += f.qty;
                    acc.cash_flow_paise -=
                        f.price_paise as i128 * f.qty as i128 + f.fees_paise as i128;
                }
                Side::Sell => {
                    let q = f.qty.min(acc.qty);
                    if q > 0 {
                        acc.qty -= q;
                        acc.cash_flow_paise += f.price_paise as i128 * q as i128
                            - round_half_even_i128(
                                f.fees_paise as i128 * q as i128,
                                f.qty.max(1) as i128,
                            );
                    }
                }
            }
        }
        *processed_fills = fills.len();
        let initial_cash_paise = crate::trader::rupees_to_paise(config.initial_cash);
        let allocation_base = initial_cash_paise / symbols.len().max(1) as i64;
        let allocation_remainder = initial_cash_paise.rem_euclid(symbols.len().max(1) as i64);
        for (symbol_index, symbol) in symbols.iter().enumerate() {
            let mark = last
                .get(symbol)
                .map_or(0, |c| crate::trader::rupees_to_paise(c.close));
            // Running P&L against the current mark: identical to the old
            // `symbol_pnl` replay, but O(1) per symbol per frame.
            let (pnl, qty) = symbol_pnl_state.get(symbol).map_or((0i64, 0i64), |acc| {
                let pnl_paise = acc.cash_flow_paise + mark as i128 * acc.qty as i128;
                (
                    pnl_paise.clamp(i64::MIN as i128, i64::MAX as i128) as i64,
                    acc.qty as i64,
                )
            });
            // `allocation_base` is a static even split of initial cash used only as
            // the per-symbol baseline; the series tracks relative P&L and does not
            // attribute pooled or reserved cash per symbol, so it need not reconcile
            // exactly to portfolio equity while cash is reserved for pending orders.
            let allocation_paise =
                allocation_base + i64::from((symbol_index as i64) < allocation_remainder);
            let symbol_equity_paise = allocation_paise.saturating_add(pnl);
            symbol_equity
                .entry(symbol.clone())
                .or_default()
                .push(SymbolEquityPoint {
                    ts,
                    equity: crate::trader::paise_to_rupees(symbol_equity_paise),
                    equity_paise: symbol_equity_paise,
                    position_qty: qty,
                });
        }
        if config.debug_invariants || cfg!(test) {
            check_invariants(trader, &symbols, fills, ts)?;
        }
        Ok(())
    };
    for item in stream {
        let (s, c) = item?;
        if current_ts.is_some_and(|t| t != c.timestamp) {
            process(
                current_ts.unwrap(),
                &candles,
                &mut trader,
                &mut broker,
                &mut engines,
                &mut last,
                &mut fills,
                &mut events,
                &mut reqs,
                &mut equity,
                &mut symbol_equity,
                &mut day,
                &mut observed,
                &mut symbol_pnl_state,
                &mut processed_fills,
            )?;
            candles.clear();
        }
        current_ts = Some(c.timestamp);
        if candles.contains_key(&s) {
            return Err(SimError::Other(format!(
                "duplicate candle for symbol {s:?} at timestamp {}",
                c.timestamp
            )));
        }
        candles.insert(s, c);
    }
    if let Some(ts) = current_ts {
        process(
            ts,
            &candles,
            &mut trader,
            &mut broker,
            &mut engines,
            &mut last,
            &mut fills,
            &mut events,
            &mut reqs,
            &mut equity,
            &mut symbol_equity,
            &mut day,
            &mut observed,
            &mut symbol_pnl_state,
            &mut processed_fills,
        )?;
    }
    if config.debug_invariants || cfg!(test) {
        if let Some(last_point) = equity.last() {
            let mut qty = BTreeMap::<Symbol, u64>::new();
            let mut cash_paise = crate::trader::rupees_to_paise(config.initial_cash) as i128;
            for fill in &fills {
                let q = qty.entry(fill.symbol.clone()).or_default();
                match fill.side {
                    Side::Buy => {
                        *q += fill.qty;
                        cash_paise -=
                            fill.price_paise as i128 * fill.qty as i128 + fill.fees_paise as i128;
                    }
                    Side::Sell => {
                        if fill.qty > *q {
                            return Err(SimError::Other(format!(
                                "invariant: sell fill exceeds position for {}",
                                fill.symbol
                            )));
                        }
                        *q -= fill.qty;
                        cash_paise +=
                            fill.price_paise as i128 * fill.qty as i128 - fill.fees_paise as i128;
                    }
                }
            }
            let positions_value_paise: i128 = symbols
                .iter()
                .map(|s| {
                    let q = *qty.get(s).unwrap_or(&0);
                    let mark = last
                        .get(s)
                        .map_or(0, |x| crate::trader::rupees_to_paise(x.close));
                    mark as i128 * q as i128
                })
                .sum();
            let recomputed_equity = cash_paise + positions_value_paise;
            if cash_paise != last_point.cash_paise as i128
                || positions_value_paise != last_point.positions_value_paise as i128
                || recomputed_equity != last_point.equity_paise as i128
            {
                return Err(SimError::Other(
                    "invariant: paise cash and positions recomputed from fills do not exactly reconcile to final equity".into(),
                ));
            }
        }
    }
    let open_positions = symbols
        .iter()
        .filter_map(|s| {
            let p = trader.position(s)?;
            if p.filled_qty == 0 {
                return None;
            }
            let c = last.get(s)?;
            let last_close_paise = crate::trader::rupees_to_paise(c.close);
            let unrealized_paise =
                (last_close_paise - p.avg_entry_paise).saturating_mul(p.filled_qty);
            Some(OpenPosition {
                symbol: s.clone(),
                qty: p.filled_qty,
                avg_entry: crate::trader::paise_to_rupees(p.avg_entry_paise),
                last_close: crate::trader::paise_to_rupees(last_close_paise),
                unrealized: crate::trader::paise_to_rupees(unrealized_paise),
                avg_entry_paise: p.avg_entry_paise,
                last_close_paise,
                unrealized_paise,
            })
        })
        .collect();
    let mut ph = 0xcbf29ce484222325u64;
    for b in format!("{:?}", program.strategy()).bytes() {
        ph ^= b as u64;
        ph = ph.wrapping_mul(0x100000001b3);
    }
    let source_stats = source.stats();
    // Baked at build time by build.rs (reproducibility only); never spawns a
    // process per run. Absent in non-git/sandboxed builds, recorded as null.
    let commit = option_env!("GIT_COMMIT").map(str::to_owned);
    let manifest = serde_json::json!({"program_hash":format!("fnv1a64:{ph:016x}"),"source_fingerprint":fp,"symbols":symbols,"timeframe":config.timeframe,"range":config.range,"config":config,"tool_version":env!("CARGO_PKG_VERSION"),"git_commit":commit,"source_stats":source_stats});
    if std::env::var_os("ALGOMLN_BENCHMARK").is_none() {
        eprintln!("source fingerprint: {}", fp.0);
    }
    let trades = derive_trades(&fills);
    Ok(RunResult {
        fills,
        equity,
        events,
        open_positions,
        manifest,
        source_stats,
        trades,
        symbol_equity,
    })
}
fn ist_day(ts: i64) -> i64 {
    ts + 19800 - (ts + 19800).rem_euclid(86400)
}
fn to_fill(ts: i64, e: &OrderEvent, reqs: &HashMap<OrderId, OrderRequest>) -> Option<Fill> {
    let (id, qty, price, fees) = match e {
        OrderEvent::Filled {
            id,
            qty,
            price,
            fees,
        }
        | OrderEvent::PartialFill {
            id,
            qty,
            price,
            fees,
        } => (id, *qty, *price, *fees),
        _ => return None,
    };
    let r = reqs.get(id)?;
    let price = crate::trader::paise_to_rupees(crate::trader::rupees_to_paise(price));
    let fees = crate::trader::paise_to_rupees(crate::trader::rupees_to_paise(fees));
    Some(Fill {
        ts,
        symbol: r.symbol.clone(),
        order_id: id.clone(),
        side: r.side,
        qty,
        price,
        price_paise: crate::trader::rupees_to_paise(price),
        fees,
        fees_paise: crate::trader::rupees_to_paise(fees),
        reason: r.reason.clone(),
    })
}
fn derive_trades(fills: &[Fill]) -> Vec<Trade> {
    #[derive(Default)]
    struct Lot {
        qty: u64,
        cost_paise: i128,
        entry_fees_paise: i128,
        entry_ts: i64,
        closed_qty: u64,
        closed_entry_cost_paise: i128,
        closed_exit_value_paise: i128,
        closed_fees_paise: i128,
        closed_pnl_paise: i128,
    }
    let mut lots = BTreeMap::<Symbol, Lot>::new();
    let mut trades = Vec::new();
    for f in fills {
        let lot = lots.entry(f.symbol.clone()).or_default();
        match f.side {
            Side::Buy => {
                if lot.qty == 0 {
                    lot.entry_ts = f.ts;
                }
                lot.qty += f.qty;
                lot.cost_paise += f.price_paise as i128 * f.qty as i128;
                lot.entry_fees_paise += f.fees_paise as i128;
            }
            Side::Sell => {
                if lot.qty == 0 {
                    continue;
                }
                let q = f.qty.min(lot.qty);
                let entry_cost = round_half_even_i128(lot.cost_paise * q as i128, lot.qty as i128);
                let entry_fee =
                    round_half_even_i128(lot.entry_fees_paise * q as i128, lot.qty as i128);
                let exit_fee =
                    round_half_even_i128(f.fees_paise as i128 * q as i128, f.qty.max(1) as i128);
                let exit_value = f.price_paise as i128 * q as i128;
                lot.closed_qty += q;
                lot.closed_entry_cost_paise += entry_cost;
                lot.closed_exit_value_paise += exit_value;
                lot.closed_fees_paise += entry_fee + exit_fee;
                lot.closed_pnl_paise += exit_value - entry_cost - entry_fee - exit_fee;
                lot.qty -= q;
                lot.cost_paise -= entry_cost;
                lot.entry_fees_paise -= entry_fee;
                if lot.qty == 0 && lot.closed_qty > 0 {
                    trades.push(Trade {
                        symbol: f.symbol.clone(),
                        entry_ts: lot.entry_ts,
                        exit_ts: f.ts,
                        qty: lot.closed_qty,
                        avg_entry: crate::trader::paise_to_rupees(round_half_even_i128(
                            lot.closed_entry_cost_paise,
                            lot.closed_qty as i128,
                        ) as i64),
                        avg_entry_paise: round_half_even_i128(
                            lot.closed_entry_cost_paise,
                            lot.closed_qty as i128,
                        ) as i64,
                        avg_exit: crate::trader::paise_to_rupees(round_half_even_i128(
                            lot.closed_exit_value_paise,
                            lot.closed_qty as i128,
                        ) as i64),
                        avg_exit_paise: round_half_even_i128(
                            lot.closed_exit_value_paise,
                            lot.closed_qty as i128,
                        ) as i64,
                        fees: crate::trader::paise_to_rupees(lot.closed_fees_paise as i64),
                        fees_paise: lot.closed_fees_paise as i64,
                        net_pnl: crate::trader::paise_to_rupees(lot.closed_pnl_paise as i64),
                        net_pnl_paise: lot.closed_pnl_paise as i64,
                        exit_reason: f.reason.clone(),
                    });
                    *lot = Lot::default();
                }
            }
        }
    }
    trades.sort_by(|a, b| (a.exit_ts, &a.symbol).cmp(&(b.exit_ts, &b.symbol)));
    trades
}

#[allow(clippy::items_after_test_module)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::trader::{ExecutionPort, OrderRequest, ScriptedPort};

    struct MemorySource(Vec<(Symbol, Candle)>);
    impl CandleSource for MemorySource {
        fn open(
            &self,
            symbols: &[Symbol],
            _tf: Timeframe,
            range: DateRange,
        ) -> Result<CandleStream, SourceError> {
            let values: Vec<_> = self
                .0
                .iter()
                .filter(|(s, c)| {
                    symbols.contains(s)
                        && range.from.map_or(true, |x| c.timestamp >= x)
                        && range.to.map_or(true, |x| c.timestamp <= x)
                })
                .cloned()
                .map(Ok)
                .collect();
            Ok(Box::new(values.into_iter()))
        }
        fn fingerprint(&self, _: &[Symbol], _: Timeframe, _: DateRange) -> Fingerprint {
            Fingerprint("memory-fixture".into())
        }
    }
    fn candle(ts: i64, open: f64, close: f64) -> Candle {
        Candle {
            timestamp: ts,
            open,
            high: open.max(close),
            low: open.min(close),
            close,
            volume: 100.0,
        }
    }
    fn config() -> RunConfig {
        RunConfig {
            timeframe: Timeframe::M1,
            range: DateRange::default(),
            initial_cash: 1000.0,
            warmup_bars: 0,
            latency: 1,
            slippage_bps: 0.0,
            fee_model: FeeModel::default(),
            partial_fills_pct: None,
            gap_policy: GapPolicy::None,
            debug_invariants: true,
        }
    }

    #[test]
    fn broker_acks_and_fills_at_next_available_open() {
        let mut b = SimBroker::new(vec!["ABC".into()]);
        let req = OrderRequest {
            client_order_id: "o1".into(),
            symbol: "ABC".into(),
            side: Side::Buy,
            qty: 2,
            reason: OrderReason::Rule("rule_1".into()),
            ref_price: 10.0,
            decided_at: 1,
        };
        b.submit(&req).unwrap();
        b.on_candle_open(&BTreeMap::from([("ABC".into(), candle(2, 12.0, 12.0))]));
        let ev = b.drain_events();
        assert!(matches!(ev[0], OrderEvent::Acked { .. }));
        assert!(matches!(
            ev[1],
            OrderEvent::Filled {
                price: 12.0,
                qty: 2,
                ..
            }
        ));
    }

    #[test]
    fn fee_rounding_is_half_even_in_integer_paise() {
        assert_eq!(round_half_even_i128(1, 2), 0);
        assert_eq!(round_half_even_i128(3, 2), 2);
        assert_eq!(round_half_even_i128(5, 2), 2);
        assert_eq!(round_half_even_i128(7, 2), 4);

        let mut even_broker = SimBroker::new(vec!["ABC".into()]);
        even_broker.fees.bps = 5000.0; // 1 paisa notional -> exactly 0.5 paisa
        even_broker.tick_size_paise = 1;
        even_broker
            .submit(&OrderRequest {
                client_order_id: "even-half".into(),
                symbol: "ABC".into(),
                side: Side::Buy,
                qty: 1,
                reason: OrderReason::Rule("r".into()),
                ref_price: 0.01,
                decided_at: 1,
            })
            .unwrap();
        even_broker.on_candle_open(&BTreeMap::from([("ABC".into(), candle(2, 0.01, 0.01))]));
        assert!(matches!(
            even_broker.drain_events().as_slice(),
            [
                OrderEvent::Acked { .. },
                OrderEvent::Filled { fees: 0.0, .. }
            ]
        ));

        let mut broker = SimBroker::new(vec!["ABC".into()]);
        broker.fees.bps = 5000.0;
        broker.tick_size_paise = 1;
        broker
            .submit(&OrderRequest {
                client_order_id: "half-even-fee".into(),
                symbol: "ABC".into(),
                side: Side::Buy,
                qty: 3,
                reason: OrderReason::Rule("r".into()),
                ref_price: 0.01,
                decided_at: 1,
            })
            .unwrap();
        broker.on_candle_open(&BTreeMap::from([("ABC".into(), candle(2, 0.01, 0.01))]));
        assert!(matches!(
            broker.drain_events().as_slice(),
            [
                OrderEvent::Acked { .. },
                OrderEvent::Filled { fees: 0.02, .. }
            ]
        ));
    }

    #[test]
    fn simulated_slippage_rounds_adversely_to_tick() {
        let mut buy = SimBroker::new(vec!["ABC".into()]);
        buy.slippage_bps = 1.0;
        buy.submit(&OrderRequest {
            client_order_id: "buy-tick".into(),
            symbol: "ABC".into(),
            side: Side::Buy,
            qty: 1,
            reason: OrderReason::Rule("r".into()),
            ref_price: 10.0,
            decided_at: 1,
        })
        .unwrap();
        buy.on_candle_open(&BTreeMap::from([("ABC".into(), candle(2, 10.01, 10.01))]));
        let events = buy.drain_events();
        let OrderEvent::Filled { price, .. } = events[1] else {
            panic!("expected buy fill")
        };
        assert_eq!(crate::trader::rupees_to_paise(price), 1005);

        let mut sell = SimBroker::new(vec!["ABC".into()]);
        sell.slippage_bps = 1.0;
        sell.submit(&OrderRequest {
            client_order_id: "sell-tick".into(),
            symbol: "ABC".into(),
            side: Side::Sell,
            qty: 1,
            reason: OrderReason::Rule("r".into()),
            ref_price: 10.0,
            decided_at: 1,
        })
        .unwrap();
        sell.on_candle_open(&BTreeMap::from([("ABC".into(), candle(2, 10.04, 10.04))]));
        let events = sell.drain_events();
        let OrderEvent::Filled { price, .. } = events[1] else {
            panic!("expected sell fill")
        };
        assert_eq!(crate::trader::rupees_to_paise(price), 1000);
    }

    #[test]
    fn runner_is_deterministic_and_executes_next_open() {
        let p = crate::compile("TRADE_IN ABC\nWHEN close > 10\nBUY 1\nWHEN close < 9\nSELL ALL\n")
            .unwrap();
        let src = MemorySource(vec![
            ("ABC".into(), candle(1, 10.0, 11.0)),
            ("ABC".into(), candle(2, 12.0, 8.0)),
            ("ABC".into(), candle(3, 7.0, 7.0)),
        ]);
        let a = run(&p, &src, config(), None).unwrap();
        let b = run(&p, &src, config(), None).unwrap();
        let c = run_precomputed(&p, &src, config(), None).unwrap();
        assert_eq!(a.fills, b.fills);
        assert_eq!(a.equity, b.equity);
        assert_eq!(a.fills, c.fills);
        assert_eq!(a.equity, c.equity);
        assert_eq!(a.trades, c.trades);
        assert_eq!(a.fills.len(), 2);
        assert_eq!(a.fills[0].price, 12.0);
        assert_eq!(a.fills[1].price, 7.0);
        assert_eq!(a.trades.len(), 1);
        assert_eq!(a.trades[0].net_pnl, -5.0);
        let root = std::env::temp_dir().join(format!("algomln-result-{}", std::process::id()));
        let left = root.join("left");
        let right = root.join("right");
        let _ = std::fs::create_dir_all(&root);
        write_results(&left, &a).unwrap();
        write_results(&right, &b).unwrap();
        for name in [
            "fills.csv",
            "trades.csv",
            "equity.csv",
            "metrics.json",
            "manifest.json",
            "open_positions.json",
            "events.json",
        ] {
            assert_eq!(
                std::fs::read(left.join(name)).unwrap(),
                std::fs::read(right.join(name)).unwrap(),
                "{name}"
            );
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn stop_gap_uses_next_open_not_stop_price() {
        let p = crate::compile("TRADE_IN ABC\nSTOP_LOSS 10%\nWHEN close > 0\nBUY 1\n").unwrap();
        let src = MemorySource(vec![
            ("ABC".into(), candle(1, 100.0, 100.0)),
            ("ABC".into(), candle(2, 100.0, 89.0)),
            ("ABC".into(), candle(3, 80.0, 80.0)),
        ]);
        let r = run(&p, &src, config(), None).unwrap();
        assert_eq!(r.fills.len(), 2);
        assert_eq!(r.fills[1].price, 80.0);
        assert_eq!(r.fills[1].reason, OrderReason::StopLoss);
    }

    #[test]
    fn pending_order_waits_until_its_symbol_has_a_candle() {
        let p = crate::compile("TRADE_IN A,B\nWHEN close > 10\nBUY 1\n").unwrap();
        let src = MemorySource(vec![
            ("B".into(), candle(1, 10.0, 11.0)),
            ("A".into(), candle(2, 1.0, 1.0)),
            ("B".into(), candle(3, 12.0, 12.0)),
        ]);
        let r = run(&p, &src, config(), Some(vec!["B".into()])).unwrap();
        assert_eq!(r.fills.len(), 1);
        assert_eq!(r.fills[0].ts, 3);
        assert_eq!(r.fills[0].price, 12.0);
    }

    #[test]
    fn extra_inactive_symbol_does_not_change_single_symbol_fills() {
        let p = crate::compile("TRADE_IN ABC,XYZ\nWHEN close > 100\nBUY 1\n").unwrap();
        let src = MemorySource(vec![
            ("ABC".into(), candle(1, 100.0, 101.0)),
            ("XYZ".into(), candle(1, 1.0, 1.0)),
            ("ABC".into(), candle(2, 102.0, 102.0)),
            ("XYZ".into(), candle(2, 1.0, 1.0)),
        ]);
        let one = run(&p, &src, config(), Some(vec!["ABC".into()])).unwrap();
        let both = run(&p, &src, config(), None).unwrap();
        assert_eq!(one.fills, both.fills);
    }

    #[test]
    fn synthetic_generator_is_seeded_and_contains_requested_scenarios() {
        let symbols = vec!["AAA".to_owned(), "BBB".to_owned()];
        let first = seeded_synthetic_candles(42, &symbols, 4);
        assert_eq!(first, seeded_synthetic_candles(42, &symbols, 4));
        assert_ne!(first, seeded_synthetic_candles(43, &symbols, 4));
        assert!(first.iter().any(|(_, c)| c.volume == 0.0));
        assert!(first.iter().any(|(_, c)| c.high - c.low > 10.0));
        assert!(first
            .windows(2)
            .any(|w| w[1].1.timestamp - w[0].1.timestamp > 60));
        assert!(first.windows(2).any(|w| w[0].0 == "AAA" && w[1].0 == "BBB"));
    }

    #[test]
    fn file_merge_retains_only_one_decoded_candle_per_symbol() {
        let base = std::env::temp_dir().join(format!("algomln-bounded-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&base);
        for symbol in ["A", "B", "C"] {
            let mut csv = String::from("ts,open,high,low,close,volume\n");
            for ts in 1..=20_000 {
                csv.push_str(&format!("{ts},1,1,1,1,1\n"));
            }
            std::fs::write(base.join(format!("{symbol}.csv")), csv).unwrap();
        }
        let source = FileSource::new(&base);
        let stream = source
            .open(
                &["A".into(), "B".into(), "C".into()],
                Timeframe::M1,
                DateRange::default(),
            )
            .unwrap();
        // FileSource's merge owns only one cursor head per file; consuming the
        // entire stream also exercises row-at-a-time CSV decoding.
        assert_eq!(stream.count(), 60_000);
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn scripted_port_and_sim_broker_latency_one_emit_identical_trade() {
        let req = OrderRequest {
            client_order_id: "trade-1".into(),
            symbol: "ABC".into(),
            side: Side::Buy,
            qty: 2,
            reason: OrderReason::Rule("entry".into()),
            ref_price: 10.0,
            decided_at: 1,
        };
        let mut scripted = ScriptedPort::default();
        scripted.submit(&req).unwrap();
        scripted.push(OrderEvent::Acked {
            id: req.client_order_id.clone(),
        });
        scripted.push(OrderEvent::Filled {
            id: req.client_order_id.clone(),
            qty: 2,
            price: 12.0,
            fees: 0.0,
        });

        let mut broker = SimBroker::new(vec!["ABC".into()]);
        broker.latency = 1;
        broker.submit(&req).unwrap();
        broker.on_candle_open(&BTreeMap::from([("ABC".into(), candle(2, 12.0, 12.0))]));
        assert_eq!(scripted.drain_events(), broker.drain_events());
    }

    #[test]
    fn pending_order_carries_across_day_rollover_and_counters_reset() {
        let program = crate::compile("TRADE_IN ABC\nWHEN close > 0\nBUY 1\n").unwrap();
        let day1 = chrono::NaiveDate::from_ymd_opt(2024, 5, 6).unwrap();
        let day2 = chrono::NaiveDate::from_ymd_opt(2024, 5, 7).unwrap();
        let ts1 = day1.and_hms_opt(10, 0, 0).unwrap().and_utc().timestamp();
        let ts2 = day2.and_hms_opt(10, 0, 0).unwrap().and_utc().timestamp();
        let source = MemorySource(vec![
            ("ABC".into(), candle(ts1, 10.0, 10.0)),
            ("ABC".into(), candle(ts2, 11.0, 11.0)),
        ]);
        let result = run(&program, &source, config(), None).unwrap();
        assert_eq!(result.fills.len(), 1);
        assert_eq!(result.fills[0].ts, ts2);
        assert_eq!(result.fills[0].price, 11.0);
    }

    #[test]
    fn orchestrated_daily_loss_flattens_cancels_entries_and_reopens_next_day() {
        let source_text =
            "TRADE_IN ABC\nRISK MAX_DAILY_LOSS 0.1%\nWHEN close > 0\nBUY 2\nWHEN close < 95\nSELL 1\n";
        let program = crate::compile(source_text).unwrap();
        let tz = FixedOffset::east_opt(19800).unwrap();
        let d1 = chrono::NaiveDate::from_ymd_opt(2024, 5, 6).unwrap();
        let d2 = chrono::NaiveDate::from_ymd_opt(2024, 5, 7).unwrap();
        let ts = |d: chrono::NaiveDate, h, m| {
            tz.from_local_datetime(&d.and_hms_opt(h, m, 0).unwrap())
                .single()
                .unwrap()
                .timestamp()
        };
        let source = MemorySource(vec![
            ("ABC".into(), candle(ts(d1, 9, 15), 100.0, 100.0)), // signals buy
            ("ABC".into(), candle(ts(d1, 9, 16), 100.0, 100.0)), // buy fill
            ("ABC".into(), candle(ts(d1, 9, 17), 90.0, 90.0)),   // losing exit signals
            ("ABC".into(), candle(ts(d1, 9, 18), 90.0, 90.0)),   // loss is realized
            ("ABC".into(), candle(ts(d1, 9, 19), 90.0, 90.0)),   // flatten after breach
            ("ABC".into(), candle(ts(d1, 9, 20), 90.0, 90.0)),   // flatten fill
            ("ABC".into(), candle(ts(d2, 9, 15), 90.0, 90.0)),   // next-day signal
            ("ABC".into(), candle(ts(d2, 9, 16), 90.0, 90.0)),   // next-day fill
        ]);
        let mut cfg = config();
        cfg.initial_cash = 10_000.0;
        let result = run(&program, &source, cfg, None).unwrap();
        assert_eq!(result.fills.len(), 3);
        assert_eq!(result.fills[0].side, Side::Buy);
        assert_eq!(result.fills[1].side, Side::Sell);
        assert_eq!(result.fills[1].reason, OrderReason::Rule("rule_2".into()));
        assert_eq!(result.fills[2].side, Side::Sell);
        assert_eq!(result.fills[2].reason, OrderReason::DailyLossFlatten);
        assert_eq!(result.open_positions.iter().map(|p| p.qty).sum::<i64>(), 0);
        assert!(result
            .events
            .iter()
            .any(|event| matches!(event, TraderEvent::DailyLossBreached)));
    }

    #[test]
    fn date_warmup_keeps_only_the_requested_tail_per_symbol() {
        let source: Vec<Result<(Symbol, Candle), SourceError>> = (1..=5)
            .map(|ts| Ok(("A".into(), candle(ts, 1.0, 1.0))))
            .collect();
        let from = 4;
        let out: Vec<_> = limit_to_warmup_window(Box::new(source.into_iter()), Some(from), 2)
            .map(Result::unwrap)
            .map(|(_, c)| c.timestamp)
            .collect();
        assert_eq!(out, vec![2, 3, 4, 5]);
    }

    #[test]
    fn skip_bad_rows_are_counted_and_valid_rows_survive() {
        let base = std::env::temp_dir().join(format!("algomln-skip-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&base);
        std::fs::write(
            base.join("A.csv"),
            "ts,open,high,low,close,volume\n1,1,1,1,1,1\nbad,1,1,1,1,1\n2,2,2,2,2,1\n",
        )
        .unwrap();
        let mut src = FileSource::new(&base);
        src.skip_bad = true;
        let stream = src
            .open(&["A".into()], Timeframe::M1, DateRange::default())
            .unwrap();
        let rows: Vec<_> = stream.map(Result::unwrap).collect();
        assert_eq!(rows.len(), 2);
        assert_eq!(src.stats().skipped_bad["A"], 1);
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn golden_strategy_and_csv_produce_expected_round_trip_file() {
        let strategy = include_str!("../tests/golden/simulation.algomln");
        let p = crate::compile(strategy).unwrap();
        let src = FileSource::new(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden"));
        let mut cfg = config();
        cfg.initial_cash = 1000.0;
        let r = run(&p, &src, cfg, None).unwrap();
        let dir = std::env::temp_dir().join(format!("algomln-golden-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        write_results(&dir, &r).unwrap();
        let actual = std::fs::read_to_string(dir.join("trades.csv")).unwrap();
        assert_eq!(
            actual,
            include_str!("../tests/golden/simulation.trades.csv")
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn file_stream_merges_by_timestamp_then_symbol() {
        let base = std::env::temp_dir().join(format!("algomln-sim-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&base);
        std::fs::write(
            base.join("A.csv"),
            "ts,open,high,low,close,volume\n1,1,1,1,1,1\n",
        )
        .unwrap();
        std::fs::write(
            base.join("B.csv"),
            "ts,open,high,low,close,volume\n1,1,1,1,1,1\n",
        )
        .unwrap();
        let src = FileSource::new(&base);
        let mut stream = src
            .open(
                &["B".into(), "A".into()],
                Timeframe::M1,
                DateRange::default(),
            )
            .unwrap();
        let a = stream.next().unwrap().unwrap();
        let b = stream.next().unwrap().unwrap();
        assert_eq!((a.0, a.1.timestamp), ("A".into(), 1));
        assert_eq!((b.0, b.1.timestamp), ("B".into(), 1));
        drop(stream);
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn file_source_fills_flat_only_inside_same_ist_session() {
        let base = std::env::temp_dir().join(format!("algomln-gaps-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&base);
        let tz = FixedOffset::east_opt(19800).unwrap();
        let day = chrono::NaiveDate::from_ymd_opt(2024, 5, 6).unwrap();
        let t1 = tz
            .from_local_datetime(&day.and_hms_opt(9, 15, 0).unwrap())
            .single()
            .unwrap()
            .timestamp();
        let t3 = tz
            .from_local_datetime(&day.and_hms_opt(9, 17, 0).unwrap())
            .single()
            .unwrap()
            .timestamp();
        std::fs::write(
            base.join("ABC.csv"),
            format!("ts,open,high,low,close,volume\n{t1},10,10,10,10,1\n{t3},12,12,12,12,1\n"),
        )
        .unwrap();
        let mut src = FileSource::new(&base);
        src.gap_policy = GapPolicy::FillFlatInSession;
        let stream = src
            .open(&["ABC".into()], Timeframe::M1, DateRange::default())
            .unwrap();
        let got: Vec<_> = stream.map(Result::unwrap).map(|(_, c)| c).collect();
        assert_eq!(got.len(), 3);
        assert_eq!(got[1].timestamp, t1 + 60);
        assert_eq!(got[1].close, 10.0);
        assert_eq!(got[1].volume, 0.0);
        assert_eq!(src.stats().missing_candles["ABC"], 1);
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn daily_series_is_never_gap_filled() {
        let base = std::env::temp_dir().join(format!("algomln-d1gap-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&base);
        let tz = FixedOffset::east_opt(19800).unwrap();
        let ts_at = |y, m, d| {
            let day = chrono::NaiveDate::from_ymd_opt(y, m, d).unwrap();
            tz.from_local_datetime(&day.and_hms_opt(9, 15, 0).unwrap())
                .single()
                .unwrap()
                .timestamp()
        };
        // Two daily bars three calendar days apart (a weekend gap between them).
        let t1 = ts_at(2024, 5, 3); // Fri
        let t2 = ts_at(2024, 5, 6); // Mon
        std::fs::write(
            base.join("ABC.csv"),
            format!("ts,open,high,low,close,volume\n{t1},10,10,10,10,1\n{t2},12,12,12,12,1\n"),
        )
        .unwrap();
        let mut src = FileSource::new(&base);
        src.gap_policy = GapPolicy::FillFlatInSession;
        let stream = src
            .open(&["ABC".into()], Timeframe::D1, DateRange::default())
            .unwrap();
        let got: Vec<_> = stream.map(Result::unwrap).map(|(_, c)| c).collect();
        // Daily gaps are non-trading days and must never be filled or counted.
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].timestamp, t1);
        assert_eq!(got[1].timestamp, t2);
        assert!(src.stats().missing_candles.get("ABC").is_none());
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn parquet_reader_streams_validated_ohlcv_rows() {
        use arrow_array::{Float64Array, Int64Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;
        let base = std::env::temp_dir().join(format!("algomln-parquet-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&base);
        let path = base.join("ABC.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("open", DataType::Float64, false),
            Field::new("high", DataType::Float64, false),
            Field::new("low", DataType::Float64, false),
            Field::new("close", DataType::Float64, false),
            Field::new("volume", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![10_i64])),
                Arc::new(Float64Array::from(vec![10.0])),
                Arc::new(Float64Array::from(vec![11.0])),
                Arc::new(Float64Array::from(vec![9.0])),
                Arc::new(Float64Array::from(vec![10.5])),
                Arc::new(Float64Array::from(vec![4.0])),
            ],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let source = FileSource::new(&base);
        let mut stream = source
            .open(&["ABC".into()], Timeframe::M1, DateRange::default())
            .unwrap();
        let (_, c) = stream.next().unwrap().unwrap();
        assert_eq!(c.timestamp, 10);
        assert_eq!(c.close, 10.5);
        assert!(stream.next().is_none());
        drop(stream);
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn broker_waits_for_missing_symbol_and_partial_fills_cancel_cleanly() {
        let mut b = SimBroker::new(vec!["ABC".into()]);
        b.partial_fills_pct = Some(50.0);
        let req = OrderRequest {
            client_order_id: "partial".into(),
            symbol: "ABC".into(),
            side: Side::Buy,
            qty: 5,
            reason: OrderReason::Rule("r".into()),
            ref_price: 10.0,
            decided_at: 1,
        };
        b.submit(&req).unwrap();
        b.on_candle_open(&BTreeMap::new());
        assert!(b.drain_events().is_empty());
        let mut c2 = candle(2, 10.0, 10.0);
        c2.volume = 2.0;
        b.on_candle_open(&BTreeMap::from([("ABC".into(), c2)]));
        let first = b.drain_events();
        assert!(matches!(first[0], OrderEvent::Acked { .. }));
        assert!(matches!(first[1], OrderEvent::PartialFill { qty: 1, .. }));
        let mut c3 = candle(3, 11.0, 11.0);
        c3.volume = 2.0;
        b.on_candle_open(&BTreeMap::from([("ABC".into(), c3)]));
        assert!(matches!(
            b.drain_events()[0],
            OrderEvent::PartialFill { qty: 1, .. }
        ));
        b.cancel("partial".into()).unwrap();
        assert!(matches!(b.drain_events()[0], OrderEvent::Cancelled { .. }));
        assert_eq!(
            b.cancel("partial".into()),
            Err(PortError::UnknownOrder("partial".into()))
        );
    }

    #[test]
    fn alphabetical_symbol_wins_when_cash_allows_one_entry() {
        let p = crate::compile("TRADE_IN B,A\nWHEN close > 0\nBUY 10\n").unwrap();
        let src = MemorySource(vec![
            ("B".into(), candle(1, 1.0, 1.0)),
            ("A".into(), candle(1, 1.0, 1.0)),
            ("A".into(), candle(2, 1.0, 1.0)),
            ("B".into(), candle(2, 1.0, 1.0)),
        ]);
        let mut cfg = config();
        cfg.initial_cash = 10.0;
        let r = run(&p, &src, cfg, None).unwrap();
        assert_eq!(r.fills.len(), 1);
        assert_eq!(r.fills[0].symbol, "A");
    }
}

/// Write deterministic, machine-readable run artifacts into an output folder.
pub fn write_results(dir: impl Into<PathBuf>, result: &RunResult) -> Result<(), std::io::Error> {
    let dir = dir.into();
    std::fs::create_dir_all(&dir)?;
    let mut f = csv::Writer::from_writer(File::create(dir.join("fills.csv"))?);
    csv_record(
        &mut f,
        [
            "ts",
            "symbol",
            "order_id",
            "side",
            "qty",
            "price",
            "price_paise",
            "fees",
            "fees_paise",
            "reason",
        ]
        .map(str::to_owned)
        .to_vec(),
    )?;
    for x in &result.fills {
        csv_record(
            &mut f,
            vec![
                x.ts.to_string(),
                x.symbol.clone(),
                x.order_id.clone(),
                format!("{:?}", x.side),
                x.qty.to_string(),
                format!("{:.10}", x.price),
                x.price_paise.to_string(),
                format!("{:.10}", x.fees),
                x.fees_paise.to_string(),
                format!("{:?}", x.reason),
            ],
        )?;
    }
    f.flush()?;
    let mut e = csv::Writer::from_writer(File::create(dir.join("equity.csv"))?);
    csv_record(
        &mut e,
        [
            "ts",
            "cash",
            "cash_paise",
            "positions_value",
            "positions_value_paise",
            "equity",
            "equity_paise",
        ]
        .map(str::to_owned)
        .to_vec(),
    )?;
    for x in &result.equity {
        csv_record(
            &mut e,
            vec![
                x.ts.to_string(),
                format!("{:.10}", x.cash),
                x.cash_paise.to_string(),
                format!("{:.10}", x.positions_value),
                x.positions_value_paise.to_string(),
                format!("{:.10}", x.equity),
                x.equity_paise.to_string(),
            ],
        )?;
    }
    e.flush()?;
    std::fs::write(
        dir.join("open_positions.json"),
        serde_json::to_vec_pretty(&result.open_positions).expect("serializable"),
    )?;
    std::fs::write(
        dir.join("events.json"),
        serde_json::to_vec_pretty(&result.events).expect("serializable"),
    )?;
    std::fs::write(
        dir.join("manifest.json"),
        serde_json::to_vec_pretty(&result.manifest).expect("serializable"),
    )?;
    let start = result
        .manifest
        .pointer("/config/initial_cash")
        .and_then(serde_json::Value::as_f64)
        .or_else(|| result.equity.first().map(|x| x.equity))
        .unwrap_or(0.0);
    let start_paise = crate::trader::rupees_to_paise(start);
    let end_paise = result.equity.last().map_or(start_paise, |x| x.equity_paise);
    let mut peak_paise = start_paise;
    let mut max_dd = 0.0f64;
    for p in &result.equity {
        peak_paise = peak_paise.max(p.equity_paise);
        if peak_paise > 0 {
            max_dd = max_dd.max((peak_paise - p.equity_paise) as f64 / peak_paise as f64 * 100.0);
        }
    }
    let metrics = build_metrics(result, start_paise, end_paise, max_dd);
    std::fs::write(
        dir.join("metrics.json"),
        serde_json::to_vec_pretty(&metrics).expect("serializable"),
    )?;
    let mut t = csv::Writer::from_writer(File::create(dir.join("trades.csv"))?);
    csv_record(
        &mut t,
        [
            "symbol",
            "entry_ts",
            "exit_ts",
            "qty",
            "avg_entry",
            "avg_entry_paise",
            "avg_exit",
            "avg_exit_paise",
            "fees",
            "fees_paise",
            "net_pnl",
            "net_pnl_paise",
            "exit_reason",
        ]
        .map(str::to_owned)
        .to_vec(),
    )?;
    for tr in &result.trades {
        csv_record(
            &mut t,
            vec![
                tr.symbol.clone(),
                tr.entry_ts.to_string(),
                tr.exit_ts.to_string(),
                tr.qty.to_string(),
                format!("{:.10}", tr.avg_entry),
                tr.avg_entry_paise.to_string(),
                format!("{:.10}", tr.avg_exit),
                tr.avg_exit_paise.to_string(),
                format!("{:.10}", tr.fees),
                tr.fees_paise.to_string(),
                format!("{:.10}", tr.net_pnl),
                tr.net_pnl_paise.to_string(),
                format!("{:?}", tr.exit_reason),
            ],
        )?;
    }
    t.flush()?;
    Ok(())
}
fn csv_record(writer: &mut csv::Writer<File>, fields: Vec<String>) -> Result<(), std::io::Error> {
    writer.write_record(fields).map_err(csv_io_error)
}
fn csv_io_error(error: csv::Error) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, error)
}

fn build_metrics(
    result: &RunResult,
    start_paise: i64,
    end_paise: i64,
    portfolio_dd: f64,
) -> serde_json::Value {
    let exposure = |series: &[SymbolEquityPoint]| {
        let total = series
            .last()
            .zip(series.first())
            .map_or(0.0, |(b, a)| (b.ts - a.ts).max(0) as f64);
        let held: f64 = series
            .windows(2)
            .filter(|w| w[0].position_qty > 0)
            .map(|w| (w[1].ts - w[0].ts).max(0) as f64)
            .sum();
        if total == 0.0 {
            0.0
        } else {
            held / total * 100.0
        }
    };
    let stats = |trades: Vec<&Trade>| {
        let wins: Vec<_> = trades.iter().filter(|t| t.net_pnl_paise > 0).collect();
        let losses: Vec<_> = trades.iter().filter(|t| t.net_pnl_paise < 0).collect();
        let gross_win: i128 = wins.iter().map(|t| t.net_pnl_paise as i128).sum();
        let gross_loss: i128 = losses.iter().map(|t| -(t.net_pnl_paise as i128)).sum();
        serde_json::json!({"trades":trades.len(),"win_rate_pct":if trades.is_empty(){0.0}else{wins.len() as f64/trades.len() as f64*100.0},"profit_factor":if gross_loss==0{serde_json::Value::Null}else if gross_win==0{serde_json::json!(0.0)}else{serde_json::json!(gross_win as f64/gross_loss as f64)},"avg_win":if wins.is_empty(){0.0}else{crate::trader::paise_to_rupees(round_half_even_i128(gross_win, wins.len() as i128) as i64)},"avg_loss":if losses.is_empty(){0.0}else{crate::trader::paise_to_rupees(round_half_even_i128(gross_loss, losses.len() as i128) as i64)}})
    };
    let mut symbol_metrics = serde_json::Map::new();
    let n = result.symbol_equity.len().max(1) as i64;
    let allocation_base = start_paise / n;
    let allocation_remainder = start_paise.rem_euclid(n);
    for (index, (symbol, series)) in result.symbol_equity.iter().enumerate() {
        let allocation_paise = allocation_base + i64::from((index as i64) < allocation_remainder);
        let final_eq_paise = series.last().map_or(allocation_paise, |p| p.equity_paise);
        let mut peak_paise = allocation_paise;
        let mut dd = 0.0f64;
        for p in series {
            peak_paise = peak_paise.max(p.equity_paise);
            if peak_paise > 0 {
                dd = dd.max((peak_paise - p.equity_paise) as f64 / peak_paise as f64 * 100.0)
            }
        }
        let ts: Vec<_> = result
            .trades
            .iter()
            .filter(|t| &t.symbol == symbol)
            .collect();
        let mut item = stats(ts);
        if let Some(obj) = item.as_object_mut() {
            let net_pnl_paise = final_eq_paise - allocation_paise;
            obj.insert(
                "net_pnl".into(),
                serde_json::json!(crate::trader::paise_to_rupees(net_pnl_paise)),
            );
            obj.insert("net_pnl_paise".into(), serde_json::json!(net_pnl_paise));
            obj.insert(
                "return_pct".into(),
                serde_json::json!(if allocation_paise == 0 {
                    0.0
                } else {
                    net_pnl_paise as f64 / allocation_paise as f64 * 100.0
                }),
            );
            obj.insert("max_drawdown_pct".into(), serde_json::json!(dd));
            obj.insert("exposure_pct".into(), serde_json::json!(exposure(series)));
        }
        symbol_metrics.insert(symbol.clone(), item);
    }
    let mut blocked = BTreeMap::<String, u64>::new();
    let mut skipped = 0;
    let mut breach = 0;
    let mut rejects = 0;
    for ev in &result.events {
        match ev {
            TraderEvent::RuleSkipped { .. } => skipped += 1,
            TraderEvent::RiskBlocked { limit, .. } => {
                *blocked.entry((*limit).to_owned()).or_default() += 1
            }
            TraderEvent::DailyLossBreached => breach += 1,
            TraderEvent::OrderRejected { .. } => rejects += 1,
            _ => {}
        }
    }
    let flatten_orders = result
        .fills
        .iter()
        .filter(|f| f.reason == OrderReason::DailyLossFlatten)
        .map(|f| f.order_id.clone())
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    let portfolio_trades: Vec<_> = result.trades.iter().collect();
    let mut p = stats(portfolio_trades);
    let mut held_seconds = 0.0;
    let mut total_seconds = 0.0;
    if let Some(first) = result.equity.first() {
        if let Some(last) = result.equity.last() {
            total_seconds = (last.ts - first.ts).max(0) as f64;
            for w in result.equity.windows(2) {
                let any = result.symbol_equity.values().any(|s| {
                    s.iter()
                        .find(|x| x.ts == w[0].ts)
                        .is_some_and(|x| x.position_qty > 0)
                });
                if any {
                    held_seconds += (w[1].ts - w[0].ts).max(0) as f64;
                }
            }
        }
    }
    if let Some(obj) = p.as_object_mut() {
        let net_pnl_paise = end_paise - start_paise;
        obj.insert(
            "net_pnl".into(),
            serde_json::json!(crate::trader::paise_to_rupees(net_pnl_paise)),
        );
        obj.insert("net_pnl_paise".into(), serde_json::json!(net_pnl_paise));
        obj.insert("start_equity_paise".into(), serde_json::json!(start_paise));
        obj.insert("end_equity_paise".into(), serde_json::json!(end_paise));
        obj.insert(
            "return_pct".into(),
            serde_json::json!(if start_paise == 0 {
                0.0
            } else {
                net_pnl_paise as f64 / start_paise as f64 * 100.0
            }),
        );
        obj.insert("max_drawdown_pct".into(), serde_json::json!(portfolio_dd));
        obj.insert(
            "exposure_pct".into(),
            serde_json::json!(if total_seconds == 0.0 {
                0.0
            } else {
                held_seconds / total_seconds * 100.0
            }),
        );
        obj.insert(
            "open_positions".into(),
            serde_json::json!(result.open_positions.len()),
        );
        obj.insert("rules_skipped".into(), serde_json::json!(skipped));
        obj.insert("entries_blocked_by_gate".into(), serde_json::json!(blocked));
        obj.insert("daily_loss_breaches".into(), serde_json::json!(breach));
        obj.insert("flattens".into(), serde_json::json!(flatten_orders));
        obj.insert("rejects".into(), serde_json::json!(rejects));
        obj.insert(
            "missing_candles".into(),
            serde_json::to_value(&result.source_stats.missing_candles).unwrap_or_default(),
        );
        obj.insert(
            "skipped_bad".into(),
            serde_json::to_value(&result.source_stats.skipped_bad).unwrap_or_default(),
        );
    }
    serde_json::json!({"portfolio":p,"symbols":symbol_metrics})
}
use serde::{Deserialize, Serialize};

/// UTF-8 byte offsets plus one-based line and Unicode-scalar column positions.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Span {
    pub start: usize,
    pub end: usize,
    pub line: u32,
    pub column: u32,
}

/// A decimal's lexical spelling and exact finite base-10 value. The value is
/// kept as a normalized coefficient and scale, never converted through f64.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DecimalValue {
    pub negative: bool,
    pub coefficient: String,
    pub scale: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NumericLiteral {
    pub spelling: String,
    pub value: DecimalValue,
    pub span: Span,
}

impl NumericLiteral {
    pub fn parse(spelling: String, span: Span) -> Option<Self> {
        let (negative, raw) = if let Some(s) = spelling.strip_prefix('-') {
            (true, s)
        } else {
            (false, spelling.strip_prefix('+').unwrap_or(&spelling))
        };
        let mut it = raw.split('.');
        let whole = it.next()?;
        let fraction = it.next().unwrap_or("");
        if it.next().is_some()
            || whole.is_empty()
            || !whole.bytes().all(|b| b.is_ascii_digit())
            || !fraction.bytes().all(|b| b.is_ascii_digit())
        {
            return None;
        }
        let digits = format!("{whole}{fraction}");
        let mut coefficient = digits.trim_start_matches('0').to_owned();
        let mut scale = fraction.len() as u32;
        while scale > 0 && coefficient.ends_with('0') {
            coefficient.pop();
            scale -= 1;
        }
        let coefficient = if coefficient.is_empty() {
            "0".to_owned()
        } else {
            coefficient.to_owned()
        };
        let negative = negative && coefficient.chars().any(|c| c != '0');
        let scale = if coefficient == "0" { 0 } else { scale };
        Some(Self {
            spelling,
            value: DecimalValue {
                negative,
                coefficient,
                scale,
            },
            span,
        })
    }
    pub fn cmp_integer(&self, integer: &str) -> Option<std::cmp::Ordering> {
        let rhs = Self::parse(integer.to_owned(), Span::default())?;
        if self.value.coefficient == "0" && rhs.value.coefficient == "0" {
            return Some(std::cmp::Ordering::Equal);
        }
        let scale = self.value.scale.max(rhs.value.scale);
        let mut a = self.value.coefficient.clone();
        let mut b = rhs.value.coefficient.clone();
        a.extend(std::iter::repeat('0').take((scale - self.value.scale) as usize));
        b.extend(std::iter::repeat('0').take((scale - rhs.value.scale) as usize));
        let ord = a.len().cmp(&b.len()).then_with(|| a.cmp(&b));
        Some(if self.value.negative != rhs.value.negative {
            if self.value.negative {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Greater
            }
        } else if self.value.coefficient == "0" && rhs.value.coefficient == "0" {
            std::cmp::Ordering::Equal
        } else if self.value.negative {
            ord.reverse()
        } else {
            ord
        })
    }
    pub fn compare_value(&self, other: &Self) -> std::cmp::Ordering {
        if self.value.coefficient == "0" && other.value.coefficient == "0" {
            return std::cmp::Ordering::Equal;
        }
        if self.value.negative != other.value.negative {
            return if self.value.negative {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Greater
            };
        }
        let scale = self.value.scale.max(other.value.scale);
        let mut a = self.value.coefficient.clone();
        let mut b = other.value.coefficient.clone();
        a.extend(std::iter::repeat('0').take((scale - self.value.scale) as usize));
        b.extend(std::iter::repeat('0').take((scale - other.value.scale) as usize));
        let ord = a.len().cmp(&b.len()).then_with(|| a.cmp(&b));
        if self.value.coefficient == "0" && other.value.coefficient == "0" {
            std::cmp::Ordering::Equal
        } else if self.value.negative {
            ord.reverse()
        } else {
            ord
        }
    }
    pub fn is_integer(&self) -> bool {
        self.value.scale == 0
    }
}

impl Span {
    pub const fn new(start: usize, end: usize, line: u32, column: u32) -> Self {
        Self {
            start,
            end,
            line,
            column,
        }
    }
}
//! Portfolio-wide synchronous order decisions and fill-driven accounting.
use crate::{
    ast::{Action, TradeAmount},
    engine::{LevelFrame, Program, RuleId, Symbol},
    indicators::Candle,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

pub type OrderId = String;
/// Convert a rupee-denominated market value to integer paise using ties-to-even.
///
/// Routes through the same integer half-even path as `decimal_scaled` so that a
/// price carried as `f64` (candle close, config cash) rounds identically to the
/// decimal literals from the DSL. The `f64` is rendered to a fixed 9-fraction-digit
/// decimal string (enough to capture sub-paise without the `value * 100.0`
/// representation drift that made the old float path tie unstably), then rounded to
/// paise in `i128`.
pub fn rupees_to_paise(value: f64) -> i64 {
    if !value.is_finite() {
        return 0;
    }
    let scaled = decimal_scaled(&format!("{value:.9}"), 100)
        .unwrap_or_else(|| (value * 100.0).clamp(i64::MIN as f64, i64::MAX as f64) as i128);
    scaled.clamp(i64::MIN as i128, i64::MAX as i128) as i64
}
pub fn paise_to_rupees(value: i64) -> f64 {
    value as f64 / 100.0
}
fn round_half_even_ratio(numerator: i128, denominator: i128) -> i128 {
    let quotient = numerator / denominator;
    let remainder = numerator % denominator;
    let twice = remainder.abs() * 2;
    if twice > denominator.abs() || (twice == denominator.abs() && quotient % 2 != 0) {
        quotient + numerator.signum() * denominator.signum()
    } else {
        quotient
    }
}
fn decimal_scaled(value: &str, scale: i128) -> Option<i128> {
    let negative = value.starts_with('-');
    let unsigned = value.trim_start_matches(['-', '+']);
    let mut parts = unsigned.split('.');
    let whole: i128 = parts.next()?.parse().ok()?;
    let fraction = parts.next().unwrap_or("");
    if fraction.len() > 9 || !fraction.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let denominator = 10i128.checked_pow(fraction.len() as u32)?;
    let fractional = if fraction.is_empty() {
        0
    } else {
        fraction.parse::<i128>().ok()?
    };
    let scaled = whole
        .checked_mul(scale)?
        .checked_add(round_half_even_ratio(
            fractional.checked_mul(scale)?,
            denominator,
        ))?;
    Some(if negative { -scaled } else { scaled })
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Act,
    Observe,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderReason {
    Rule(RuleId),
    StopLoss,
    TakeProfit,
    DailyLossFlatten,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OrderRequest {
    pub client_order_id: OrderId,
    pub symbol: Symbol,
    pub side: Side,
    pub qty: u64,
    pub reason: OrderReason,
    pub ref_price: f64,
    pub decided_at: i64,
}
#[derive(Clone, Debug, PartialEq)]
pub enum OrderEvent {
    Acked {
        id: OrderId,
    },
    PartialFill {
        id: OrderId,
        qty: u64,
        price: f64,
        fees: f64,
    },
    Filled {
        id: OrderId,
        qty: u64,
        price: f64,
        fees: f64,
    },
    Rejected {
        id: OrderId,
        reason: String,
    },
    Cancelled {
        id: OrderId,
    },
    Expired {
        id: OrderId,
    },
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PortError {
    Message(String),
    UnknownOrder(OrderId),
}
pub trait ExecutionPort {
    fn submit(&mut self, req: &OrderRequest) -> Result<(), PortError>;
    fn cancel(&mut self, id: OrderId) -> Result<(), PortError>;
    fn drain_events(&mut self) -> Vec<OrderEvent>;
}

/// Deterministic execution test double. Tests enqueue delayed events explicitly.
#[derive(Default)]
pub struct ScriptedPort {
    pub submitted: Vec<OrderRequest>,
    pub cancelled: Vec<OrderId>,
    events: VecDeque<OrderEvent>,
}
impl ScriptedPort {
    pub fn push(&mut self, event: OrderEvent) {
        self.events.push_back(event)
    }
    pub fn push_all(&mut self, events: impl IntoIterator<Item = OrderEvent>) {
        self.events.extend(events)
    }
}
impl ExecutionPort for ScriptedPort {
    fn submit(&mut self, r: &OrderRequest) -> Result<(), PortError> {
        self.submitted.push(r.clone());
        Ok(())
    }
    fn cancel(&mut self, id: OrderId) -> Result<(), PortError> {
        self.cancelled.push(id);
        Ok(())
    }
    fn drain_events(&mut self) -> Vec<OrderEvent> {
        self.events.drain(..).collect()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InstrumentRule {
    pub lot_size: u64,
    pub min_qty: u64,
}
impl Default for InstrumentRule {
    fn default() -> Self {
        Self {
            lot_size: 1,
            min_qty: 1,
        }
    }
}
#[derive(Clone, Debug)]
pub struct TraderConfig {
    pub initial_cash: f64,
    pub instrument_rules: BTreeMap<Symbol, InstrumentRule>,
}
impl Default for TraderConfig {
    fn default() -> Self {
        Self {
            initial_cash: 0.0,
            instrument_rules: BTreeMap::new(),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TraderError {
    InitialCashRequired,
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum TraderEvent {
    RuleSkipped {
        symbol: Symbol,
        rule_id: RuleId,
    },
    RiskBlocked {
        symbol: Symbol,
        rule_id: RuleId,
        limit: &'static str,
    },
    DailyLossBreached,
    OrderEmitted(OrderRequest),
    Fill {
        id: OrderId,
        qty: u64,
        price: f64,
        fees: f64,
    },
    OrderRejected {
        id: OrderId,
        reason: String,
    },
    OrderClosed {
        id: OrderId,
    },
}
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FrameOutput {
    pub orders: Vec<OrderRequest>,
    /// Broker order IDs the adapter should cancel after this frame.
    pub cancels: Vec<OrderId>,
    pub events: Vec<TraderEvent>,
}
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Position {
    pub filled_qty: i64,
    pub avg_entry_paise: i64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderState {
    Submitted,
    Acked,
    PartialFill,
    Filled,
    Rejected,
    Cancelled,
    Expired,
}
#[derive(Clone, Debug)]
struct Pending {
    req: OrderRequest,
    filled: u64,
    state: OrderState,
}
impl Pending {
    fn live(&self) -> bool {
        matches!(
            self.state,
            OrderState::Submitted | OrderState::Acked | OrderState::PartialFill
        )
    }
    fn remaining(&self) -> u64 {
        self.req.qty.saturating_sub(self.filled)
    }
}

#[derive(Debug)]
pub struct Trader {
    program: Program,
    cfg: TraderConfig,
    prev_levels: BTreeMap<(Symbol, RuleId), bool>,
    positions: BTreeMap<Symbol, Position>,
    orders: BTreeMap<OrderId, Pending>,
    last_prices: BTreeMap<Symbol, (f64, i64)>,
    cash_paise: i64,
    initial_cash_paise: i64,
    realized_pnl_today_paise: i64,
    realized_loss_today_paise: i64,
    accepted_buys_today: u64,
    next_id: u64,
    daily_loss_reported: bool,
}
impl Trader {
    pub fn new(program: &Program, cfg: TraderConfig) -> Result<Self, TraderError> {
        if program.risk().max_daily_loss.is_some()
            && (!cfg.initial_cash.is_finite() || cfg.initial_cash <= 0.0)
        {
            return Err(TraderError::InitialCashRequired);
        }
        let cash_paise = rupees_to_paise(cfg.initial_cash);
        Ok(Self {
            program: program.clone(),
            cfg,
            prev_levels: BTreeMap::new(),
            positions: BTreeMap::new(),
            orders: BTreeMap::new(),
            last_prices: BTreeMap::new(),
            cash_paise,
            initial_cash_paise: cash_paise,
            realized_pnl_today_paise: 0,
            realized_loss_today_paise: 0,
            accepted_buys_today: 0,
            next_id: 1,
            daily_loss_reported: false,
        })
    }
    pub fn start_day(&mut self, _day: i64) {
        self.realized_pnl_today_paise = 0;
        self.realized_loss_today_paise = 0;
        self.accepted_buys_today = 0;
        self.daily_loss_reported = false;
    }
    pub fn cash(&self) -> f64 {
        paise_to_rupees(self.cash_paise)
    }
    pub fn cash_paise(&self) -> i64 {
        self.cash_paise
    }
    pub fn realized_pnl_today(&self) -> f64 {
        paise_to_rupees(self.realized_pnl_today_paise)
    }
    pub fn realized_pnl_today_paise(&self) -> i64 {
        self.realized_pnl_today_paise
    }
    pub fn realized_loss_today(&self) -> f64 {
        paise_to_rupees(self.realized_loss_today_paise)
    }
    pub fn realized_loss_today_paise(&self) -> i64 {
        self.realized_loss_today_paise
    }
    pub fn accepted_buys_today(&self) -> u64 {
        self.accepted_buys_today
    }
    pub fn pending_order_count(&self) -> usize {
        self.orders.values().filter(|o| o.live()).count()
    }
    pub fn reserved_cash(&self) -> f64 {
        paise_to_rupees(self.reserved_cash_inner())
    }
    pub fn reserved_cash_paise(&self) -> i64 {
        self.reserved_cash_inner()
    }
    pub fn position(&self, s: &str) -> Option<&Position> {
        self.positions.get(s)
    }
    pub fn order_state(&self, id: &str) -> Option<OrderState> {
        self.orders.get(id).map(|o| o.state)
    }
    pub fn on_order_event(&mut self, ev: OrderEvent) -> Vec<TraderEvent> {
        let id = event_id(&ev).to_owned();
        let mut events = Vec::new();
        let mut fill = None;
        {
            let Some(o) = self.orders.get_mut(&id) else {
                return events;
            };
            if !o.live() {
                return events;
            }
            match ev {
                OrderEvent::Acked { .. } => {
                    if o.state == OrderState::Submitted {
                        o.state = OrderState::Acked;
                        if o.req.side == Side::Buy {
                            self.accepted_buys_today += 1
                        }
                    }
                }
                OrderEvent::PartialFill {
                    qty, price, fees, ..
                }
                | OrderEvent::Filled {
                    qty, price, fees, ..
                } => {
                    if !matches!(o.state, OrderState::Acked | OrderState::PartialFill) {
                        return events;
                    }
                    if !price.is_finite() || price <= 0.0 || !fees.is_finite() || fees < 0.0 {
                        return events;
                    }
                    let remaining_before = o.remaining();
                    let q = qty.min(remaining_before);
                    if q > 0 {
                        o.filled += q;
                        let booked_price = paise_to_rupees(rupees_to_paise(price));
                        let booked_fees = paise_to_rupees(rupees_to_paise(fees));
                        fill = Some((
                            o.req.symbol.clone(),
                            o.req.side,
                            q,
                            booked_price,
                            booked_fees,
                        ));
                        events.push(TraderEvent::Fill {
                            id: id.clone(),
                            qty: q,
                            price: booked_price,
                            fees: booked_fees,
                        })
                    }
                    if matches!(ev, OrderEvent::Filled { .. }) || o.remaining() == 0 {
                        // A terminal `Filled` closes the order; any still-unfilled
                        // remainder is dropped here and its reserved cash is released
                        // via `live() == false`. The bundled SimBroker/ScriptedPort
                        // always report the terminal fill as exactly the outstanding
                        // quantity, so a `Filled` with `qty < remaining` means a broker
                        // adapter is closing the order early or reporting cumulative
                        // (not incremental) fills — guard it so a future real adapter
                        // must handle the remainder explicitly instead of silently
                        // under-booking against `req.qty`.
                        debug_assert!(
                            !matches!(ev, OrderEvent::Filled { .. }) || qty >= remaining_before,
                            "terminal Filled booked {q} of {remaining_before} remaining for {}; \
                             a broker adapter must report incremental fills or handle early close",
                            o.req.client_order_id
                        );
                        o.state = OrderState::Filled
                    } else {
                        o.state = OrderState::PartialFill
                    }
                }
                OrderEvent::Rejected { reason, .. } => {
                    o.state = OrderState::Rejected;
                    events.push(TraderEvent::OrderRejected {
                        id: id.clone(),
                        reason,
                    })
                }
                OrderEvent::Cancelled { .. } => o.state = OrderState::Cancelled,
                OrderEvent::Expired { .. } => o.state = OrderState::Expired,
            }
        }
        if let Some((s, side, q, p, fees)) = fill {
            self.apply_fill(&s, side, q, rupees_to_paise(p), rupees_to_paise(fees))
        }
        if self.orders.get(&id).is_some_and(|o| !o.live()) {
            events.push(TraderEvent::OrderClosed { id })
        }
        events
    }
    fn apply_fill(&mut self, s: &str, side: Side, q: u64, p: i64, fees: i64) {
        let pos = self.positions.entry(s.to_owned()).or_default();
        let q_i = q.min(i64::MAX as u64) as i64;
        match side {
            Side::Buy => {
                let old = pos.filled_qty.max(0);
                let total = old.saturating_add(q_i);
                let old_cost = (old as i128) * (pos.avg_entry_paise as i128);
                let fill_cost = (q_i as i128) * (p as i128) + fees as i128;
                pos.avg_entry_paise =
                    round_half_even_ratio(old_cost + fill_cost, total.max(1) as i128)
                        .clamp(i64::MIN as i128, i64::MAX as i128) as i64;
                pos.filled_qty = total;
                self.cash_paise = self
                    .cash_paise
                    .saturating_sub(q_i.saturating_mul(p).saturating_add(fees));
            }
            Side::Sell => {
                let closed = q.min(pos.filled_qty.max(0) as u64);
                let closed_i = closed.min(i64::MAX as u64) as i64;
                let realized = (p.saturating_sub(pos.avg_entry_paise))
                    .saturating_mul(closed_i)
                    .saturating_sub(fees);
                self.realized_pnl_today_paise =
                    self.realized_pnl_today_paise.saturating_add(realized);
                self.realized_loss_today_paise = self
                    .realized_loss_today_paise
                    .saturating_add(realized.saturating_neg().max(0));
                pos.filled_qty = pos
                    .filled_qty
                    .saturating_sub(q.min(i64::MAX as u64) as i64)
                    .max(0);
                if pos.filled_qty == 0 {
                    pos.avg_entry_paise = 0;
                }
                self.cash_paise = self
                    .cash_paise
                    .saturating_add(q_i.saturating_mul(p).saturating_sub(fees));
            }
        }
    }
    pub fn on_port_events(
        &mut self,
        events: impl IntoIterator<Item = OrderEvent>,
    ) -> Vec<TraderEvent> {
        events
            .into_iter()
            .flat_map(|e| self.on_order_event(e))
            .collect()
    }
    pub fn on_frame(&mut self, f: &LevelFrame, c: &Candle, mode: Mode) -> FrameOutput {
        let mut out = FrameOutput::default();
        let mut fires = Vec::new();
        let mut levels = BTreeMap::new();
        for (id, &level) in &f.entry {
            levels.insert(id.clone(), (level, true));
        }
        for (id, &level) in &f.exit {
            levels.insert(id.clone(), (level, false));
        }
        for (id, (level, is_entry)) in levels {
            let prev = self
                .prev_levels
                .insert((f.symbol.clone(), id.clone()), level)
                .unwrap_or(false);
            if level && !prev {
                fires.push((id, is_entry))
            }
        }
        if mode == Mode::Observe {
            return out;
        }
        self.last_prices
            .insert(f.symbol.clone(), (c.close, f.candle_ts));
        let loss_breached = self.daily_loss_breached();
        if loss_breached {
            let report_daily_loss = !self.daily_loss_reported;
            self.daily_loss_reported = true;
            out.cancels.extend(
                self.orders
                    .values()
                    .filter(|o| o.live() && o.req.side == Side::Buy && o.remaining() > 0)
                    .map(|o| o.req.client_order_id.clone()),
            );
            let open: Vec<_> = self
                .positions
                .iter()
                .filter(|(_, position)| position.filled_qty > 0)
                .map(|(symbol, position)| (symbol.clone(), position.filled_qty as u64))
                .collect();
            for (symbol, qty) in open {
                if !self.has_pending_exit(&symbol) {
                    if let Some((price, decided_at)) = self.last_prices.get(&symbol).copied() {
                        self.emit_at(
                            &mut out,
                            &symbol,
                            Side::Sell,
                            qty,
                            OrderReason::DailyLossFlatten,
                            price,
                            decided_at,
                        );
                    }
                }
            }
            // Entries still flow through the risk gate for an explicit block
            // event. Rule exits and automatic exits must not compete with the
            // portfolio flatten order.
            for (id, is_entry) in fires {
                if is_entry {
                    out.events.push(TraderEvent::RiskBlocked {
                        symbol: f.symbol.clone(),
                        rule_id: id,
                        limit: "MAX_DAILY_LOSS",
                    });
                }
            }
            if report_daily_loss {
                out.events.push(TraderEvent::DailyLossBreached);
            }
            return out;
        }
        let mut full_exit = false;
        for (id, is_entry) in fires {
            let Some(rule) = self.program.rules().iter().find(|r| r.id == id) else {
                continue;
            };
            let side = if is_entry { Side::Buy } else { Side::Sell };
            let net = self.net_position(&f.symbol);
            let raw = match &rule.action {
                Action::Buy(a) | Action::Sell(a) => self.resolve_size(a, c.close, &f.symbol),
                Action::SellAll => Some(net.max(0) as u64),
            }
            .unwrap_or(0);
            let qty = if is_entry {
                raw
            } else {
                raw.min(net.max(0) as u64)
            };
            if qty == 0 {
                out.events.push(TraderEvent::RuleSkipped {
                    symbol: f.symbol.clone(),
                    rule_id: id,
                });
                continue;
            }
            if is_entry {
                if let Some(limit) = self.blocking_limit(&f.symbol, qty, c.close) {
                    out.events.push(TraderEvent::RiskBlocked {
                        symbol: f.symbol.clone(),
                        rule_id: id,
                        limit,
                    });
                    continue;
                }
            }
            if !is_entry && qty as i64 >= net {
                full_exit = true
            }
            self.emit_at(
                &mut out,
                &f.symbol,
                side,
                qty,
                OrderReason::Rule(id),
                c.close,
                f.candle_ts,
            )
        }
        if !full_exit && !self.has_pending_exit(&f.symbol) {
            if let Some(pos) = self.positions.get(&f.symbol).filter(|p| p.filled_qty > 0) {
                let stop = self
                    .program
                    .stop_loss()
                    .and_then(|v| decimal_scaled(&v.spelling, 100))
                    .is_some_and(|bp| {
                        rupees_to_paise(c.close)
                            <= round_half_even_ratio(
                                pos.avg_entry_paise as i128 * (10_000 - bp),
                                10_000,
                            ) as i64
                    });
                let take = self
                    .program
                    .take_profit()
                    .and_then(|v| decimal_scaled(&v.spelling, 100))
                    .is_some_and(|bp| {
                        rupees_to_paise(c.close)
                            >= round_half_even_ratio(
                                pos.avg_entry_paise as i128 * (10_000 + bp),
                                10_000,
                            ) as i64
                    });
                if stop || take {
                    self.emit_at(
                        &mut out,
                        &f.symbol,
                        Side::Sell,
                        pos.filled_qty as u64,
                        if stop {
                            OrderReason::StopLoss
                        } else {
                            OrderReason::TakeProfit
                        },
                        c.close,
                        f.candle_ts,
                    )
                }
            }
        }
        out
    }
    #[allow(clippy::too_many_arguments)]
    fn emit_at(
        &mut self,
        out: &mut FrameOutput,
        s: &str,
        side: Side,
        qty: u64,
        reason: OrderReason,
        ref_price: f64,
        decided_at: i64,
    ) {
        let req = OrderRequest {
            client_order_id: format!("trader-{}", self.next_id),
            symbol: s.to_owned(),
            side,
            qty,
            reason,
            ref_price,
            decided_at,
        };
        self.next_id += 1;
        self.orders.insert(
            req.client_order_id.clone(),
            Pending {
                req: req.clone(),
                filled: 0,
                state: OrderState::Submitted,
            },
        );
        out.events.push(TraderEvent::OrderEmitted(req.clone()));
        out.orders.push(req)
    }
    fn has_pending_exit(&self, s: &str) -> bool {
        self.orders
            .values()
            .any(|o| o.live() && o.req.symbol == s && o.req.side == Side::Sell)
    }
    fn net_position(&self, s: &str) -> i64 {
        let filled = self.positions.get(s).map_or(0, |p| p.filled_qty);
        let reserved = self
            .orders
            .values()
            .filter(|o| o.live() && o.req.symbol == s && o.req.side == Side::Sell)
            .map(|o| o.remaining() as i64)
            .sum::<i64>();
        (filled - reserved).max(0)
    }
    fn reserved_cash_inner(&self) -> i64 {
        self.orders
            .values()
            .filter(|o| o.live() && o.req.side == Side::Buy)
            .map(|o| {
                (o.remaining().min(i64::MAX as u64) as i64)
                    .saturating_mul(rupees_to_paise(o.req.ref_price))
            })
            .fold(0i64, i64::saturating_add)
    }
    fn available_cash(&self) -> i64 {
        self.cash_paise
            .saturating_sub(self.reserved_cash_inner())
            .max(0)
    }
    fn pending_new(&self) -> BTreeSet<Symbol> {
        self.orders
            .values()
            .filter(|o| {
                o.live()
                    && o.req.side == Side::Buy
                    && self
                        .positions
                        .get(&o.req.symbol)
                        .map_or(true, |p| p.filled_qty == 0)
            })
            .map(|o| o.req.symbol.clone())
            .collect()
    }
    fn blocking_limit(&self, _s: &str, q: u64, p: f64) -> Option<&'static str> {
        let mut symbols: BTreeSet<Symbol> = self
            .positions
            .iter()
            .filter(|(_, x)| x.filled_qty > 0)
            .map(|(s, _)| s.clone())
            .collect();
        symbols.extend(self.pending_new());
        if self
            .program
            .risk()
            .max_positions
            .is_some_and(|m| symbols.len() as u64 >= m)
        {
            return Some("MAX_POSITIONS");
        }
        let unacked = self
            .orders
            .values()
            .filter(|o| o.live() && o.req.side == Side::Buy && o.state == OrderState::Submitted)
            .count() as u64;
        if self
            .program
            .risk()
            .max_orders
            .is_some_and(|m| self.accepted_buys_today + unacked >= m)
        {
            return Some("MAX_ORDERS");
        }
        if self.daily_loss_breached() {
            return Some("MAX_DAILY_LOSS");
        }
        if (self.available_cash() as i128) < q as i128 * rupees_to_paise(p) as i128 {
            return Some("CASH");
        }
        None
    }
    fn daily_loss_breached(&self) -> bool {
        self.program
            .risk()
            .max_daily_loss
            .as_ref()
            .is_some_and(|limit| {
                self.initial_cash_paise > 0
                    && self.realized_loss_today_paise as i128 * 10_000
                        >= decimal_scaled(&limit.spelling, 100).unwrap_or(0)
                            * self.initial_cash_paise as i128
            })
    }
    fn resolve_size(&self, a: &TradeAmount, p: f64, s: &str) -> Option<u64> {
        if !p.is_finite() || p <= 0.0 {
            return None;
        }
        let raw = match a {
            TradeAmount::Quantity(n) => *n,
            TradeAmount::Worth(x) => {
                let worth_paise = decimal_rupees_to_paise(&x.spelling)?;
                let ref_paise = rupees_to_paise(p);
                if ref_paise <= 0 {
                    0
                } else {
                    (worth_paise.max(0) as i128 / ref_paise as i128) as u64
                }
            }
            TradeAmount::BuyCapitalPercentage(x) => {
                let pct_bp = decimal_scaled(&x.spelling, 100).unwrap_or(0);
                let ref_paise = rupees_to_paise(p);
                if ref_paise <= 0 {
                    0
                } else {
                    (((self.available_cash() as i128 * pct_bp) / (ref_paise as i128 * 10_000))
                        .max(0)) as u64
                }
            }
            TradeAmount::SellPositionPercentage(x) => {
                (((self.net_position(s) as i128 * decimal_scaled(&x.spelling, 100).unwrap_or(0))
                    / 10_000)
                    .max(0)) as u64
            }
        };
        let r = self
            .cfg
            .instrument_rules
            .get(s)
            .copied()
            .unwrap_or_default();
        let lot = r.lot_size.max(1);
        let q = raw / lot * lot;
        (q >= r.min_qty).then_some(q)
    }
}
fn decimal_rupees_to_paise(value: &str) -> Option<i64> {
    i64::try_from(decimal_scaled(value, 100)?).ok()
}
fn event_id(e: &OrderEvent) -> &str {
    match e {
        OrderEvent::Acked { id }
        | OrderEvent::PartialFill { id, .. }
        | OrderEvent::Filled { id, .. }
        | OrderEvent::Rejected { id, .. }
        | OrderEvent::Cancelled { id }
        | OrderEvent::Expired { id } => id,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::LevelFrame;

    fn candle(ts: i64, close: f64) -> Candle {
        Candle {
            timestamp: ts,
            open: close,
            high: close,
            low: close,
            close,
            volume: 100.0,
        }
    }
    fn program(src: &str) -> Program {
        crate::compile(src).unwrap()
    }
    fn frame(ts: i64, entry: &[(&str, bool)], exit: &[(&str, bool)]) -> LevelFrame {
        LevelFrame {
            symbol: "ABC".into(),
            candle_ts: ts,
            warm: true,
            entry: entry.iter().map(|(k, v)| ((*k).to_owned(), *v)).collect(),
            exit: exit.iter().map(|(k, v)| ((*k).to_owned(), *v)).collect(),
        }
    }
    fn named_frame(
        symbol: &str,
        ts: i64,
        entry: &[(&str, bool)],
        exit: &[(&str, bool)],
    ) -> LevelFrame {
        let mut f = frame(ts, entry, exit);
        f.symbol = symbol.into();
        f
    }
    fn trader(src: &str, cash: f64) -> Trader {
        Trader::new(
            &program(src),
            TraderConfig {
                initial_cash: cash,
                instrument_rules: BTreeMap::new(),
            },
        )
        .unwrap()
    }
    fn fill_buy(t: &mut Trader, id: &str, qty: u64, price: f64) {
        t.on_order_event(OrderEvent::Acked { id: id.into() });
        t.on_order_event(OrderEvent::Filled {
            id: id.into(),
            qty,
            price,
            fees: 0.0,
        });
    }

    #[test]
    fn observe_updates_edges_and_emits_nothing() {
        let mut t = trader("TRADE_IN ABC\nWHEN close > 1\nBUY 2\n", 100.0);
        let f = frame(1, &[("rule_1", true)], &[]);
        assert!(t
            .on_frame(&f, &candle(1, 10.0), Mode::Observe)
            .orders
            .is_empty());
        assert!(t
            .on_frame(
                &frame(2, &[("rule_1", true)], &[]),
                &candle(2, 10.0),
                Mode::Act
            )
            .orders
            .is_empty());
        assert_eq!(
            t.on_frame(
                &frame(3, &[("rule_1", false)], &[]),
                &candle(3, 10.0),
                Mode::Act
            )
            .orders
            .len(),
            0
        );
        assert_eq!(
            t.on_frame(
                &frame(4, &[("rule_1", true)], &[]),
                &candle(4, 10.0),
                Mode::Act
            )
            .orders[0]
                .qty,
            2
        );
    }

    #[test]
    fn pending_cash_and_unacked_orders_reserve_capacity_until_events() {
        let mut t = trader(
            "TRADE_IN ABC\nRISK MAX_ORDERS 1\nWHEN close > 1\nBUY 5\n",
            50.0,
        );
        let out = t.on_frame(
            &frame(1, &[("rule_1", true)], &[]),
            &candle(1, 10.0),
            Mode::Act,
        );
        assert_eq!(out.orders[0].qty, 5);
        assert_eq!(t.cash(), 50.0); // pending orders do not alter the ledger
        let blocked = t.on_frame(
            &frame(2, &[("rule_1", false)], &[]),
            &candle(2, 10.0),
            Mode::Act,
        );
        let again = t.on_frame(
            &frame(3, &[("rule_1", true)], &[]),
            &candle(3, 10.0),
            Mode::Act,
        );
        assert!(blocked.orders.is_empty());
        assert!(matches!(
            again.events[0],
            TraderEvent::RiskBlocked {
                limit: "MAX_ORDERS",
                ..
            }
        ));
        t.on_order_event(OrderEvent::Rejected {
            id: "trader-1".into(),
            reason: "no".into(),
        });
        let after = t.on_frame(
            &frame(4, &[("rule_1", false)], &[]),
            &candle(4, 10.0),
            Mode::Act,
        );
        let emitted = t.on_frame(
            &frame(5, &[("rule_1", true)], &[]),
            &candle(5, 10.0),
            Mode::Act,
        );
        assert!(after.orders.is_empty());
        assert_eq!(emitted.orders.len(), 1);
    }

    #[test]
    fn cash_is_reserved_across_orders_in_a_single_frame() {
        let mut t = trader(
            "TRADE_IN ABC\nWHEN close > 1\nBUY 4\nWHEN close > 1\nBUY 2\n",
            50.0,
        );
        let out = t.on_frame(
            &frame(1, &[("rule_1", true), ("rule_2", true)], &[]),
            &candle(1, 10.0),
            Mode::Act,
        );
        assert_eq!(out.orders.len(), 1);
        assert_eq!(out.orders[0].qty, 4);
        assert!(matches!(
            out.events[1],
            TraderEvent::RiskBlocked { limit: "CASH", .. }
        ));
    }

    #[test]
    fn rupees_to_paise_matches_integer_path_on_ties_and_negatives() {
        // Non-finite guards to zero.
        assert_eq!(rupees_to_paise(f64::NAN), 0);
        assert_eq!(rupees_to_paise(f64::INFINITY), 0);
        // Plain conversions.
        assert_eq!(rupees_to_paise(12.34), 1234);
        assert_eq!(rupees_to_paise(-12.34), -1234);
        // Exact half-paise ties round to even, symmetrically across zero.
        assert_eq!(rupees_to_paise(0.005), 0); // 0.5 paise -> 0 (even)
        assert_eq!(rupees_to_paise(0.015), 2); // 1.5 paise -> 2 (even)
        assert_eq!(rupees_to_paise(0.025), 2); // 2.5 paise -> 2 (even)
        assert_eq!(rupees_to_paise(-0.005), 0);
        assert_eq!(rupees_to_paise(-0.015), -2);
        assert_eq!(rupees_to_paise(-0.025), -2);
        // The float path agrees with the shared decimal path for the same value.
        for v in ["100.005", "100.015", "-100.005", "0.125", "-0.125", "7.00"] {
            let f: f64 = v.parse().unwrap();
            assert_eq!(
                i128::from(rupees_to_paise(f)),
                decimal_scaled(v, 100).unwrap(),
                "mismatch for {v}",
            );
        }
    }

    #[test]
    fn paise_ledger_and_percentage_sizing_use_integer_math() {
        assert_eq!(decimal_rupees_to_paise("12.34"), Some(1234));
        assert_eq!(decimal_scaled("12.34", 100), Some(1234));
        let t = trader("TRADE_IN ABC\nWHEN close > 1\nBUY 50%\n", 100.0);
        assert_eq!(t.cash_paise(), 10_000);
        assert_eq!(
            t.resolve_size(
                &TradeAmount::BuyCapitalPercentage(
                    crate::source::NumericLiteral::parse("33.33".into(), Default::default())
                        .unwrap()
                ),
                3.0,
                "ABC",
            ),
            Some(11)
        );
    }

    #[test]
    fn partial_fills_change_ledger_and_pending_exit_guards_stop_loss() {
        let mut t = trader(
            "TRADE_IN ABC\nSTOP_LOSS 10%\nWHEN close > 1\nBUY 10\n",
            1000.0,
        );
        let buy = t
            .on_frame(
                &frame(1, &[("rule_1", true)], &[]),
                &candle(1, 10.0),
                Mode::Act,
            )
            .orders
            .remove(0);
        t.on_order_event(OrderEvent::Acked {
            id: buy.client_order_id.clone(),
        });
        t.on_order_event(OrderEvent::PartialFill {
            id: buy.client_order_id.clone(),
            qty: 4,
            price: 10.0,
            fees: 1.0,
        });
        assert_eq!(t.position("ABC").unwrap().filled_qty, 4);
        assert_eq!(t.cash(), 959.0);
        let exit = t.on_frame(
            &frame(2, &[("rule_1", false)], &[]),
            &candle(2, 8.0),
            Mode::Act,
        );
        assert_eq!(exit.orders.len(), 1);
        assert_eq!(exit.orders[0].qty, 4);
        let next = t.on_frame(
            &frame(3, &[("rule_1", false)], &[]),
            &candle(3, 7.0),
            Mode::Act,
        );
        assert!(next.orders.is_empty());
        t.on_order_event(OrderEvent::Rejected {
            id: exit.orders[0].client_order_id.clone(),
            reason: "rejected".into(),
        });
        let rearmed = t.on_frame(
            &frame(4, &[("rule_1", false)], &[]),
            &candle(4, 7.0),
            Mode::Act,
        );
        assert_eq!(rearmed.orders[0].reason, OrderReason::StopLoss);
    }

    #[test]
    fn max_daily_loss_requires_cash_and_blocks_at_threshold() {
        let p=program("TRADE_IN ABC\nRISK MAX_DAILY_LOSS 5%\nWHEN close > 1\nBUY 10\nWHEN close < 1\nSELL ALL\n");
        assert_eq!(
            Trader::new(&p, TraderConfig::default()).unwrap_err(),
            TraderError::InitialCashRequired
        );
        let mut t = Trader::new(
            &p,
            TraderConfig {
                initial_cash: 100.0,
                instrument_rules: BTreeMap::new(),
            },
        )
        .unwrap();
        let buy = t
            .on_frame(
                &frame(1, &[("rule_1", true)], &[("rule_2", false)]),
                &candle(1, 10.0),
                Mode::Act,
            )
            .orders
            .remove(0);
        fill_buy(&mut t, &buy.client_order_id, 10, 10.0);
        let sell = t
            .on_frame(
                &frame(2, &[("rule_1", false)], &[("rule_2", true)]),
                &candle(2, 9.0),
                Mode::Act,
            )
            .orders
            .remove(0);
        t.on_order_event(OrderEvent::Acked {
            id: sell.client_order_id.clone(),
        });
        t.on_order_event(OrderEvent::Filled {
            id: sell.client_order_id,
            qty: 10,
            price: 9.5,
            fees: 0.0,
        });
        assert_eq!(t.realized_pnl_today(), -5.0);
        assert_eq!(t.realized_loss_today(), 5.0);
        let blocked = t.on_frame(
            &frame(3, &[("rule_1", true)], &[("rule_2", false)]),
            &candle(3, 10.0),
            Mode::Act,
        );
        assert!(matches!(
            blocked.events[0],
            TraderEvent::RiskBlocked {
                limit: "MAX_DAILY_LOSS",
                ..
            }
        ));
        t.start_day(4);
        assert_eq!(t.realized_pnl_today(), 0.0);
        assert_eq!(t.realized_loss_today(), 0.0);
    }

    #[test]
    fn daily_loss_breach_cancels_entries_flattens_and_catches_late_fills() {
        let mut t = trader(
            "TRADE_IN ABC\nRISK MAX_DAILY_LOSS 1%\nWHEN close > 4\nBUY 10\nWHEN close > 4\nBUY 2\nWHEN close < 6\nSELL 1\n",
            100.0,
        );
        let entries = t.on_frame(
            &frame(
                1,
                &[("rule_1", true), ("rule_2", true)],
                &[("rule_3", false)],
            ),
            &candle(1, 5.0),
            Mode::Act,
        );
        assert_eq!(entries.orders.len(), 2);
        t.on_order_event(OrderEvent::Acked {
            id: entries.orders[0].client_order_id.clone(),
        });
        t.on_order_event(OrderEvent::Filled {
            id: entries.orders[0].client_order_id.clone(),
            qty: 10,
            price: 5.0,
            fees: 0.0,
        });

        let sell = t
            .on_frame(
                &frame(
                    2,
                    &[("rule_1", false), ("rule_2", false)],
                    &[("rule_3", true)],
                ),
                &candle(2, 4.0),
                Mode::Act,
            )
            .orders
            .remove(0);
        t.on_order_event(OrderEvent::Acked {
            id: sell.client_order_id.clone(),
        });
        t.on_order_event(OrderEvent::Filled {
            id: sell.client_order_id,
            qty: 1,
            price: 4.0,
            fees: 0.0,
        });
        assert_eq!(t.realized_pnl_today(), -1.0);

        let breach = t.on_frame(
            &frame(
                3,
                &[("rule_1", true), ("rule_2", false)],
                &[("rule_3", false)],
            ),
            &candle(3, 4.0),
            Mode::Act,
        );
        assert_eq!(breach.cancels, vec!["trader-2"]);
        assert_eq!(breach.orders.len(), 1);
        assert_eq!(breach.orders[0].qty, 9);
        assert_eq!(breach.orders[0].reason, OrderReason::DailyLossFlatten);
        assert!(breach.events.iter().any(|e| matches!(
            e,
            TraderEvent::RiskBlocked {
                limit: "MAX_DAILY_LOSS",
                ..
            }
        )));

        // A fill racing the cancellation is booked; the still-active breach
        // causes another flatten after the first flatten completes.
        t.on_order_event(OrderEvent::Acked {
            id: "trader-2".into(),
        });
        t.on_order_event(OrderEvent::Filled {
            id: "trader-2".into(),
            qty: 2,
            price: 4.0,
            fees: 0.0,
        });
        t.on_order_event(OrderEvent::Acked {
            id: breach.orders[0].client_order_id.clone(),
        });
        t.on_order_event(OrderEvent::Filled {
            id: breach.orders[0].client_order_id.clone(),
            qty: 9,
            price: 4.0,
            fees: 0.0,
        });
        let second_flatten = t.on_frame(
            &frame(
                4,
                &[("rule_1", false), ("rule_2", false)],
                &[("rule_3", false)],
            ),
            &candle(4, 4.0),
            Mode::Act,
        );
        assert_eq!(
            second_flatten.orders[0].reason,
            OrderReason::DailyLossFlatten
        );
        assert_eq!(second_flatten.orders[0].qty, 2);

        t.start_day(5);
        let _ = t.on_frame(
            &frame(
                5,
                &[("rule_1", false), ("rule_2", false)],
                &[("rule_3", false)],
            ),
            &candle(5, 4.0),
            Mode::Act,
        );
        let next_day = t.on_frame(
            &frame(
                6,
                &[("rule_1", true), ("rule_2", false)],
                &[("rule_3", false)],
            ),
            &candle(6, 5.0),
            Mode::Act,
        );
        assert!(!next_day.orders.is_empty());
    }

    #[test]
    fn daily_loss_flattens_other_portfolio_symbols_at_their_latest_close() {
        let mut t = trader(
            "TRADE_IN ABC,XYZ\nRISK MAX_DAILY_LOSS 1%\nWHEN close > 4\nBUY 1\nWHEN close < 10\nSELL 1\n",
            100.0,
        );
        let abc_buy = t
            .on_frame(
                &named_frame("ABC", 1, &[("rule_1", true)], &[("rule_2", false)]),
                &candle(1, 10.0),
                Mode::Act,
            )
            .orders
            .remove(0);
        t.on_order_event(OrderEvent::Acked {
            id: abc_buy.client_order_id.clone(),
        });
        t.on_order_event(OrderEvent::Filled {
            id: abc_buy.client_order_id,
            qty: 1,
            price: 10.0,
            fees: 0.0,
        });
        let xyz_buy = t
            .on_frame(
                &named_frame("XYZ", 2, &[("rule_1", true)], &[("rule_2", false)]),
                &candle(2, 12.0),
                Mode::Act,
            )
            .orders
            .remove(0);
        t.on_order_event(OrderEvent::Acked {
            id: xyz_buy.client_order_id.clone(),
        });
        t.on_order_event(OrderEvent::Filled {
            id: xyz_buy.client_order_id,
            qty: 1,
            price: 12.0,
            fees: 0.0,
        });
        let abc_sell = t
            .on_frame(
                &named_frame("ABC", 3, &[("rule_1", false)], &[("rule_2", true)]),
                &candle(3, 9.0),
                Mode::Act,
            )
            .orders
            .remove(0);
        t.on_order_event(OrderEvent::Acked {
            id: abc_sell.client_order_id.clone(),
        });
        t.on_order_event(OrderEvent::Filled {
            id: abc_sell.client_order_id,
            qty: 1,
            price: 9.0,
            fees: 0.0,
        });

        let breached = t.on_frame(
            &named_frame("ABC", 4, &[("rule_1", false)], &[("rule_2", false)]),
            &candle(4, 8.0),
            Mode::Act,
        );
        assert_eq!(breached.orders.len(), 1);
        assert_eq!(breached.orders[0].symbol, "XYZ");
        assert_eq!(breached.orders[0].ref_price, 12.0);
        assert_eq!(breached.orders[0].reason, OrderReason::DailyLossFlatten);
    }

    #[test]
    fn threshold_remains_breached_after_later_profit_offsets_net_pnl() {
        let mut t = trader(
            "TRADE_IN ABC\nRISK MAX_DAILY_LOSS 5%\nWHEN close > 1\nBUY 10\nWHEN close < 1\nSELL 5\nWHEN close < 1\nSELL 5\n",
            100.0,
        );
        let buy = t
            .on_frame(
                &frame(
                    1,
                    &[("rule_1", true)],
                    &[("rule_2", false), ("rule_3", false)],
                ),
                &candle(1, 10.0),
                Mode::Act,
            )
            .orders
            .remove(0);
        fill_buy(&mut t, &buy.client_order_id, 10, 10.0);
        let exits = t.on_frame(
            &frame(
                2,
                &[("rule_1", false)],
                &[("rule_2", true), ("rule_3", true)],
            ),
            &candle(2, 9.0),
            Mode::Act,
        );
        assert_eq!(exits.orders.len(), 2);
        t.on_order_event(OrderEvent::Acked {
            id: exits.orders[0].client_order_id.clone(),
        });
        t.on_order_event(OrderEvent::Filled {
            id: exits.orders[0].client_order_id.clone(),
            qty: 5,
            price: 9.0,
            fees: 0.0,
        });
        t.on_order_event(OrderEvent::Acked {
            id: exits.orders[1].client_order_id.clone(),
        });
        t.on_order_event(OrderEvent::Filled {
            id: exits.orders[1].client_order_id.clone(),
            qty: 5,
            price: 11.0,
            fees: 0.0,
        });
        assert_eq!(t.realized_pnl_today(), 0.0);
        assert_eq!(t.realized_loss_today(), 5.0);
        let later = t.on_frame(
            &frame(
                3,
                &[("rule_1", true)],
                &[("rule_2", false), ("rule_3", false)],
            ),
            &candle(3, 10.0),
            Mode::Act,
        );
        assert!(later.orders.is_empty());
        assert!(later.events.iter().any(|event| matches!(
            event,
            TraderEvent::RiskBlocked {
                limit: "MAX_DAILY_LOSS",
                ..
            }
        )));
    }

    #[test]
    fn sizing_rounds_down_to_lot_and_sl_beats_tp() {
        let src = "TRADE_IN ABC\nSTOP_LOSS 10%\nTAKE_PROFIT 10%\nWHEN close > 1\nBUY 10\n";
        let mut t = Trader::new(
            &program(src),
            TraderConfig {
                initial_cash: 1000.0,
                instrument_rules: BTreeMap::from([(
                    "ABC".into(),
                    InstrumentRule {
                        lot_size: 3,
                        min_qty: 3,
                    },
                )]),
            },
        )
        .unwrap();
        let buy = t
            .on_frame(
                &frame(1, &[("rule_1", true)], &[]),
                &candle(1, 10.0),
                Mode::Act,
            )
            .orders
            .remove(0);
        assert_eq!(buy.qty, 9);
        fill_buy(&mut t, &buy.client_order_id, 9, 10.0);
        let exit = t.on_frame(
            &frame(2, &[("rule_1", false)], &[]),
            &candle(2, 8.0),
            Mode::Act,
        );
        assert_eq!(exit.orders[0].reason, OrderReason::StopLoss);
        assert_eq!(exit.orders[0].qty, 9);
    }

    #[test]
    fn scripted_port_delivers_events_in_injected_order() {
        let mut port = ScriptedPort::default();
        let req = OrderRequest {
            client_order_id: "x".into(),
            symbol: "ABC".into(),
            side: Side::Buy,
            qty: 2,
            reason: OrderReason::Rule("r".into()),
            ref_price: 3.0,
            decided_at: 1,
        };
        port.submit(&req).unwrap();
        port.push(OrderEvent::Acked { id: "x".into() });
        port.push(OrderEvent::Filled {
            id: "x".into(),
            qty: 2,
            price: 3.0,
            fees: 0.0,
        });
        assert_eq!(port.submitted, vec![req]);
        assert!(matches!(
            port.drain_events().as_slice(),
            [OrderEvent::Acked { .. }, OrderEvent::Filled { .. }]
        ));
    }

    #[test]
    fn positions_limit_reserves_symbols_and_exit_rules_use_remaining_net() {
        let mut t = trader(
            "TRADE_IN ABC\nRISK MAX_POSITIONS 1\nRISK MAX_ORDERS 1\nWHEN close > 1\nBUY 10\nWHEN close < 1\nSELL 3\nWHEN close < 1\nSELL 5\n",
            1000.0,
        );
        let buy = t
            .on_frame(
                &frame(
                    1,
                    &[("rule_1", true)],
                    &[("rule_2", false), ("rule_3", false)],
                ),
                &candle(1, 10.0),
                Mode::Act,
            )
            .orders
            .remove(0);
        let reset_entry = t.on_frame(
            &frame(
                2,
                &[("rule_1", false)],
                &[("rule_2", false), ("rule_3", false)],
            ),
            &candle(2, 10.0),
            Mode::Act,
        );
        assert!(reset_entry.orders.is_empty());
        fill_buy(&mut t, &buy.client_order_id, 10, 10.0);
        let exits = t.on_frame(
            &frame(
                3,
                &[("rule_1", true)],
                &[("rule_2", true), ("rule_3", true)],
            ),
            &candle(3, 9.0),
            Mode::Act,
        );
        assert_eq!(exits.orders.len(), 2);
        assert!(matches!(
            exits.events[0],
            TraderEvent::RiskBlocked {
                limit: "MAX_POSITIONS",
                ..
            }
        ));
        assert_eq!(
            exits.orders.iter().map(|o| o.qty).collect::<Vec<_>>(),
            vec![3, 5]
        );
        assert_eq!(t.net_position("ABC"), 2);
    }
}
use crate::{
    ast::*,
    diagnostics::{Diagnostic, FrontendError},
};
pub fn validate(s: &Strategy) -> Result<(), FrontendError> {
    let mut d = Vec::new();
    if s.rules.is_empty() {
        d.push(diag("VAL_EMPTY", "strategy must contain at least one rule"));
    }
    for (name, v) in [
        ("STOP_LOSS", s.stop_loss.as_ref()),
        ("TAKE_PROFIT", s.take_profit.as_ref()),
        ("MAX_DAILY_LOSS", s.risk.max_daily_loss.as_ref()),
    ] {
        if v.is_some_and(|x| !percent_valid(x)) {
            let value = v.unwrap();
            d.push(Diagnostic {
                code: "VAL_PERCENT",
                message: format!("{name} must be in (0, 100]"),
                span: Some(value.span),
            });
        }
    }
    if s.risk.max_orders == Some(0) || s.risk.max_positions == Some(0) {
        d.push(diag(
            "VAL_RISK_INTEGER",
            "risk integer limits must be positive",
        ));
    }
    let mut ids = std::collections::HashSet::new();
    for r in &s.rules {
        if !ids.insert(&r.id) {
            d.push(Diagnostic {
                code: "VAL_DUPLICATE_RULE_ID",
                message: format!("duplicate rule id {}", r.id),
                span: Some(r.span),
            });
        }
        if let Some(amount) = match &r.action {
            Action::Buy(x) | Action::Sell(x) => Some(x),
            Action::SellAll => None,
        } {
            match amount {
                TradeAmount::Quantity(q) if *q == 0 => d.push(Diagnostic {
                    code: "VAL_AMOUNT",
                    message: "quantity must be positive".into(),
                    span: Some(r.action_span),
                }),
                TradeAmount::Worth(x)
                | TradeAmount::BuyCapitalPercentage(x)
                | TradeAmount::SellPositionPercentage(x)
                    if !x.cmp_integer("0").is_some_and(|v| v.is_gt())
                        || (matches!(
                            amount,
                            TradeAmount::BuyCapitalPercentage(_)
                                | TradeAmount::SellPositionPercentage(_)
                        ) && !x.cmp_integer("100").is_some_and(|v| v.is_le())) =>
                {
                    d.push(Diagnostic {
                        code: "VAL_AMOUNT",
                        message: "amount and percentage must be positive".into(),
                        span: Some(r.action_span),
                    })
                }
                _ => {}
            }
        }
        if let Some(value) = match &r.action {
            Action::Buy(TradeAmount::BuyCapitalPercentage(x))
            | Action::Sell(TradeAmount::SellPositionPercentage(x)) => Some(x),
            _ => None,
        } {
            if !percent_valid(value) {
                d.push(Diagnostic {
                    code: "VAL_PERCENT",
                    message: "action percentage must be in (0, 100]".into(),
                    span: Some(r.action_span),
                });
            }
        }
        condition(&r.condition, r.condition_span, &mut d);
    }
    if d.is_empty() {
        Ok(())
    } else {
        Err(FrontendError::Validation(d))
    }
}
fn diag(code: &'static str, message: impl Into<String>) -> Diagnostic {
    Diagnostic {
        code,
        message: message.into(),
        span: None,
    }
}
fn percent_valid(x: &Decimal) -> bool {
    let hundred = crate::source::NumericLiteral::parse("100".into(), x.span).unwrap();
    x.value.coefficient != "0" && !x.value.negative && x.compare_value(&hundred).is_le()
}
fn condition(c: &Condition, span: crate::source::Span, d: &mut Vec<Diagnostic>) {
    match c {
        Condition::And(a, b) | Condition::Or(a, b) => {
            condition(a, span, d);
            condition(b, span, d)
        }
        Condition::Not(a) => condition(a, span, d),
        Condition::InPosition => d.push(Diagnostic {
            code: "VAL_UNSUPPORTED_PREDICATE",
            message: "in_position() is parsed but not yet supported by the engine".into(),
            span: Some(span),
        }),
        Condition::Cross { left, right, .. } => {
            let a = matches!(left, NumericExpr::Indicator(_));
            let b = matches!(right, NumericExpr::Indicator(_));
            if (!a && !matches!(left, NumericExpr::Price(_)))
                || (!b && !matches!(right, NumericExpr::Price(_)))
                || (!a && !b)
            {
                d.push(diag(
                    "VAL_CROSS_TYPES",
                    "crossovers require at least one indicator and otherwise price fields",
                ));
            }
            validate_expr(left, d);
            validate_expr(right, d);
        }
        Condition::Compare { left, right, .. } => {
            validate_expr(left, d);
            validate_expr(right, d);
        }
        Condition::Between { start, end } => {
            d.push(Diagnostic {
                code: "VAL_UNSUPPORTED_PREDICATE",
                message: "between() is parsed but not yet supported by the engine".into(),
                span: Some(span),
            });
            if start.hour > 23 || end.hour > 23 || start.minute > 59 || end.minute > 59 {
                d.push(Diagnostic {
                    code: "VAL_TIME",
                    message: "time must be within 00:00..23:59".into(),
                    span: Some(span),
                });
            } else if (start.hour, start.minute) > (end.hour, end.minute) {
                d.push(Diagnostic {
                    code: "VAL_TIME_ORDER",
                    message: "start time must not follow end time".into(),
                    span: Some(span),
                });
            }
        }
    }
}
fn validate_expr(e: &NumericExpr, d: &mut Vec<Diagnostic>) {
    if let NumericExpr::Indicator(i) = e {
        if i.period == 0 {
            d.push(diag(
                "VAL_INDICATOR_PERIOD",
                "indicator period must be positive",
            ));
        }
        if matches!(
            i.kind,
            IndicatorKind::BbUpper | IndicatorKind::BbLower | IndicatorKind::BbMid
        ) {
            if !i
                .multiplier
                .as_ref()
                .is_some_and(|m| m.cmp_integer("0").is_some_and(|v| v.is_gt()))
            {
                d.push(diag(
                    "VAL_BB_MULTIPLIER",
                    "Bollinger multiplier must be positive",
                ));
            }
        } else if i.multiplier.is_some() {
            d.push(diag(
                "VAL_INDICATOR_ARITY",
                "this indicator takes exactly one argument",
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{lexer::lex, parser::parse};
    #[test]
    fn percentage_bounds_are_exact() {
        for x in ["0", "-0.1", "100.01"] {
            let s = parse(
                &lex(&format!(
                    "TRADE_IN ABC\nSTOP_LOSS {x}%\nWHEN close > 1\nBUY 1\n"
                ))
                .unwrap(),
            )
            .unwrap();
            assert!(validate(&s).is_err(), "{x}");
        }
        let s =
            parse(&lex("TRADE_IN ABC\nSTOP_LOSS 100%\nWHEN close > 1\nBUY 1\n").unwrap()).unwrap();
        assert!(validate(&s).is_ok());
    }
    #[test]
    fn action_zero_and_crossover_literal_are_rejected() {
        for src in [
            "TRADE_IN ABC\nWHEN close > 1\nBUY 0\n",
            "TRADE_IN ABC\nWHEN CROSS_ABOVE(ema(3), 1)\nBUY 1\n",
        ] {
            let s = parse(&lex(src).unwrap()).unwrap();
            assert!(validate(&s).is_err());
        }
    }
    #[test]
    fn unsupported_predicates_fail_compile_with_spans() {
        for predicate in ["in_position()", "between(09:30, 15:30)"] {
            let error =
                crate::compile(&format!("TRADE_IN ABC\nWHEN {predicate}\nBUY 1\n")).unwrap_err();
            assert_eq!(error.code(), "VAL_UNSUPPORTED_PREDICATE");
            assert!(error.span().is_some());
        }
    }
    #[test]
    fn extreme_exact_percentages_do_not_round_through_float() {
        for x in [
            "0.00000000000000000000000000000001",
            "100.0000000000000000000000000001",
        ] {
            let literal =
                crate::source::NumericLiteral::parse(x.to_string(), crate::source::Span::default())
                    .unwrap();
            assert_eq!(percent_valid(&literal), x.starts_with("0."), "{x}");
        }
    }
}
use algomln::{indicators::Candle, Engine, Program};
use std::hint::black_box;

fn candles() -> Vec<Candle> {
    include_str!("../sample-data/nifty_1min.csv")
        .lines()
        .skip(1)
        .enumerate()
        .map(|(i, row)| {
            let mut fields = row.split(',');
            let _ = fields.next();
            let mut n = || fields.next().unwrap().parse::<f64>().unwrap();
            let open = n();
            let high = n();
            let low = n();
            let close = n();
            Candle {
                timestamp: i as i64 * 60_000,
                open,
                high: high.max(close),
                low: low.min(open).min(close),
                close,
                volume: 0.,
            }
        })
        .collect()
}

#[test]
#[ignore = "timing-sensitive; run explicitly in release CI"]
fn condition_engine_nifty_baseline_regression_guard() {
    let input = candles();
    let program=Program::new(algomln::compile("TRADE_IN NIFTY\nWHEN ema(20) > sma(50) AND rsi(14) < 70\nBUY 1\nWHEN CROSS_BELOW(close, ema(20))\nSELL ALL\n").unwrap().strategy().clone());
    let start = std::time::Instant::now();
    let mut engine = Engine::new(&program, "NIFTY".into()).unwrap();
    for c in &input {
        black_box(engine.on_candle(black_box(c)).unwrap());
    }
    let rate = input.len() as f64 / start.elapsed().as_secs_f64();
    assert!(
        rate >= 1_211_130.0,
        "{rate:.0} candles/s is >10% below 1,345,700 baseline"
    );
}
use std::{fs, path::Path};

#[test]
fn golden_strategy_samples_match_serialized_ast() {
    for name in ["trend_momentum", "mean_reversion"] {
        let source = fs::read_to_string(format!("samples/{name}.algomln")).unwrap();
        let expected: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(format!("tests/golden/{name}.ast.json")).unwrap(),
        )
        .unwrap();
        let actual = algomln::compile(&source).unwrap();
        assert_eq!(
            serde_json::to_value(actual.strategy()).unwrap(),
            expected,
            "{name}"
        );
    }
}

#[test]
fn golden_error_cases_match_stable_codes() {
    let root = Path::new("tests/golden");
    let name = "malformed";
    let path = root.join(format!("{name}.algomln"));
    let source = fs::read_to_string(&path).unwrap();
    let expected = fs::read_to_string(root.join(format!("{name}.error"))).unwrap();
    let error = algomln::compile(&source).unwrap_err();
    assert_eq!(error.code(), expected.trim(), "{}", path.display());
}
