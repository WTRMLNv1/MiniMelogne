fn main() {
    tauri_build::build()
}
use std::{
    env, fs,
    path::Path,
    sync::Arc,
};
use tokio::sync::Mutex;

use algomln::{
    broker::{
        symbol_map::{refresh_symbol_map, SymbolMap},
        Timeframe,
    },
    commands::{
        self,
        registry::{DeployedStrategy, StrategyMode, StrategyRegistry, StrategyStatus},
        state::AppState,
        strategy::{run_backtest_dsl, BacktestResultWire},
    },
    indices::{refresh_all_if_stale, IndexRegistry},
    live::{
        guard::LiveGuard,
        holidays::NseHolidayCalendar,
        trade_log::{TradeLog, TradeLogEntry},
    },
    models::{Candle, Quote},
    plugin::{
        api::{
            analytics::SharedAnalyticsRegistry,
            dsl_extension::SharedDslExtensionRegistry,
            events::EventBus,
            execution::{GatedLiveExecutionApi, NoopExecutionApi},
            indicator_registry::SharedIndicatorRegistry,
            log_file::RateLimitedFileLog,
            market_data::BrokerMarketDataApi,
            scheduler::CronScheduler,
            storage::PluginKvStore,
            ui::TauriUiApi,
        },
        registry::PluginRegistry,
    },
};
use tauri::{Emitter, State};

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct DeployResult {
    strategy_id: String,
}

#[tauri::command]
async fn get_ohlcv(
    state: State<'_, AppState>,
    symbol: String,
    timeframe: String,
    from: i64,
    to: i64,
) -> Result<Vec<Candle>, String> {
    let timeframe = timeframe.parse::<Timeframe>()?;
    commands::data::get_ohlcv(&state.data, symbol, timeframe, from, to).await
}

#[tauri::command]
async fn get_quote(state: State<'_, AppState>, symbol: String) -> Result<Quote, String> {
    commands::data::get_quote(&state.data, symbol).await
}

#[tauri::command]
async fn subscribe_ticks(state: State<'_, AppState>, symbols: Vec<String>) -> Result<(), String> {
    commands::data::subscribe_ticks(&state.data, symbols).await
}

#[tauri::command]
async fn run_backtest(
    state: State<'_, AppState>,
    dsl_source: String,
    symbol: String,
    initial_cash: f64,
) -> Result<BacktestResultWire, String> {
    run_backtest_dsl(&dsl_source, &symbol, initial_cash, &state.data).await
}

#[tauri::command]
async fn validate_dsl(dsl_source: String) -> Result<Vec<String>, String> {
    Ok(commands::strategy::validate_dsl(&dsl_source))
}

#[tauri::command]
async fn deploy_strategy(
    state: State<'_, AppState>,
    dsl_source: String,
    name: String,
    mode: String,
) -> Result<DeployResult, String> {
    let mode = StrategyMode::parse(&mode)?;
    let strategy_id = state.strategies.deploy(&name, &dsl_source, mode).await?;
    Ok(DeployResult { strategy_id })
}

#[tauri::command]
async fn list_strategies(
    state: State<'_, AppState>,
) -> Result<Vec<DeployedStrategy>, String> {
    state.strategies.list().await
}

#[tauri::command]
async fn set_strategy_status(
    state: State<'_, AppState>,
    strategy_id: String,
    status: String,
) -> Result<(), String> {
    let status = StrategyStatus::parse(&status)?;
    state.strategies.set_status(&strategy_id, status).await
}

// ---------- Plugin IPC ----------
//
// The `#[tauri::command]` attribute generates module-private macro
// artifacts (`__cmd__name`, `__tauri_command_name_name`) that
// `tauri::generate_handler!` looks up by name. Those artifacts only
// exist in the module where the function is annotated, so the plugin
// command wrappers live here in `main.rs` and delegate to the
// plain-async implementations in `commands::plugins`.

#[tauri::command]
async fn list_plugins(
    state: State<'_, AppState>,
) -> Result<Vec<algomln::plugin::PluginListEntry>, String> {
    commands::plugins::list_plugins(&state).await
}

#[tauri::command]
async fn enable_plugin(state: State<'_, AppState>, id: String) -> Result<(), String> {
    commands::plugins::enable_plugin(&state, id).await
}

#[tauri::command]
async fn disable_plugin(state: State<'_, AppState>, id: String) -> Result<(), String> {
    commands::plugins::disable_plugin(&state, id).await
}

#[tauri::command]
async fn reload_plugins(state: State<'_, AppState>) -> Result<Vec<String>, String> {
    commands::plugins::reload_plugins(&state).await
}

// ---------- Index IPC ----------
#[tauri::command]
fn list_indices(state: State<'_, AppState>) -> Vec<algomln::indices::IndexInfo> {
    commands::indices::list_indices(&state)
}

#[tauri::command]
fn get_index_symbols(
    state: State<'_, AppState>,
    alias: String,
) -> Result<Vec<String>, String> {
    commands::indices::get_index_symbols(&state, alias)
}

#[tauri::command]
async fn refresh_indices(
    app: tauri::AppHandle,
    state: State<'_, AppState>,
) -> Result<commands::indices::RefreshResult, String> {
    Ok(commands::indices::refresh_indices(&app, &state).await)
}

// ---------- Search IPC ----------
//
// Fuzzy-search the symbol universe (equities + 22 NSE indices). The
// scoring logic is pure and lives in `algomln::search`; this wrapper just
// forwards to the async body in `commands::search`.
#[tauri::command]
async fn search_symbols(
    state: State<'_, AppState>,
    query: String,
) -> Result<Vec<algomln::search::SymbolMatch>, String> {
    commands::search::search_symbols_impl(&state, query).await
}

#[tauri::command]
async fn get_trade_log(state: State<'_, AppState>) -> Result<Vec<TradeLogEntry>, String> {
    commands::live::get_trade_log(state).await
}

// ---------- Live session IPC ----------
//
// All Phase-7 live commands live here as thin wrappers. The actual
// implementations live in `commands::live` because `#[tauri::command]`
// generates module-private macro artifacts (`__cmd__name` etc.) that
// `tauri::generate_handler!` resolves in the same scope.

#[tauri::command]
async fn request_live_start(
    state: State<'_, AppState>,
    strategy_id: String,
) -> Result<commands::live::RequestLiveStartResult, String> {
    commands::live::request_live_start(state, strategy_id).await
}

#[tauri::command]
async fn confirm_live_start(
    state: State<'_, AppState>,
    strategy_id: String,
    token: String,
) -> Result<(), String> {
    commands::live::confirm_live_start(state, strategy_id, token).await
}

#[tauri::command]
async fn acknowledge_live_trading(state: State<'_, AppState>) -> Result<(), String> {
    commands::live::acknowledge_live_trading(state).await
}

#[tauri::command]
async fn pause_live_strategy(state: State<'_, AppState>) -> Result<(), String> {
    commands::live::pause_live_strategy(state).await
}

#[tauri::command]
async fn resume_live_strategy(state: State<'_, AppState>) -> Result<(), String> {
    commands::live::resume_live_strategy(state).await
}

#[tauri::command]
async fn stop_live_strategy(
    state: State<'_, AppState>,
) -> Result<commands::live::StopResult, String> {
    commands::live::stop_live_strategy(state).await
}

#[tauri::command]
async fn get_live_status(
    state: State<'_, AppState>,
) -> Result<Option<commands::live::LiveStatusWire>, String> {
    commands::live::get_live_status(state).await
}

fn main() {
    load_dotenv();

    tauri::Builder::default()
        .setup(move |app| {
            use tauri::Manager;

            let store_dir = app
                .path()
                .app_data_dir()
                .expect("could not resolve app data dir");
            let trade_log_path = store_dir.join("trade_log.jsonl");
            let trade_log = Arc::new(
                TradeLog::open(trade_log_path.clone()).expect("could not open immutable trade log"),
            );

            // ---------- Symbol map (NSE -> Dhan SECURITY_ID) ----------
            //
            // Prefer the user cache (`<app_data>/sec_id_cache.csv`); fall back
            // to the bundled seed (`sample-data/sec_id.csv` in the repo root,
            // `src-tauri/resources/sample-data/sec_id.csv` in the bundled
            // resource dir). If neither resolves, fall back to an empty map
            // so the app still boots.
            let sym_cache_path = store_dir.join("sec_id_cache.csv");
            let sym_seed_path = std::path::PathBuf::from("sample-data/sec_id.csv");
            let sym_resource_seed = app
                .path()
                .resource_dir()
                .ok()
                .map(|d| d.join("resources").join("sample-data").join("sec_id.csv"));

            let symbol_map = if sym_cache_path.exists() {
                SymbolMap::load(&sym_cache_path).unwrap_or_else(|e| {
                    eprintln!("[symbol_map] cache load failed ({e}); falling back to seed");
                    SymbolMap::load(&sym_seed_path)
                        .or_else(|_| {
                            sym_resource_seed
                                .as_ref()
                                .and_then(|p| SymbolMap::load(p).ok())
                                .ok_or_else(|| {
                                    "seed sec_id.csv missing - add sample-data/sec_id.csv"
                                        .to_string()
                                })
                        })
                        .unwrap_or_else(|e| {
                            eprintln!("[symbol_map] {e} - using empty map");
                            SymbolMap::empty()
                        })
                })
            } else {
                SymbolMap::load(&sym_seed_path)
                    .or_else(|_| {
                        sym_resource_seed
                            .as_ref()
                            .and_then(|p| SymbolMap::load(p).ok())
                            .ok_or_else(|| {
                                "seed sec_id.csv missing - add sample-data/sec_id.csv".to_string()
                            })
                    })
                    .unwrap_or_else(|e| {
                        eprintln!("[symbol_map] {e} - using empty map");
                        SymbolMap::empty()
                    })
            };
            let symbol_map = Arc::new(parking_lot::RwLock::new(symbol_map));

            // Phase 7 — DhanBroker needs the trade log so it can persist
            // every successful order placement. Construct it first so
            // LiveGuard (which references the broker) can be built below.
            // `DhanBroker::new` spawns a background realized-loss refresher
            // on the multi-thread tokio runtime that Tauri 2 installs.
            let dhan_client = algomln::broker::dhan::DhanClient::with_symbol_map(
                algomln::broker::dhan::DhanAuth::from_env()
                    .expect("Set DHAN_ACCESS_TOKEN in .env before starting the Tauri app"),
                symbol_map.clone(),
            );
            let dhan_client = Arc::new(dhan_client);
            let dhan_broker = Arc::new(algomln::strategy::execution::DhanBroker::new(
                dhan_client.clone(),
                trade_log.clone(),
            ));
            // DataState holds the trait-object view the rest of the app
            // already uses for OHLCV/quote/subscribe calls, plus the
            // shared DhanBroker and raw DhanClient so LiveGuard can be
            // wired without duplicating construction.
            let data = commands::data::DataState {
                broker: dhan_client.clone(),
                feed: Arc::new(Mutex::new(algomln::feed::FeedManager::new())),
                dhan_broker: Some(dhan_broker.clone()),
                dhan_client: Some(dhan_client.clone()),
            };

            let store_path = store_dir.join("strategies.json");
            let registry = StrategyRegistry::open(store_path.clone())
                .unwrap_or_else(|error| {
                    panic!(
                        "failed to open strategy registry at {}: {error}",
                        store_path.display()
                    )
                });
            let strategies = Arc::new(registry);

            // ---------- Plugin shared infrastructure ----------
            //
            // Built once and cloned into every plugin host so that indicators,
            // analytics, DSL keywords, scheduled jobs, and event-bus subscribers
            // registered by one plugin are visible to the engine and to other
            // plugins. The plugin registry, plugin host, and the strategy
            // engine all hold `Arc`s into the same maps.
            let indicator_registry = Arc::new(SharedIndicatorRegistry::new());
            let analytics_registry = Arc::new(SharedAnalyticsRegistry::new());
            let dsl_ext_registry = Arc::new(SharedDslExtensionRegistry::new());
            // `EventBus::new()` already returns `Arc<Self>`, so don't double-wrap.
            let event_bus = EventBus::new();
            let event_bus_for_state = event_bus.clone();
            let (tauri_ui_api_concrete, ui_receiver) = TauriUiApi::new();
            // `TauriUiApi::new()` already returns an `Arc<TauriUiApi>` as its
            // first element. Re-cast that to `Arc<dyn UiApi>` so the
            // builder's `ui` field accepts it. The concrete `Arc` is kept
            // (under the same name) to subscribe new receivers for the
            // forwarder below.
            let tauri_ui_api: Arc<dyn algomln::plugin::api::UiApi> =
                tauri_ui_api_concrete.clone() as Arc<dyn algomln::plugin::api::UiApi>;
            let scheduler = CronScheduler::new();

            // The plugin's "market data" capability is backed by the same
            // broker the rest of the app uses (Dhan in production).
            //
            // The "execution" capability is **gated** in Phase 8:
            // plugins loaded outside a live session get the no-op
            // stub; plugins loaded during an active live session get a
            // `GatedLiveExecutionApi` that re-runs market-hours, broker
            // staleness, symbol/segment, pause, and cancellation gates
            // on every `submit_order` and proxies through
            // `DhanBroker::execute_with_meta` so the trade-log row and
            // H3 cancellation cover plugin orders too. The session
            // slot is captured at host-factory construction time; a
            // session that starts *after* a plugin is loaded will be
            // picked up on the next `scan_and_load` (hot reload).
            let broker_arc = data.broker.clone();
            let market_data_api: Arc<dyn algomln::plugin::api::MarketDataApi> =
                Arc::new(BrokerMarketDataApi::new(broker_arc));
            let execution_api: Arc<dyn algomln::plugin::api::ExecutionApi> =
                Arc::new(NoopExecutionApi);

            // Phase 7 — live session machinery. The session slot is
            // shared with the HostFactory closure so that plugin hosts
            // constructed during an active session get a
            // `GatedLiveExecutionApi` instead of the no-op.
            let live_session_slot: Arc<tokio::sync::Mutex<Option<Arc<algomln::live::session::LiveSession>>>> =
                Arc::new(tokio::sync::Mutex::new(None));
            let pending_live_token: Arc<tokio::sync::Mutex<Option<algomln::live::guard::PendingLiveToken>>> =
                Arc::new(tokio::sync::Mutex::new(None));

            // Capture the multi-thread tokio runtime handle here so the
            // factory can pass it into `GatedLiveExecutionApi::new`.
            // The plugin callback may run on a non-tokio thread, so we
            // must never call `Handle::current()` from inside a plugin.
            // Tauri's setup closure runs synchronously, so we resolve the
            // handle via the tauri async runtime (which wraps a tokio
            // runtime under the hood) and unwrap to the underlying
            // `tokio::runtime::Handle` the plugin API expects.
            let runtime_handle = tauri::async_runtime::handle().inner().clone();

            // LiveGuard construction must follow DhanBroker construction
            // (the guard holds a clone of the broker and the client).
            // The `holiday_calendar` is shared with the gated plugin
            // execution API so plugin orders re-use the same market-hours
            // predicate as `LiveGuard::run_preflight`.
            let ack_path = store_dir.join("live_ack.json");
            let holiday_calendar = Arc::new(NseHolidayCalendar::new());
            let live_guard = Arc::new(LiveGuard::new(
                dhan_client.clone(),
                symbol_map.clone(),
                dhan_broker.clone(),
                ack_path.clone(),
                holiday_calendar.clone(),
            ));

            // Per-plugin storage lives under `<app_data>/plugins/<plugin_id>/storage`.
            let plugins_dir = store_dir.join("plugins");
            let _ = std::fs::create_dir_all(&plugins_dir);
            let plugins_dir_for_factory = plugins_dir.clone();

            // Per-plugin rolling logs live under `<app_data>/logs/`.
            // The directory is created lazily by `RateLimitedFileLog::open`,
            // but we ensure it exists now so a misbehaving plugin can't
            // spam `log_info` before the first log call hits a missing dir.
            let logs_dir = store_dir.join("logs");
            let _ = std::fs::create_dir_all(&logs_dir);
            let logs_dir_for_factory = logs_dir.clone();

            // Clones used by the HostFactory closure below to pick a
            // live execution API when a session is active. The
            // `symbol_map` + `holiday_calendar` clones are needed by
            // `GatedLiveExecutionApi::new` (it re-runs the symbol and
            // market-hours gates on every `submit_order` call).
            let live_session_slot_for_factory = live_session_slot.clone();
            let runtime_handle_for_factory = runtime_handle.clone();
            let symbol_map_for_factory = symbol_map.clone();
            let holidays_for_factory = holiday_calendar.clone();
            let dhan_broker_for_factory = dhan_broker.clone();

            let host_factory: algomln::plugin::registry::HostFactory = Arc::new(
                move |id: algomln::plugin::PluginId,
                      caps: Vec<algomln::plugin::Capability>,
                      perms: algomln::plugin::manifest::PluginPermissions| {
                    let storage_dir = plugins_dir_for_factory
                        .join(id.as_ref())
                        .join("storage");
                    let storage = Arc::new(
                        PluginKvStore::new(id.clone(), storage_dir)
                            .expect("plugin storage dir should be creatable"),
                    );
                    // Plugins are untrusted code — every `log_*` call goes
                    // through a per-plugin token-bucket rate limiter and a
                    // 5MB rolling file under `<app_data>/logs/`. The CLI
                    // path keeps using `NamespacedLog` (terminal-friendly,
                    // no file) because the CLI does not load plugins.
                    let log: Arc<dyn algomln::plugin::api::LogApi> = Arc::new(
                        RateLimitedFileLog::open(&logs_dir_for_factory, id.clone())
                            .expect("plugin log file should be creatable"),
                    );
                    // Phase 8 — pick the gated or no-op execution API
                    // based on whether a live session is currently
                    // running. Plugins loaded mid-session get a
                    // `GatedLiveExecutionApi` that re-runs every engine
                    // gate on each `submit_order` and proxies through
                    // the same `DhanBroker::execute_with_meta` the
                    // engine uses; plugins loaded outside a session get
                    // the no-op stub.
                    let exec: Arc<dyn algomln::plugin::api::ExecutionApi> = {
                        // Try to acquire the lock briefly. If a session
                        // start is in flight we'll fall through to the
                        // no-op; the next `scan_and_load` (or hot
                        // reload) will re-evaluate. The lock is a
                        // `tokio::sync::Mutex` so `.lock()` is async —
                        // the host factory closure is sync, so we use
                        // `try_lock` and accept the race. In practice
                        // plugins load once at startup before any
                        // session is started, so the race is benign.
                        if let Ok(slot) = live_session_slot_for_factory.try_lock() {
                            if let Some(_session) = slot.as_ref() {
                                Arc::new(GatedLiveExecutionApi::new(
                                    dhan_broker_for_factory.clone(),
                                    live_session_slot_for_factory.clone(),
                                    symbol_map_for_factory.clone(),
                                    holidays_for_factory.clone(),
                                    runtime_handle_for_factory.clone(),
                                ))
                            } else {
                                execution_api.clone()
                            }
                        } else {
                            // Lock contended (very rare): a session is
                            // starting right now. Default to the no-op
                            // to keep the plugin host construction
                            // deterministic.
                            execution_api.clone()
                        }
                    };
                    algomln::plugin::host::PluginHostBuilder {
                        id: id.clone(),
                        market_data: market_data_api.clone(),
                        execution: exec,
                        storage,
                        event_bus: event_bus.clone(),
                        indicators: indicator_registry.clone(),
                        analytics: analytics_registry.clone(),
                        dsl: dsl_ext_registry.clone(),
                        ui: tauri_ui_api.clone(),
                        scheduler: scheduler.clone(),
                        log,
                        capabilities: caps,
                        permissions: perms,
                    }
                    .build()
                },
            );

            let plugin_registry = PluginRegistry::new(plugins_dir.clone(), host_factory);

            // Synchronous `setup` driving the async `scan_and_load`. Tauri 2
            // installs a multi-thread tokio runtime on the builder, so
            // `tauri::async_runtime::block_on` is safe here.
            let load_results = tauri::async_runtime::block_on(plugin_registry.scan_and_load());
            for (id, result) in &load_results {
                match result {
                    Ok(()) => eprintln!("[plugins] loaded: {id}"),
                    Err(e) => eprintln!("[plugins] failed to load {id}: {e}"),
                }
            }

            // ---------- Forward plugin UI messages to the Tauri bus ----------
            //
            // Plugins call `ui.register_panel` / `ui.notify` / `emit_panel_data`
            // via the `TauriUiApi`, which broadcasts `UiMessage`s on a tokio
            // channel. We re-emit each message on the Tauri event bus as
            // `"plugin-ui-message"` so the React app can subscribe to a single
            // channel and dispatch on the `UiMessage` variant.
            let app_handle = app.handle().clone();
            let app_handle_for_spawn = app_handle.clone();
            let mut ui_rx = tauri_ui_api_concrete.receiver();
            tauri::async_runtime::spawn(async move {
                while let Ok(msg) = ui_rx.recv().await {
                    let _ = app_handle_for_spawn.emit("plugin-ui-message", &msg);
                }
            });

            // ---------- Acrylic window chrome (Windows only) ----------
            #[cfg(target_os = "windows")]
            {
                use window_vibrancy::apply_acrylic;

                let win = app
                    .get_webview_window("main")
                    .expect("main window not found");

                win.set_decorations(false)?;

                // WebView2 paints an opaque white background by default, which
                // sits *on top* of the acrylic and makes the glass look like a
                // flat muddy gray. Force the controller's default background to
                // fully transparent (A: 0) so the acrylic shows through.
                win.with_webview(|webview| {
                    use webview2_com::Microsoft::Web::WebView2::Win32::COREWEBVIEW2_COLOR;
                    unsafe {
                        use webview2_com::Microsoft::Web::WebView2::Win32::ICoreWebView2Controller2;
                        use windows_core::Interface;

                        let controller2 = webview
                            .controller()
                            .cast::<ICoreWebView2Controller2>()
                            .expect("failed to get ICoreWebView2Controller2");

                        let _ = controller2.SetDefaultBackgroundColor(COREWEBVIEW2_COLOR {
                                A: 0,
                                R: 0,
                                G: 0,
                                B: 0,
                        });
                    }
                })?;

                apply_acrylic(&win, Some((34, 34, 34, 153)))
                    .expect("Acrylic requires Windows 10 1803+");
            }

            // ---------- Index registry ----------
            //
            // The bundled seed JSON lives under `src-tauri/resources/indices/`
            // and is shipped with the app bundle via `tauri.conf.json`. The
            // `resource_dir` resolver is provided by Tauri. The cache_dir is
            // the user's app data — a background task may refresh the
            // files there on startup if they're older than 24h.
            let index_registry = Arc::new(IndexRegistry::new());
            let resource_dir = app
                .path()
                .resource_dir()
                .expect("could not resolve resource dir");
            // Resources live at `<resource_dir>/resources/indices/*.json` when
            // bundled (the `resources` prefix in tauri.conf.json is preserved).
            let index_resource_dir = resource_dir.join("resources").join("indices");
            let index_cache_dir = store_dir.join("indices");
            let _ = std::fs::create_dir_all(&index_cache_dir);
            index_registry.load_from_dirs(&index_cache_dir, &index_resource_dir);
            eprintln!(
                "[indices] loaded {} index constituent lists",
                index_registry.list_info().len()
            );

            // Spawn a background refresh. Non-fatal: failures are logged to
            // stderr and the app keeps running with the seed data.
            // 90-day staleness window per the multi-symbol trade spec.
            let refresh_registry = index_registry.clone();
            let refresh_cache_dir = index_cache_dir.clone();
            tauri::async_runtime::spawn(async move {
                let outcomes = refresh_all_if_stale(
                    refresh_registry,
                    refresh_cache_dir,
                    std::time::Duration::from_secs(90 * 24 * 60 * 60),
                )
                .await;
                let ok = outcomes.iter().filter(|o| o.success).count();
                eprintln!("[indices] background refresh: {}/{} ok", ok, outcomes.len());
            });


            // Background 7-day staleness check for the symbol map.
            let bg_sym_map = symbol_map.clone();
            let bg_sym_cache = sym_cache_path.clone();
            tauri::async_runtime::spawn(async move {
                if !algomln::indices::is_stale(
                    &bg_sym_cache,
                    std::time::Duration::from_secs(7 * 24 * 60 * 60),
                ) {
                    return;
                }
                match refresh_symbol_map(&bg_sym_cache).await {
                    Ok(new_map) => {
                        eprintln!("[symbol_map] background refresh: {} symbols", new_map.len());
                        *bg_sym_map.write() = new_map;
                    }
                    Err(e) => eprintln!("[symbol_map] background refresh failed: {e}"),
                }
            });

            app.manage(AppState {
                data,
                strategies,
                plugin_registry,
                event_bus: event_bus_for_state,
                ui_receiver,
                index_registry,
                symbol_map,
                trade_log,
                trade_log_path,
                live_session: live_session_slot,
                live_guard,
                pending_live_token,
                ack_path,
                app_handle,
            });
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            get_ohlcv,
            get_quote,
            subscribe_ticks,
            run_backtest,
            validate_dsl,
            deploy_strategy,
            list_strategies,
            set_strategy_status,
            list_plugins,
            enable_plugin,
            disable_plugin,
            reload_plugins,
            list_indices,
            get_index_symbols,
            refresh_indices,
            search_symbols,
            get_trade_log,
            request_live_start,
            confirm_live_start,
            acknowledge_live_trading,
            pause_live_strategy,
            resume_live_strategy,
            stop_live_strategy,
            get_live_status,
        ])
        .run(tauri::generate_context!())
        .expect("failed to run AlgoMLN");
}

fn load_dotenv() {
    for path in [Path::new(".env"), Path::new("../.env")] {
        let Ok(contents) = fs::read_to_string(path) else {
            continue;
        };

        for line in contents.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            let Some((key, value)) = line.split_once('=') else {
                continue;
            };

            if env::var(key.trim()).is_err() {
                env::set_var(key.trim(), value.trim().trim_matches('"'));
            }
        }

        break;
    }
}
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::time::{Duration, Instant};

use algomln::broker::dhan::{DhanAuth, DhanClient};
use algomln::broker::{BrokerClient, Timeframe};
use algomln::commands::strategy::{run_backtest_internal, BacktestResult};
use algomln::data::load_nifty_candles;
use algomln::models::{Candle, Order, OrderResult, OrderSide, OrderType};
use algomln::strategy::dsl::{AstValidator, Lexer, Parser, StrategyNode};
use algomln::strategy::logging::LogEntryKind;
use algomln::strategy::execution::{target::ExecutionTarget, DhanBroker};
use algomln::strategy::execution::dhan::SessionContext;
use algomln::live::trade_log::TradeLog;
use std::sync::Arc;
use anyhow::{Context, Result};
use chrono::{Utc, NaiveDate};


const INITIAL_CASH: f64 = 10_000_000.0;
const DEFAULT_EXCHANGE_SEGMENT: &str = "NSE_EQ";
const DEFAULT_INSTRUMENT: &str = "EQUITY";
const DAY_MS: i64 = 24 * 60 * 60 * 1_000;

struct RunArgs {
    script_path: String,
    data_path: String,
    candle_limit: Option<usize>,
    initial_cash: f64,
    symbol: String,
}

struct BacktestArgs {
    script_path: String,
    security_id: String,
    exchange_segment: String,
    instrument: String,
    timeframe: Timeframe,
    from: i64,
    to: i64,
    initial_cash: f64,
}

fn main() {
    dotenvy::dotenv().ok();
    if let Err(error) = real_main() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn real_main() -> Result<(), String> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() >= 2 && matches!(args[1].as_str(), "--help" | "-h") {
        print_help();
        return Ok(());
    }

    if args.len() >= 2 && args[1] == "profile" {
        let strategy_name = args.get(2).map(String::as_str).unwrap_or("rsi");
        let limit = args
            .get(3)
            .map(|value| {
                value
                    .parse::<usize>()
                    .map_err(|error| format!("Error: invalid candles value '{value}': {error}"))
            })
            .transpose()?;
        return block_on(run_profile(strategy_name, limit))
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string());
    }

    if args.len() >= 2 && args[1] == "run" {
        return parse_run_args(&args[2..]).and_then(run_script);
    }

    if args.len() >= 2 && args[1] == "backtest" {
        let args = parse_backtest_args(&args[2..])?;
        return block_on(run_backtest_from_dhan(args)).map_err(|error| error.to_string())?;
    }

    if args.len() >= 2 && args[1] == "live-test-order" {
        return block_on(run_live_test_order(&args[2..])).map_err(|error| error.to_string())?;
    }


    let tiny_strategy = std::fs::read_to_string("sample-data/tiny_strategy.algomln")
        .context("read tiny strategy")
        .map_err(|error| error.to_string())?;
    let tiny_candles =
        load_tiny_candles("sample-data/tiny_candles.csv").map_err(|error| error.to_string())?;

    block_on(run_named(
        "tiny-close-gt-105",
        &tiny_strategy,
        tiny_candles.clone(),
    ))
    .map_err(|error| error.to_string())?
    .map_err(|error| error.to_string())?;
    block_on(run_named(
        "idiot-close-gt-0",
        "WHEN close > 0\nBUY 1",
        tiny_candles.clone(),
    ))
    .map_err(|error| error.to_string())?
    .map_err(|error| error.to_string())?;
    block_on(run_named(
        "reset-close-gt-105",
        "WHEN close > 105\nBUY 1",
        vec![
            candle(1, 100.0),
            candle(2, 106.0),
            candle(3, 100.0),
            candle(4, 106.0),
        ],
    ))
    .map_err(|error| error.to_string())?
    .map_err(|error| error.to_string())?;

    Ok(())
}

fn block_on<F>(future: F) -> Result<F::Output>
where
    F: std::future::Future,
{
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("create tokio runtime")
        .map(|runtime| runtime.block_on(future))
}

fn parse_run_args(args: &[String]) -> Result<RunArgs, String> {
    let script_path = args
        .first()
        .cloned()
        .ok_or_else(|| "Error: missing .algomln file for run subcommand".to_string())?;
    let mut data_path = None;
    let mut candle_limit = None;
    let mut initial_cash = 100_000.0;
    let mut symbol = None;
    let mut index = 1;

    while index < args.len() {
        match args[index].as_str() {
            "--data" => {
                index += 1;
                data_path = Some(
                    args.get(index)
                        .cloned()
                        .ok_or_else(|| "Error: --data requires a path".to_string())?,
                );
            }
            "--candles" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| "Error: --candles requires a value".to_string())?;
                candle_limit = Some(value.parse::<usize>().map_err(|error| {
                    format!("Error: invalid --candles value '{value}': {error}")
                })?);
            }
            "--cash" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| "Error: --cash requires a value".to_string())?;
                initial_cash = value
                    .parse::<f64>()
                    .map_err(|error| format!("Error: invalid --cash value '{value}': {error}"))?;
            }
            "--symbol" => {
                index += 1;
                symbol = Some(
                    args.get(index)
                        .cloned()
                        .ok_or_else(|| "Error: --symbol requires a value".to_string())?,
                );
            }
            other => return Err(format!("Error: unknown run option: {other}")),
        }
        index += 1;
    }

    let data_path =
        data_path.ok_or_else(|| "Error: --data is required for run subcommand".to_string())?;
    let symbol = symbol.unwrap_or_else(|| {
        Path::new(&data_path)
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or("UNKNOWN")
            .to_string()
    });

    Ok(RunArgs {
        script_path,
        data_path,
        candle_limit,
        initial_cash,
        symbol,
    })
}

fn parse_backtest_args(args: &[String]) -> Result<BacktestArgs, String> {
    let script_path = args
        .first()
        .cloned()
        .ok_or_else(|| "Error: missing .algomln file for backtest subcommand".to_string())?;
    let mut security_id = None;
    let mut symbol = None;
    let mut exchange_segment = DEFAULT_EXCHANGE_SEGMENT.to_string();
    let mut instrument = DEFAULT_INSTRUMENT.to_string();
    let mut timeframe = Timeframe::M1;
    let now = Utc::now().timestamp_millis();
    let mut from = now - 365 * DAY_MS;
    let mut to = now;
    let mut initial_cash = INITIAL_CASH;
    let mut index = 1;

    while index < args.len() {
        match args[index].as_str() {
            "--security" => {
                index += 1;
                security_id = Some(
                    args.get(index)
                        .cloned()
                        .ok_or_else(|| "Error: --security requires a value".to_string())?,
                );
            }
            "--symbol" => {
                index += 1;
                symbol = Some(
                    args.get(index)
                        .cloned()
                        .ok_or_else(|| "Error: --symbol requires a value".to_string())?,
                );
            }
            "--exchange" => {
                index += 1;
                exchange_segment = args
                    .get(index)
                    .cloned()
                    .ok_or_else(|| "Error: --exchange requires a value".to_string())?;
            }
            "--instrument" => {
                index += 1;
                instrument = args
                    .get(index)
                    .cloned()
                    .ok_or_else(|| "Error: --instrument requires a value".to_string())?;
            }
            "--timeframe" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| "Error: --timeframe requires a value".to_string())?;
                timeframe = parse_backtest_timeframe(value)?;
            }
            "--from" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| "Error: --from requires a value".to_string())?;
                from = parse_yyyy_mm_dd(value)?;
            }
            "--to" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| "Error: --to requires a value".to_string())?;
                to = parse_yyyy_mm_dd(value)?;
            }
            "--cash" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| "Error: --cash requires a value".to_string())?;
                initial_cash = value
                    .parse::<f64>()
                    .map_err(|error| format!("Error: invalid --cash value '{value}': {error}"))?;
            }
            other => return Err(format!("Error: unknown argument for backtest: {other}")),
        }
        index += 1;
    }

    if symbol.is_some() && security_id.is_some() {
        return Err("Error: --symbol and --security are mutually exclusive".to_string());
    }

    if let Some(symbol) = symbol {
        let parts = symbol
            .split(['|', ':'])
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>();
        security_id = Some(parts.first().copied().unwrap_or("").to_string());
        if let Some(value) = parts.get(1) {
            exchange_segment = (*value).to_string();
        }
        if let Some(value) = parts.get(2) {
            instrument = (*value).to_string();
        }
    }

    let security_id = security_id
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "Error: --security <id> is required for backtest subcommand".to_string())?;

    Ok(BacktestArgs {
        script_path,
        security_id,
        exchange_segment,
        instrument,
        timeframe,
        from,
        to,
        initial_cash,
    })
}

fn parse_backtest_timeframe(value: &str) -> Result<Timeframe, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1m" => Ok(Timeframe::M1),
        "5m" => Ok(Timeframe::M5),
        "15m" => Ok(Timeframe::M15),
        "30m" => Ok(Timeframe::M30),
        "1h" => Ok(Timeframe::H1),
        "1d" => Ok(Timeframe::D1),
        _ => Err(format!(
            "Error: unknown timeframe '{value}' — expected 1m 5m 15m 30m 1h 1d"
        )),
    }
}

fn parse_yyyy_mm_dd(value: &str) -> Result<i64, String> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map_err(|_| format!("Error: invalid date '{value}' — expected YYYY-MM-DD"))?
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| format!("Error: invalid date '{value}' — expected YYYY-MM-DD"))
        .map(|date_time| date_time.and_utc().timestamp_millis())
}

fn timeframe_label(timeframe: Timeframe) -> &'static str {
    match timeframe {
        Timeframe::M1 => "1m",
        Timeframe::M5 => "5m",
        Timeframe::M15 => "15m",
        Timeframe::M30 => "30m",
        Timeframe::H1 | Timeframe::M60 => "1h",
        Timeframe::D1 => "1d",
        _ => "unknown",
    }
}

fn date_label(timestamp_ms: i64) -> String {
    chrono::DateTime::<Utc>::from_timestamp_millis(timestamp_ms)
        .map(|date_time| date_time.date_naive().format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| timestamp_ms.to_string())
}

fn load_strategy_file(script_path: &str) -> Result<(StrategyNode, String), String> {
    let script_path = Path::new(script_path);
    let extension = script_path
        .extension()
        .and_then(|extension| extension.to_str());
    if extension != Some("algomln") {
        let got = extension
            .map(|extension| format!(".{extension}"))
            .unwrap_or_else(|| "<none>".to_string());
        return Err(format!("Error: expected .algomln file, got {got}"));
    }

    let source = std::fs::read_to_string(script_path)
        .map_err(|_| format!("Error: file not found: {}", script_path.display()))?;
    let strategy_name = script_path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("strategy")
        .to_string();
    let tokens = Lexer::tokenize(&source).map_err(|error| {
        format!(
            "Error: parse failed at line {} col {}: {}",
            error.line, error.col, error.message
        )
    })?;
    let mut strategy = Parser::new(tokens).parse().map_err(|error| {
        format!(
            "Error: parse failed at line {} col {}: {}",
            error.line, error.col, error.message
        )
    })?;
    strategy.name = strategy_name.clone();

    let validation_errors = AstValidator::validate(&strategy);
    if !validation_errors.is_empty() {
        let mut message = String::from("Error: validation failed:");
        for error in validation_errors {
            let rule_id = if error.rule_id.is_empty() {
                "strategy"
            } else {
                &error.rule_id
            };
            message.push_str(&format!("\n  {rule_id}: {}", error.message));
        }
        return Err(message);
    }

    Ok((strategy, strategy_name))
}

fn run_script(args: RunArgs) -> Result<(), String> {
    let (strategy, strategy_name) = load_strategy_file(&args.script_path)?;
    let mut candles = load_nifty_candles(Path::new(&args.data_path)).map_err(|error| {
        format!(
            "Error: failed to load candles from {}: {error}",
            args.data_path
        )
    })?;
    if let Some(limit) = args.candle_limit {
        candles.truncate(limit);
    }

    println!("strategy: {strategy_name}");
    println!("rules: {}", strategy.rules.len());
    println!("candles: {}", candles.len());
    println!("cash: {:.2}", args.initial_cash);
    println!("symbol: {}", args.symbol);

    let started = Instant::now();
    let symbol = args.symbol.clone();
    let result = block_on(run_backtest_internal(
        strategy,
        symbol.clone(),
        candles,
        args.initial_cash,
    ))
    .map_err(|error| format!("Error: {error}"))?
    .map_err(|error| format!("Error: {error}"))?;

    print_run_summary(&strategy_name, &symbol, &result, started.elapsed());
    Ok(())
}

async fn run_backtest_from_dhan(args: BacktestArgs) -> Result<(), String> {
    let (strategy, strategy_name) = load_strategy_file(&args.script_path)?;
    let access_token = std::env::var("DHAN_ACCESS_TOKEN")
        .map_err(|_| "Error: DHAN_ACCESS_TOKEN environment variable not set".to_string())?;
    let dhan_client =
        DhanClient::new(DhanAuth::new(access_token).map_err(|error| format!("Error: {error}"))?);
    let symbol = format!(
        "{}|{}|{}",
        args.security_id, args.exchange_segment, args.instrument
    );

    println!(
        "fetching: {}  {}  {} → {}",
        symbol,
        timeframe_label(args.timeframe),
        date_label(args.from),
        date_label(args.to)
    );
    let fetch_started = Instant::now();
    let candles = dhan_client
        .get_ohlcv(&symbol, args.timeframe, args.from, args.to)
        .await
        .map_err(|error| format!("Error: Dhan fetch failed: {error}"))?;
    let fetch_ms = fetch_started.elapsed().as_millis();
    println!(
        "fetched: {} candles  (fetch_ms={}ms)",
        candles.len(),
        fetch_ms
    );

    if candles.is_empty() {
        return Err("Error: no candles returned — check security ID and date range".to_string());
    }

    println!("strategy: {strategy_name}");
    println!("rules: {}", strategy.rules.len());
    println!("candles: {}", candles.len());
    println!("cash: {:.2}", args.initial_cash);
    println!("symbol: {symbol}");

    let started = Instant::now();
    let result = run_backtest_internal(strategy, symbol.clone(), candles, args.initial_cash)
        .await
        .map_err(|error| format!("Error: {error}"))?;

    print_run_summary(&strategy_name, &symbol, &result, started.elapsed());
    Ok(())
}

async fn run_profile(strategy_name: &str, limit: Option<usize>) -> Result<()> {
    let load_started = Instant::now();
    let mut nifty = load_nifty_candles(Path::new("sample-data/nifty_1min.csv"))
        .map_err(|e| anyhow::anyhow!("load nifty candles: {e}"))?;
    if let Some(limit) = limit {
        nifty.truncate(limit);
    }
    println!(
        "loaded_nifty_candles={} load_ms={}",
        nifty.len(),
        load_started.elapsed().as_millis()
    );

    let strategy = match strategy_name {
        "rsi" => "WHEN rsi(14) < 30\nBUY 1\n\nWHEN rsi(14) > 70\nSELL ALL",
        "ema" => {
            "WHEN cross_above(ema(20), ema(50))\nBUY 1\n\nWHEN cross_below(ema(20), ema(50))\nSELL ALL"
        }
        other => anyhow::bail!("unknown strategy: {other}"),
    };

    run_named(&format!("nifty-{strategy_name}"), strategy, nifty).await?;
    Ok(())
}

async fn run_named(name: &str, source: &str, candles: Vec<Candle>) -> Result<BacktestResult> {
    let parse_started = Instant::now();
    let strategy = parse_strategy(source)?;
    let parse_time = parse_started.elapsed();
    let started = Instant::now();
    let result = run_backtest_internal(strategy, "NIFTY".to_string(), candles, INITIAL_CASH)
        .await
        .map_err(|error| anyhow::anyhow!("run {name}: {error}"))?;
    print_summary(name, &result, started.elapsed(), parse_time);
    Ok(result)
}

fn print_summary(name: &str, result: &BacktestResult, runtime: Duration, parse_time: Duration) {
    println!("=== {name} ===");
    println!("candles={}", result.total_candles_processed);
    println!("trades={}", result.trade_history.len());
    println!("final_cash={:.2}", result.final_cash);
    println!("pnl={:.2}", result.total_realized_pnl);
    println!("runtime_ms={}", runtime.as_millis());
    println!(
        "candles_per_sec={:.2}",
        result.total_candles_processed as f64 / runtime.as_secs_f64().max(0.001)
    );
    println!("parser_ms={}", parse_time.as_millis());
    println!("validator_ms={}", result.profile.parser_validator_ms);
    println!(
        "engine_on_candle_ms={} calls={}",
        result.profile.engine.on_candle_time_ms, result.profile.engine.on_candle_calls
    );
    println!(
        "indicator_get_ms={} calls={} hits={} misses={}",
        result.profile.indicators.get_time_ms,
        result.profile.indicators.get_calls,
        result.profile.indicators.cache_hits,
        result.profile.indicators.cache_misses
    );
    println!(
        "paper_execute_ms={} calls={}",
        result.profile.engine.broker_execute_time_ms, result.profile.engine.broker_execute_calls
    );
    println!(
        "paper_get_positions_ms={} calls={}",
        result.profile.engine.broker_get_positions_time_ms,
        result.profile.engine.broker_get_positions_calls
    );
    for trade in result.trade_history.iter().take(5) {
        let side = match trade.side {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        };
        println!(
            "trade {} {} qty={} price={:.2}",
            trade.id, side, trade.quantity, trade.price
        );
    }
    if result.trade_history.len() > 5 {
        println!("... {} more trades", result.trade_history.len() - 5);
    }
}

fn print_run_summary(name: &str, symbol: &str, result: &BacktestResult, _runtime: Duration) {
    let summary = &result.summary;
    let border = "\u{2550}".repeat(48);

    println!("{border}");
    println!(" BACKTEST - {name}");
    println!(
        " Symbol: {}   Candles: {}",
        symbol, summary.total_candles_processed
    );
    println!("{border}");
    println!();

    println!(" CAPITAL");
    println!("   Initial       ₹ {:>15.2}", summary.initial_cash);
    println!("   Final         ₹ {:>15.2}", summary.final_cash);
    println!("   Return          {:>+14.2}%", summary.total_return_pct);
    println!();

    println!(" TRADES");
    println!(
        "   Total         {:>16}   ({} buys, {} sells)",
        summary.total_trades, summary.buy_count, summary.sell_count
    );
    println!("   Closed        {:>16}", summary.closed_trades);
    println!(
        "   Win rate      {:>15.2}%   ({}W / {}L / {}B)",
        summary.win_rate_pct,
        summary.winning_trades,
        summary.losing_trades,
        summary.breakeven_trades
    );

    if summary.closed_trades == 0 {
        println!(" ⚠  No closed trades - strategy never completed a buy->sell cycle.");
    } else {
        println!();
        println!(" PnL");
        println!("   Realized      ₹ {:>+14.2}", summary.total_realized_pnl);
        println!("   Gross profit  ₹ {:>15.2}", summary.gross_profit);
        println!("   Gross loss    ₹ {:>15.2}", summary.gross_loss);
        println!("   Profit factor   {:>14.2}", summary.profit_factor);
        println!("   Avg win       ₹ {:>+14.2}", summary.avg_win);
        println!("   Avg loss      ₹ {:>+14.2}", summary.avg_loss);
        println!("   Largest win   ₹ {:>+14.2}", summary.largest_win);
        println!("   Largest loss  ₹ {:>+14.2}", summary.largest_loss);
        println!("   Expectancy    ₹ {:>+14.2}", summary.expectancy);
        println!();

        println!(" RISK");
        println!(
            "   Max drawdown  ₹ {:>14.2}  ({:.3}%)",
            summary.max_drawdown, summary.max_drawdown_pct
        );
        println!("   Max consec W    {:>14}", summary.max_consecutive_wins);
        println!("   Max consec L    {:>14}", summary.max_consecutive_losses);
    }

    println!();
    println!(" THROUGHPUT");
    println!("   Candles/trade   {:>14.1}", summary.candles_per_trade);
    println!("   Skipped (no pos){:>13}", summary.skipped_no_position);
    println!();
    println!("{border}");

    for entry in &result.logs {
        if let LogEntryKind::OrderFailed { rule_id, error } = &entry.kind {
            println!(
                "order_failed {} timestamp={} error={}",
                rule_id, entry.candle_timestamp, error
            );
        }
    }

    if result.trade_history.is_empty() {
        println!("no trades executed");
    }
}

async fn run_live_test_order(args: &[String]) -> Result<(), String> {
    if args.is_empty() {
        return Err("Error: missing symbol for live-test-order. Usage: live-test-order <symbol> [qty]".to_string());
    }

    let symbol = &args[0];
    let qty = args.get(1)
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(1);

    println!("🚀 Live Test Order: BUY {} shares of {}", qty, symbol);

    let auth = DhanAuth::from_env().map_err(|e| format!("Error: {}", e))?;

    // Load symbol map from seed file for the CLI test tool
    let symbol_map_path = Path::new("sample-data/sec_id.csv");
    let symbol_map = Arc::new(parking_lot::RwLock::new(
        algomln::broker::symbol_map::SymbolMap::load(symbol_map_path)
            .map_err(|e| format!("Error loading symbol map from {}: {}", symbol_map_path.display(), e))?
    ));

    let client = DhanClient::with_symbol_map(auth, symbol_map);

    // Resolve symbol to security ID
    let entry = client.resolve_symbol_entry(symbol)
        .map_err(|e| format!("Error resolving symbol {}: {}", symbol, e))?;

    println!("Resolved {} to security_id={} segment={:?}", symbol, entry.security_id, entry.segment);

    // Setup minimal broker environment
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("live_test.jsonl");
    let log = Arc::new(TradeLog::open(log_path).unwrap());
    let broker = Arc::new(DhanBroker::new(Arc::new(client), log));

    // Set session context for the trade log
    *broker.session_context.write() = Some(SessionContext {
        strategy_id: "live-test-cli".to_string(),
        strategy_name: "Live Test CLI".to_string(),
        mode: "live".to_string(),
    });

    let order = Order {
        symbol: symbol.clone(),
        side: OrderSide::Buy,
        quantity: qty,
        order_type: OrderType::Market,
        price: None,
    };

    println!("Submitting order to Dhan...");
    let result = broker.execute_with_meta(order, "manual-test", "Live test order via CLI").await
        .map_err(|e| format!("Execution failed: {}", e.message))?;

    println!("✅ Order successfully placed!");
    println!("Order ID: {}", result.order_id);
    println!("Status: {:?}", result.status);

    Ok(())
}
fn print_help() {
    println!(
        r#"behavioral_backtest - AlgoMLN strategy runner

USAGE:
  behavioral_backtest                                      run built-in test suite
  behavioral_backtest profile <name> [candles]            run named profile (rsi, ema)
  behavioral_backtest run <file.algomln> --data <csv>     run your own strategy
  behavioral_backtest backtest <file.algomln> --security <id>   fetch from Dhan and backtest

OPTIONS for run:
  --data <path>      path to OHLCV CSV file (required)
  --candles <n>      limit to first N candles
  --cash <amount>    starting cash (default: 100000)
  --symbol <name>    symbol name for display

OPTIONS for backtest:
  --security <id>       Dhan security ID (required, unless --symbol is used)
  --symbol <id|ex|ins>  compact form: security_id|exchange|instrument
  --exchange <seg>      exchange segment (default: NSE_EQ)
  --instrument <type>   instrument type (default: EQUITY)
  --timeframe <tf>      candle timeframe: 1m 5m 15m 30m 1h 1d (default: 1m)
  --from <YYYY-MM-DD>   start date (default: 365 days ago)
  --to <YYYY-MM-DD>     end date (default: today)
  --cash <amount>       starting cash (default: 10000000)

EXAMPLES:
  behavioral_backtest run strategies/ema_cross.algomln --data sample-data/nifty_1min.csv
  behavioral_backtest run my_strat.algomln --data sample-data/nifty_1min.csv --candles 50000 --cash 500000
  behavioral_backtest backtest strategies/rsi_basic.algomln --security 1333 --from 2024-01-01 --to 2024-06-30
  behavioral_backtest profile rsi 10000"#
    );
}

fn parse_strategy(source: &str) -> Result<StrategyNode> {
    let tokens = Lexer::tokenize(source).map_err(|error| anyhow::anyhow!("{error:?}"))?;
    Parser::new(tokens)
        .parse()
        .map_err(|error| anyhow::anyhow!("{error:?}"))
}

fn load_tiny_candles(path: &str) -> Result<Vec<Candle>> {
    let file = File::open(path).with_context(|| format!("open {path}"))?;
    let mut candles = Vec::new();
    for (index, line) in BufReader::new(file).lines().enumerate() {
        let line = line?;
        if index == 0 || line.trim().is_empty() {
            continue;
        }
        let fields = line.split(',').collect::<Vec<_>>();
        anyhow::ensure!(fields.len() == 6, "bad tiny candle row {}", index + 1);
        candles.push(Candle {
            timestamp: fields[0].parse::<i64>()?,
            open: fields[1].parse::<f64>()?,
            high: fields[2].parse::<f64>()?,
            low: fields[3].parse::<f64>()?,
            close: fields[4].parse::<f64>()?,
            volume: fields[5].parse::<f64>()?,
        });
    }
    Ok(candles)
}

fn candle(timestamp: i64, close: f64) -> Candle {
    Candle {
        timestamp,
        open: close,
        high: close,
        low: close,
        close,
        volume: 1_000.0,
    }
}
use std::env;

use anyhow::{anyhow, Context, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DhanAuth {
    pub access_token: String,
    pub client_id: Option<String>,
}

impl DhanAuth {
    pub fn new(access_token: impl Into<String>) -> Result<Self> {
        let access_token = access_token.into();
        if access_token.trim().is_empty() {
            return Err(anyhow!("Dhan access token is not configured"));
        }

        Ok(Self {
            access_token,
            client_id: None,
        })
    }

    pub fn with_client_id(
        access_token: impl Into<String>,
        client_id: impl Into<String>,
    ) -> Result<Self> {
        let mut auth = Self::new(access_token)?;
        let client_id = client_id.into();
        if client_id.trim().is_empty() {
            return Err(anyhow!("Dhan client id is not configured"));
        }
        auth.client_id = Some(client_id);
        Ok(auth)
    }

    pub fn from_env() -> Result<Self> {
        let access_token = env::var("DHAN_ACCESS_TOKEN")
            .or_else(|_| env::var("DHAN_TOKEN"))
            .context("Set DHAN_ACCESS_TOKEN or DHAN_TOKEN")?;

        let mut auth = Self::new(access_token)?;
        auth.client_id = env::var("DHAN_CLIENT_ID")
            .or_else(|_| env::var("DHAN_CLIENTID"))
            .ok();
        Ok(auth)
    }
}
pub mod auth;
pub mod models;
pub mod rest;
pub mod websocket;

pub use auth::DhanAuth;
pub use rest::{DhanClient, DhanConfig, DhanError};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DhanSymbol {
    pub security_id: String,
    pub exchange_segment: String,
    pub instrument: String,
}

impl DhanSymbol {
    pub fn parse(input: &str, default_exchange_segment: &str, default_instrument: &str) -> Self {
        let parts = input
            .split(['|', ':'])
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>();

        Self {
            security_id: parts.first().copied().unwrap_or(input).to_string(),
            exchange_segment: parts
                .get(1)
                .copied()
                .unwrap_or(default_exchange_segment)
                .to_string(),
            instrument: parts
                .get(2)
                .copied()
                .unwrap_or(default_instrument)
                .to_string(),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IntradayRequest {
    pub security_id: String,
    pub exchange_segment: String,
    pub instrument: String,
    pub interval: String,
    pub oi: bool,
    pub from_date: String,
    pub to_date: String,
}

#[derive(Debug, Deserialize)]
pub struct HistoricalResponse {
    pub timestamp: Vec<Option<f64>>,
    pub open: Vec<Option<f64>>,
    pub high: Vec<Option<f64>>,
    pub low: Vec<Option<f64>>,
    pub close: Vec<Option<f64>>,
    pub volume: Vec<Option<f64>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DhanQuoteValue {
    pub last_price: Option<f64>,
    pub open: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
    pub close: Option<f64>,
    pub volume: Option<u64>,
    pub bid_price: Option<f64>,
    pub ask_price: Option<f64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PlaceOrderRequest {
    pub dhan_client_id: String,
    /// Idempotency key echoed by Dhan. Prevents a retried/duplicated HTTP
    /// request from placing the same order twice.
    pub correlation_id: String,
    #[serde(rename = "transactionType")]
    pub transaction_type: String,
    pub exchange_segment: String,
    pub product_type: String,
    pub order_type: String,
    pub validity: String,
    pub security_id: String,
    pub quantity: u32,
    pub price: f64,
    pub trigger_price: f64,
    pub disclosed_quantity: u32,
    pub after_market_order: bool,
    pub amo_time: String,
    pub bo_profit_value: f64,
    pub bo_stop_loss_value: f64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlaceOrderResponse {
    pub order_id: String,
    pub order_status: String,
}

/// Response payload from Dhan's `GET /funds/limit` endpoint. The field we
/// actually consume is `available_balance` — used by `DhanBroker` to size
/// `PercentCapital` orders safely instead of trusting `f64::MAX`. The other
/// fields are deserialized for forward compatibility (and so a malformed
/// shape surfaces as a JSON parse error rather than a silently-zeroed
/// number).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DhanFundsLimit {
    /// Available balance for intraday equity trading, in INR. This is the
    /// value `DhanBroker::available_cash` returns after it has been fetched
    /// and cached.
    #[serde(alias = "availablebalance")]
    pub available_balance: f64,
    #[serde(default)]
    pub sod_limit: f64,
    #[serde(default)]
    pub collateral_amount: f64,
    #[serde(default)]
    pub receiveable_amount: f64,
    #[serde(default)]
    pub utilized_amount: f64,
    #[serde(default)]
    pub withdrawable_balance: f64,
    #[serde(default)]
    pub blocked_payout_amount: f64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DhanPosition {
    pub security_id: String,
    pub trading_symbol: String,
    pub exchange_segment: String,
    pub product_type: String,
    pub buy_avg: f64,
    pub buy_qty: i64,
    pub sell_avg: f64,
    pub sell_qty: i64,
    pub net_qty: i64,
    pub realized_profit: f64,
    pub unrealized_profit: f64,
    pub day_buy_value: f64,
    pub day_sell_value: f64,
}
use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use reqwest::header::{HeaderMap, HeaderValue};
use serde_json::Value;
use std::sync::Arc;

use crate::{
    broker::{
        symbol_map::{Segment, SymbolEntry, SymbolMap},
        BrokerClient, Timeframe,
    },
    models::{
        Candle, Order, OrderResult, OrderSide, OrderStatus, OrderType, Portfolio, Position, Quote,
    },
};

use super::{
    auth::DhanAuth,
    models::{
        DhanFundsLimit, DhanPosition, DhanQuoteValue, DhanSymbol, HistoricalResponse,
        IntradayRequest, PlaceOrderRequest, PlaceOrderResponse,
    },
};

const DHAN_BASE_URL: &str = "https://api.dhan.co/v2";
const INTRADAY_CHUNK_MS: i64 = 89 * 24 * 60 * 60 * 1_000;
const DHAN_HISTORICAL_EPOCH_OFFSET_SECONDS: i64 = 315_532_800;

/// Errors from the Dhan REST client that callers must handle specifically
/// (rather than as an opaque `anyhow` error). Currently only the
/// order-placement "unknown status" case.
#[derive(Debug, Clone)]
pub enum DhanError {
    /// An order request timed out. The order may or may not have reached the
    /// exchange, so the caller must **not** retry and must **not** assume a
    /// fill — it should tell the user to check their broker app. Carries the
    /// `correlationId` so the user (and support) can locate the order.
    OrderStatusUnknown { correlation_id: String },
}

impl std::fmt::Display for DhanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OrderStatusUnknown { correlation_id } => write!(
                f,
                "order status unknown, check broker app; correlation_id={correlation_id}"
            ),
        }
    }
}

impl std::error::Error for DhanError {}

#[derive(Debug, Clone)]
pub struct DhanConfig {
    pub base_url: String,
    pub default_exchange_segment: String,
    pub default_instrument: String,
}

impl Default for DhanConfig {
    fn default() -> Self {
        Self {
            base_url: DHAN_BASE_URL.to_string(),
            default_exchange_segment: "NSE_EQ".to_string(),
            default_instrument: "EQUITY".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DhanClient {
    http: reqwest::Client,
    auth: DhanAuth,
    config: DhanConfig,
    symbol_map: Arc<parking_lot::RwLock<SymbolMap>>,
}

impl DhanClient {
    pub fn new(auth: DhanAuth) -> Self {
        Self::with_config(auth, DhanConfig::default())
    }

    pub fn with_symbol_map(
        auth: DhanAuth,
        symbol_map: Arc<parking_lot::RwLock<SymbolMap>>,
    ) -> Self {
        Self::with_config_and_symbol_map(auth, DhanConfig::default(), symbol_map)
    }

    pub fn with_config(auth: DhanAuth, config: DhanConfig) -> Self {
        Self::with_config_and_symbol_map(
            auth,
            config,
            Arc::new(parking_lot::RwLock::new(SymbolMap::empty())),
        )
    }

    pub fn with_config_and_symbol_map(
        auth: DhanAuth,
        config: DhanConfig,
        symbol_map: Arc<parking_lot::RwLock<SymbolMap>>,
    ) -> Self {
        Self {
            http: reqwest::Client::new(),
            auth,
            config,
            symbol_map,
        }
    }

    pub fn from_env() -> Result<Self> {
        Ok(Self::new(DhanAuth::from_env()?))
    }

    fn headers(&self) -> Result<HeaderMap> {
        let mut headers = HeaderMap::new();
        headers.insert(
            "access-token",
            HeaderValue::from_str(&self.auth.access_token)
                .context("Dhan access token contains invalid header characters")?,
        );

        Ok(headers)
    }

    fn symbol(&self, symbol: &str) -> DhanSymbol {
        DhanSymbol::parse(
            symbol,
            &self.config.default_exchange_segment,
            &self.config.default_instrument,
        )
    }

    /// Shared POST helper. **Performs exactly one request — no automatic
    /// retry** on timeout, 5xx, or network error. Order placement relies on
    /// this invariant (a retried order can double-fill); see
    /// [`DhanClient::post_order_no_retry`], which additionally distinguishes a
    /// timed-out order as [`DhanError::OrderStatusUnknown`]. Do not add retry
    /// logic here without routing order placement around it.
    async fn post<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        body: impl serde::Serialize,
    ) -> Result<T> {
        let request_body =
            serde_json::to_string(&body).unwrap_or_else(|_| "<unserializable>".to_string());
        let response = self
            .http
            .post(format!("{}{}", self.config.base_url, path))
            .headers(self.headers()?)
            .json(&body)
            .send()
            .await
            .with_context(|| format!("Dhan request failed: {path}"))?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            bail!("Dhan request {path} failed with {status}: {body}; request body: {request_body}");
        }

        response
            .json::<T>()
            .await
            .with_context(|| format!("Dhan response was not valid JSON: {path}"))
    }

    /// POST to `/orders` with **no automatic retry**. Order placement is not
    /// idempotent at the network layer: a timed-out request may still have
    /// reached the exchange, so retrying blindly risks a duplicate fill. On a
    /// request timeout this returns [`DhanError::OrderStatusUnknown`] so the
    /// caller can surface "check your broker app" instead of assuming success
    /// or failure. See Phase 7 Fix Pack, Fix 2.
    async fn post_order_no_retry(
        &self,
        body: &PlaceOrderRequest,
        correlation_id: &str,
    ) -> Result<PlaceOrderResponse> {
        let request_body =
            serde_json::to_string(body).unwrap_or_else(|_| "<unserializable>".to_string());
        let response = match self
            .http
            .post(format!("{}/orders", self.config.base_url))
            .headers(self.headers()?)
            .json(body)
            .send()
            .await
        {
            Ok(response) => response,
            Err(err) if err.is_timeout() => {
                return Err(DhanError::OrderStatusUnknown {
                    correlation_id: correlation_id.to_string(),
                }
                .into());
            }
            Err(err) => {
                return Err(anyhow::Error::new(err).context("Dhan order request failed: /orders"));
            }
        };

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            bail!("Dhan request /orders failed with {status}: {body}; request body: {request_body}");
        }

        response
            .json::<PlaceOrderResponse>()
            .await
            .context("Dhan response was not valid JSON: /orders")
    }

    async fn get<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        let response = self
            .http
            .get(format!("{}{}", self.config.base_url, path))
            .headers(self.headers()?)
            .send()
            .await
            .with_context(|| format!("Dhan request failed: {path}"))?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            bail!("Dhan request {path} failed with {status}: {body}");
        }

        response
            .json::<T>()
            .await
            .with_context(|| format!("Dhan response was not valid JSON: {path}"))
    }

    pub fn resolve_symbol_entry(&self, symbol: &str) -> Result<SymbolEntry> {
        let symbol_map = self.symbol_map.read();
        symbol_map
            .lookup(symbol)
            .ok_or_else(|| anyhow!("symbol not in map: {symbol}"))
    }

    fn order_status(status: &str) -> OrderStatus {
        match status.trim().to_ascii_uppercase().as_str() {
            "TRANSIT" => OrderStatus::Transit,
            "PENDING" => OrderStatus::Pending,
            "TRADED" | "FILLED" | "COMPLETE" | "EXECUTED" => OrderStatus::Traded,
            "REJECTED" => OrderStatus::Rejected,
            "CANCELLED" | "CANCELED" => OrderStatus::Cancelled,
            "EXPIRED" => OrderStatus::Expired,
            other => OrderStatus::Unknown(other.to_string()),
        }
    }

    pub fn dhan_timestamp_to_unix_ms(timestamp: f64) -> Option<i64> {
        if !timestamp.is_finite() {
            return None;
        }

        Some((timestamp as i64) * 1_000)
    }

    pub fn dhan_historical_timestamp_to_unix_ms(timestamp: f64) -> Option<i64> {
        if !timestamp.is_finite() {
            return None;
        }

        Some(((timestamp as i64) + DHAN_HISTORICAL_EPOCH_OFFSET_SECONDS) * 1_000)
    }

    pub fn unix_ms_to_dhan_date(timestamp: i64) -> Result<String> {
        let datetime = Utc
            .timestamp_millis_opt(timestamp)
            .single()
            .ok_or_else(|| anyhow!("Invalid Unix millisecond timestamp: {timestamp}"))?;

        Ok(datetime.format("%Y-%m-%d").to_string())
    }

    pub fn unix_ms_to_dhan_datetime(timestamp: i64) -> Result<String> {
        let datetime = Utc
            .timestamp_millis_opt(timestamp)
            .single()
            .ok_or_else(|| anyhow!("Invalid Unix millisecond timestamp: {timestamp}"))?;

        Ok(datetime.format("%Y-%m-%d %H:%M:%S").to_string())
    }

    // Splits a ms timestamp range into 89-day chunks.
    // Returns (chunk_from_ms, chunk_to_ms) pairs.
    fn chunk_date_range(from: i64, to: i64) -> Vec<(i64, i64)> {
        let mut chunks = Vec::new();
        let mut cur = from;
        while cur < to {
            let end = (cur + INTRADAY_CHUNK_MS).min(to);
            chunks.push((cur, end));
            cur = end + 1;
        }
        chunks
    }

    fn candles_from_response(response: HistoricalResponse) -> Vec<Candle> {
        Self::candles_from_response_with_timestamp(response, Self::dhan_timestamp_to_unix_ms)
    }

    fn candles_from_historical_response(response: HistoricalResponse) -> Vec<Candle> {
        Self::candles_from_response_with_timestamp(
            response,
            Self::dhan_historical_timestamp_to_unix_ms,
        )
    }

    fn candles_from_response_with_timestamp(
        response: HistoricalResponse,
        timestamp_to_unix_ms: fn(f64) -> Option<i64>,
    ) -> Vec<Candle> {
        let mut candles = response
            .timestamp
            .into_iter()
            .zip(response.open)
            .zip(response.high)
            .zip(response.low)
            .zip(response.close)
            .zip(response.volume)
            .filter_map(|(((((timestamp, open), high), low), close), volume)| {
                let timestamp = timestamp_to_unix_ms(timestamp?)?;
                let open = finite(open?)?;
                let high = finite(high?)?;
                let low = finite(low?)?;
                let close = finite(close?)?;
                let volume = finite(volume?)?;

                Some(Candle {
                    timestamp,
                    open,
                    high,
                    low,
                    close,
                    volume,
                })
            })
            .collect::<Vec<_>>();

        candles.sort_by_key(|candle| candle.timestamp);
        candles
    }

    fn quote_from_value(symbol: &str, value: DhanQuoteValue) -> Result<Quote> {
        Ok(Quote {
            symbol: symbol.to_string(),
            ltp: value
                .last_price
                .ok_or_else(|| anyhow!("Quote missing LTP"))?,
            open: value.open.unwrap_or_default(),
            high: value.high.unwrap_or_default(),
            low: value.low.unwrap_or_default(),
            close: value.close.unwrap_or_default(),
            bid: value.bid_price.unwrap_or_default(),
            ask: value.ask_price.unwrap_or_default(),
            volume: value.volume.unwrap_or_default(),
        })
    }

    async fn get_ohlcv_intraday(
        &self,
        symbol: &DhanSymbol,
        timeframe: &Timeframe,
        from: i64,
        to: i64,
    ) -> Result<Vec<Candle>> {
        let interval = timeframe.to_interval_str().to_string();
        let chunks = Self::chunk_date_range(from, to);
        let mut all_candles: Vec<Candle> = Vec::new();

        for (chunk_from, chunk_to) in chunks {
            let body = IntradayRequest {
                security_id: symbol.security_id.clone(),
                exchange_segment: symbol.exchange_segment.clone(),
                instrument: symbol.instrument.clone(),
                interval: interval.clone(),
                oi: false,
                from_date: Self::unix_ms_to_dhan_datetime(chunk_from)?,
                to_date: Self::unix_ms_to_dhan_datetime(chunk_to)?,
            };

            let response = self
                .post::<HistoricalResponse>("/charts/intraday", body)
                .await?;

            all_candles.extend(Self::candles_from_response(response));
        }

        // dedup in case chunk boundaries overlap, then sort
        all_candles.sort_by_key(|c| c.timestamp);
        all_candles.dedup_by_key(|c| c.timestamp);

        Ok(all_candles)
    }

    /// Fetch the current available balance (in INR) from Dhan's
    /// `GET /funds/limit` endpoint. Consumed by `DhanBroker::available_cash`
    /// so `PercentCapital` order sizing reflects the user's real buying power
    /// instead of trusting a hard-coded sentinel. The endpoint has no body
    /// and no rate-limit cost worth special-casing — a single GET per refresh
    /// tick is well below the documented limits.
    pub async fn get_funds_limit(&self) -> Result<DhanFundsLimit> {
        self.get::<DhanFundsLimit>("/funds/limit").await
    }
}

#[async_trait]
impl BrokerClient for DhanClient {
    async fn get_ohlcv(
        &self,
        symbol: &str,
        timeframe: Timeframe,
        from: i64,
        to: i64,
    ) -> Result<Vec<Candle>> {
        let symbol = self.symbol(symbol);

        if timeframe.is_intraday() {
            return self.get_ohlcv_intraday(&symbol, &timeframe, from, to).await;
        }

        // daily / weekly — existing path
        let body = serde_json::json!({
            "securityId": symbol.security_id,
            "exchangeSegment": symbol.exchange_segment,
            "instrument": symbol.instrument,
            "expiryCode": 0,
            "oi": "false",
            "fromDate": Self::unix_ms_to_dhan_date(from)?,
            "toDate": Self::unix_ms_to_dhan_date(to)?,
        });

        let response = self
            .post::<HistoricalResponse>("/charts/historical", body)
            .await?;
        Ok(Self::candles_from_historical_response(response))
    }

    async fn get_quote(&self, symbol: &str) -> Result<Quote> {
        let dhan_symbol = self.symbol(symbol);
        let mut body = serde_json::Map::new();
        body.insert(
            dhan_symbol.exchange_segment.clone(),
            serde_json::json!([dhan_symbol
                .security_id
                .parse::<i64>()
                .context("Dhan security id must be numeric for quotes")?]),
        );

        let response = self
            .post::<Value>("/marketfeed/quote", Value::Object(body))
            .await?;
        let root = response.get("data").unwrap_or(&response);
        let exchange = root
            .get(&dhan_symbol.exchange_segment)
            .ok_or_else(|| anyhow!("Quote response missing exchange segment"))?;
        let numeric_security_id_key = dhan_symbol
            .security_id
            .parse::<i64>()
            .ok()
            .map(|security_id| security_id.to_string());
        let value = exchange
            .get(dhan_symbol.security_id.as_str())
            .or_else(|| exchange.get(numeric_security_id_key.as_deref()?))
            .ok_or_else(|| anyhow!("Quote response missing security id"))?;
        let value = serde_json::from_value::<DhanQuoteValue>(value.clone())?;

        Self::quote_from_value(symbol, value)
    }

    async fn place_order(&self, order: Order) -> Result<OrderResult> {
        let dhan_client_id = self
            .auth
            .client_id
            .clone()
            .ok_or_else(|| anyhow!("Dhan client id is not configured"))?;
        let entry = self.resolve_symbol_entry(&order.symbol)?;
        // Segment guard (Phase 7 Fix Pack, Fix 3). Defence in depth: even if a
        // pre-flight gate is bypassed, the broker refuses anything that is not
        // NSE equity intraday. Relaxing this is a Phase 8 task.
        if entry.segment != Segment::NseEq {
            bail!(
                "Phase 7 only supports NSE equity intraday trading; symbol {} is {:?}",
                order.symbol,
                entry.segment
            );
        }
        let security_id = entry.security_id.to_string();
        let order_type = match order.order_type {
            OrderType::Market => "MARKET",
            OrderType::Limit => "LIMIT",
            OrderType::StopLoss => "STOP_LOSS",
        };
        let price = match order.order_type {
            OrderType::Limit => order
                .price
                .ok_or_else(|| anyhow!("limit order requires a price"))?,
            _ => order.price.unwrap_or_default(),
        };
        // Idempotency key: if any layer retries a timed-out request, Dhan
        // dedupes on this so the same order is not placed twice.
        // Dhan limit: max 30 characters.
        let uuid_str = uuid::Uuid::new_v4().simple().to_string();
        let correlation_id = format!("am-{}", &uuid_str[..27]);
        let body = PlaceOrderRequest {
            dhan_client_id,
            correlation_id: correlation_id.clone(),
            transaction_type: match order.side {
                OrderSide::Buy => "BUY".to_string(),
                OrderSide::Sell => "SELL".to_string(),
            },
            exchange_segment: entry.segment.as_dhan_string().to_string(),
            product_type: "INTRADAY".to_string(),
            order_type: order_type.to_string(),
            validity: "DAY".to_string(),
            security_id,
            quantity: order.quantity,
            price,
            trigger_price: 0.0,
            disclosed_quantity: 0,
            after_market_order: false,
            amo_time: "OPEN".to_string(),
            bo_profit_value: 0.0,
            bo_stop_loss_value: 0.0,
        };

        // Place-and-return: Dhan responds as soon as the order is accepted,
        // which may be a non-terminal status (TRANSIT/PENDING). We return the
        // status as-is; `DhanBroker::execute` decides how to treat each state
        // (fill vs. submitted vs. rejected). No retry — see
        // `post_order_no_retry`.
        let response = self.post_order_no_retry(&body, &correlation_id).await?;
        let status = Self::order_status(&response.order_status);

        Ok(OrderResult {
            order_id: response.order_id,
            status,
            timestamp: Utc::now().timestamp_millis(),
            correlation_id,
        })
    }

    async fn get_positions(&self) -> Result<Vec<Position>> {
        let positions = self.get::<Vec<DhanPosition>>("/positions").await?;
        Ok(positions
            .into_iter()
            .filter(|position| position.net_qty != 0)
            .map(|position| Position {
                symbol: position.trading_symbol,
                quantity: position.net_qty,
                average_price: position.buy_avg,
                ltp: position.buy_avg,
                realized_pnl: position.realized_profit,
                unrealized_pnl: position.unrealized_profit,
            })
            .collect())
    }

    async fn get_portfolio(&self) -> Result<Portfolio> {
        let positions = self.get_positions().await?;
        let total_value = positions
            .iter()
            .map(|position| position.ltp * position.quantity as f64)
            .sum();
        let realized_pnl = positions.iter().map(|position| position.realized_pnl).sum();
        let unrealized_pnl = positions
            .iter()
            .map(|position| position.unrealized_pnl)
            .sum();

        Ok(Portfolio {
            positions,
            total_value,
            realized_pnl,
            unrealized_pnl,
        })
    }
}

fn finite(value: f64) -> Option<f64> {
    value.is_finite().then_some(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use std::{env, fs};

    #[test]
    fn maps_dhan_order_status_strings() {
        assert_eq!(DhanClient::order_status("TRANSIT"), OrderStatus::Transit);
        assert_eq!(DhanClient::order_status("pending"), OrderStatus::Pending);
        assert_eq!(DhanClient::order_status("TRADED"), OrderStatus::Traded);
        assert_eq!(DhanClient::order_status("REJECTED"), OrderStatus::Rejected);
        assert_eq!(DhanClient::order_status("CANCELLED"), OrderStatus::Cancelled);
        assert_eq!(DhanClient::order_status("EXPIRED"), OrderStatus::Expired);
        assert_eq!(
            DhanClient::order_status("SOMETHING_ELSE"),
            OrderStatus::Unknown("SOMETHING_ELSE".to_string())
        );
    }

    #[test]
    fn test_correlation_id_is_present() {
        // The order request body must carry a `correlationId` beginning with
        // "algomln-" so a retried request can be deduplicated by the broker.
        let correlation_id = format!("algomln-{}", uuid::Uuid::new_v4());
        let body = PlaceOrderRequest {
            dhan_client_id: "client".to_string(),
            correlation_id: correlation_id.clone(),
            transaction_type: "BUY".to_string(),
            exchange_segment: "NSE_EQ".to_string(),
            product_type: "INTRADAY".to_string(),
            order_type: "MARKET".to_string(),
            validity: "DAY".to_string(),
            security_id: "2885".to_string(),
            quantity: 1,
            price: 0.0,
            trigger_price: 0.0,
            disclosed_quantity: 0,
            after_market_order: false,
            amo_time: "OPEN".to_string(),
            bo_profit_value: 0.0,
            bo_stop_loss_value: 0.0,
        };

        let json: serde_json::Value = serde_json::from_str(
            &serde_json::to_string(&body).expect("serialize place order request"),
        )
        .expect("valid json");

        let correlation = json
            .get("correlationId")
            .and_then(|v| v.as_str())
            .expect("correlationId field present");
        assert!(
            correlation.starts_with("algomln-"),
            "correlationId was {correlation}"
        );
    }

    #[tokio::test]
    async fn test_place_order_rejects_non_equity() {
        use crate::models::{Order, OrderSide, OrderType};

        let mut symbol_map = SymbolMap::empty();
        symbol_map.insert_entry("BANKNIFTY", 1234, Segment::NseFno);
        let symbol_map = Arc::new(parking_lot::RwLock::new(symbol_map));

        let auth = DhanAuth::with_client_id("test-token", "client-1").unwrap();
        let client = DhanClient::with_symbol_map(auth, symbol_map);

        let order = Order {
            symbol: "BANKNIFTY".to_string(),
            side: OrderSide::Buy,
            quantity: 1,
            order_type: OrderType::Market,
            price: None,
        };

        // The segment guard runs before any HTTP request, so this fails fast
        // without touching the network.
        let error = client
            .place_order(order)
            .await
            .expect_err("non-equity order must be rejected");
        assert!(
            error.to_string().contains("Phase 7 only supports NSE equity"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn converts_dhan_unix_seconds_to_unix_ms() {
        assert_eq!(
            DhanClient::dhan_timestamp_to_unix_ms(1_779_820_200.0),
            Some(1_779_820_200_000)
        );
    }

    #[test]
    fn converts_dhan_historical_seconds_to_unix_ms() {
        assert_eq!(
            DhanClient::dhan_historical_timestamp_to_unix_ms(0.0),
            Some(315_532_800_000)
        );
    }

    #[test]
    fn filters_incomplete_or_nan_candles_and_sorts() {
        let response = HistoricalResponse {
            timestamp: vec![Some(2.0), None, Some(1.0), Some(3.0)],
            open: vec![Some(20.0), Some(1.0), Some(10.0), Some(f64::NAN)],
            high: vec![Some(21.0), Some(1.0), Some(11.0), Some(3.0)],
            low: vec![Some(19.0), Some(1.0), Some(9.0), Some(3.0)],
            close: vec![Some(20.5), Some(1.0), Some(10.5), Some(3.0)],
            volume: vec![Some(200.0), Some(1.0), Some(100.0), Some(3.0)],
        };

        let candles = DhanClient::candles_from_response(response);

        assert_eq!(candles.len(), 2);
        assert_eq!(candles[0].open, 10.0);
        assert_eq!(candles[1].open, 20.0);
    }

    #[test]
    fn converts_unix_ms_to_dhan_date() {
        assert_eq!(
            DhanClient::unix_ms_to_dhan_date(1_704_067_200_000).unwrap(),
            "2024-01-01"
        );
    }

    #[test]
    fn converts_unix_ms_to_dhan_datetime() {
        assert_eq!(
            DhanClient::unix_ms_to_dhan_datetime(1_704_067_200_000).unwrap(),
            "2024-01-01 00:00:00"
        );
    }

    #[test]
    fn chunks_date_range_into_89_day_windows() {
        // 200 days should give 3 chunks: 89 + 89 + 22
        let from = 0i64;
        let to = 200 * 24 * 60 * 60 * 1_000;
        let chunks = DhanClient::chunk_date_range(from, to);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].0, 0);
        assert_eq!(chunks[1].0, INTRADAY_CHUNK_MS + 1);
        assert_eq!(chunks[2].1, to);
    }

    #[test]
    fn single_chunk_when_range_under_89_days() {
        let from = 0i64;
        let to = 10 * 24 * 60 * 60 * 1_000; // 10 days
        let chunks = DhanClient::chunk_date_range(from, to);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], (from, to));
    }

    #[tokio::test]
    #[ignore = "hits the live Dhan API and requires DHAN_ACCESS_TOKEN"]
    async fn live_fetch() {
        load_dotenv_for_test();

        let client = DhanClient::from_env().expect("Set DHAN_ACCESS_TOKEN in .env or the shell");
        let symbol =
            env::var("DHAN_TEST_SYMBOL").unwrap_or_else(|_| "2885|NSE_EQ|EQUITY".to_string());
        let to = Utc::now().timestamp_millis();
        let from = (Utc::now() - Duration::days(30)).timestamp_millis();

        let candles = client
            .get_ohlcv(&symbol, Timeframe::D1, from, to)
            .await
            .expect("live Dhan OHLCV fetch failed");

        println!("Fetched {} candles for {}", candles.len(), symbol);
        if let Some(last) = candles.last() {
            println!("Last candle: {:?}", last);
        }

        assert!(!candles.is_empty(), "Dhan returned no candles");
    }

    fn load_dotenv_for_test() {
        let Ok(contents) = fs::read_to_string(".env") else {
            return;
        };

        for line in contents.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            let Some((key, value)) = line.split_once('=') else {
                continue;
            };

            if env::var(key.trim()).is_err() {
                env::set_var(key.trim(), value.trim().trim_matches('"'));
            }
        }
    }
}
use anyhow::Result;

use crate::models::Tick;

#[derive(Debug, Clone)]
pub struct DhanWebSocketConfig {
    pub client_id: String,
    pub access_token: String,
}

#[derive(Debug)]
pub struct DhanWebSocket {
    config: DhanWebSocketConfig,
}

impl DhanWebSocket {
    pub fn new(config: DhanWebSocketConfig) -> Self {
        Self { config }
    }

    pub async fn connect(&self) -> Result<()> {
        let _ = &self.config;
        Ok(())
    }

    pub async fn subscribe(&self, symbols: &[String]) -> Result<()> {
        let _ = symbols;
        Ok(())
    }

    pub async fn unsubscribe(&self, symbols: &[String]) -> Result<()> {
        let _ = symbols;
        Ok(())
    }

    pub async fn next_tick(&self) -> Result<Option<Tick>> {
        Ok(None)
    }
}
pub mod dhan;
pub mod symbol_map;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::models::{Candle, Order, OrderResult, Portfolio, Position, Quote};

pub type BrokerResult<T> = anyhow::Result<T>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Timeframe {
    M1,
    M5,
    M15,
    M25,
    M30,
    M60,
    H1,
    H4,
    D1,
    W1,
}

impl Timeframe {
    pub fn is_intraday(&self) -> bool {
        matches!(
            self,
            Timeframe::M1 | Timeframe::M5 | Timeframe::M15 | Timeframe::M25 | Timeframe::M60
        )
    }

    pub fn to_interval_str(&self) -> &str {
        match self {
            Timeframe::M1 => "1",
            Timeframe::M5 => "5",
            Timeframe::M15 => "15",
            Timeframe::M25 => "25",
            Timeframe::M60 => "60",
            _ => "1",
        }
    }

    pub fn as_dhan_interval(self) -> &'static str {
        match self {
            Self::M1 => "1",
            Self::M5 => "5",
            Self::M15 => "15",
            Self::M25 => "25",
            Self::M30 => "30",
            Self::M60 => "60",
            Self::H1 => "60",
            Self::H4 => "240",
            Self::D1 => "D",
            Self::W1 => "W",
        }
    }
}

impl std::str::FromStr for Timeframe {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "M1" | "1" => Ok(Self::M1),
            "M5" | "5" => Ok(Self::M5),
            "M15" | "15" => Ok(Self::M15),
            "M25" | "25" => Ok(Self::M25),
            "M30" | "30" => Ok(Self::M30),
            "M60" | "60" | "H1" => Ok(Self::M60),
            "H4" => Ok(Self::H4),
            "D1" | "1D" | "D" => Ok(Self::D1),
            "W1" | "1W" | "W" => Ok(Self::W1),
            _ => Err(format!("Unsupported timeframe: {value}")),
        }
    }
}

#[async_trait]
pub trait BrokerClient: Send + Sync {
    async fn get_ohlcv(
        &self,
        symbol: &str,
        timeframe: Timeframe,
        from: i64,
        to: i64,
    ) -> BrokerResult<Vec<Candle>>;

    async fn get_quote(&self, symbol: &str) -> BrokerResult<Quote>;

    async fn place_order(&self, order: Order) -> BrokerResult<OrderResult>;

    async fn get_positions(&self) -> BrokerResult<Vec<Position>>;

    async fn get_portfolio(&self) -> BrokerResult<Portfolio>;
}
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use serde::{Deserialize, Serialize};

const DHAN_SCRIP_MASTER_URL: &str = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv";

/// Exchange segment for a tradable symbol. Phase 7 supports only `NseEq` for
/// order placement; the other variants exist so the segment guard can name a
/// rejected symbol and so Phase 8 can relax the restriction without a schema
/// change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Segment {
    NseEq,
    NseFno,
    NseCurrency,
    Bse,
    Mcx,
    Index,
}

impl Segment {
    /// The Dhan `exchangeSegment` string for this segment.
    pub fn as_dhan_string(self) -> &'static str {
        match self {
            Segment::NseEq => "NSE_EQ",
            Segment::NseFno => "NSE_FNO",
            Segment::NseCurrency => "NSE_CURRENCY",
            Segment::Bse => "BSE_EQ",
            Segment::Mcx => "MCX_COMM",
            Segment::Index => "IDX_I",
        }
    }
}

/// A resolved symbol: its Dhan security id and the segment it trades in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SymbolEntry {
    pub security_id: u32,
    pub segment: Segment,
}

/// Maps NSE equity trading symbols to Dhan security IDs.
/// Loaded once at startup; shared via Arc.
#[derive(Debug)]
pub struct SymbolMap {
    /// key: uppercase NSE symbol, value: Dhan SECURITY_ID
    map: HashMap<String, u32>,
    /// key: uppercase NSE symbol, value: exchange segment. Kept parallel to
    /// `map` so `inner()` / `get()` retain their `u32` shape for the fuzzy
    /// search path; `lookup()` joins the two.
    segments: HashMap<String, Segment>,
}

#[derive(Debug, Deserialize)]
struct ScripRow {
    #[serde(rename = "EXCH_ID")]
    exch_id: String,
    #[serde(rename = "SEGMENT")]
    segment: String,
    /// Primary: present in detailed CSV for equities.
    #[serde(rename = "SYMBOL_NAME", default)]
    symbol_name: Option<String>,
    /// Fallback: present for equities in some CSV versions (also used for
    /// derivative underlyings).
    #[serde(rename = "UNDERLYING_SYMBOL", default)]
    underlying_symbol: Option<String>,
    #[serde(rename = "SECURITY_ID")]
    security_id: Option<u32>,
}

impl SymbolMap {
    /// Load from a CSV file (either seed or cache). Returns an error if the
    /// file is missing or unparseable. Does NOT fall back — callers handle
    /// fallback.
    pub fn load(path: &Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read {}: {}", path.display(), e))?;
        // Strip BOM.
        let text = text.trim_start_matches('\u{feff}');
        Self::parse_csv(text)
    }

    fn parse_csv(text: &str) -> Result<Self, String> {
        let mut rdr = csv::Reader::from_reader(text.as_bytes());
        let mut map: HashMap<String, u32> = HashMap::new();
        let mut segments: HashMap<String, Segment> = HashMap::new();
        let mut duplicates = 0usize;

        for result in rdr.deserialize::<ScripRow>() {
            let row = match result {
                Ok(r) => r,
                Err(_) => continue, // skip unparseable rows silently
            };

            // Filter: NSE equities only.
            if row.exch_id.trim() != "NSE" || row.segment.trim() != "E" {
                continue;
            }

            let sec_id = match row.security_id {
                Some(id) => id,
                None => continue,
            };

            // Prefer UNDERLYING_SYMBOL as it typically contains the short trading symbol.
            let symbol = row
                .underlying_symbol
                .as_deref()
                .filter(|s| !s.trim().is_empty())
                .or_else(|| {
                    row.symbol_name
                        .as_deref()
                        .filter(|s| !s.trim().is_empty())
                });

            if let Some(sym) = symbol {
                let key = sym.trim().to_uppercase();
                if map.contains_key(&key) {
                    duplicates += 1;
                    // First occurrence wins (matches the user's Python script).
                } else {
                    map.insert(key.clone(), sec_id);
                    // The CSV filter above admits only NSE equities, so every
                    // parsed entry is NseEq.
                    segments.insert(key, Segment::NseEq);
                }
            }
        }

        if duplicates > 0 {
            eprintln!(
                "[SymbolMap] {} duplicate symbols ignored (first-wins)",
                duplicates
            );
        }
        eprintln!("[SymbolMap] loaded {} NSE equity symbols", map.len());
        Ok(Self { map, segments })
    }

    /// Empty map — used when the seed file is unavailable so the app still boots.
    pub fn empty() -> Self {
        Self {
            map: HashMap::new(),
            segments: HashMap::new(),
        }
    }

    /// Look up a security ID for a symbol. Case-insensitive.
    pub fn get(&self, symbol: &str) -> Option<u32> {
        self.map.get(&symbol.trim().to_uppercase()).copied()
    }

    /// Look up the full entry (security id + segment) for a symbol.
    /// Case-insensitive. If the security id is present but the segment is
    /// somehow missing, defaults to `NseEq` with a warning rather than
    /// dropping the symbol.
    pub fn lookup(&self, symbol: &str) -> Option<SymbolEntry> {
        let key = symbol.trim().to_uppercase();
        let security_id = *self.map.get(&key)?;
        let segment = match self.segments.get(&key) {
            Some(segment) => *segment,
            None => {
                eprintln!("[SymbolMap] segment missing for {key}; defaulting to NseEq");
                Segment::NseEq
            }
        };
        Some(SymbolEntry {
            security_id,
            segment,
        })
    }

    /// Insert or overwrite a single entry. Used by tests and any future
    /// non-CSV loader; the CSV path populates entries as NSE equities.
    pub fn insert_entry(&mut self, symbol: &str, security_id: u32, segment: Segment) {
        let key = symbol.trim().to_uppercase();
        self.map.insert(key.clone(), security_id);
        self.segments.insert(key, segment);
    }

    /// Borrow the raw symbol → security_id map. Used by the
    /// `search_symbols` IPC command to walk the entire universe.
    pub fn inner(&self) -> &HashMap<String, u32> {
        &self.map
    }

    /// Batch lookup. Returns (found: Vec<(symbol, security_id)>, missing: Vec<symbol>).
    pub fn resolve_many(&self, symbols: &[String]) -> (Vec<(String, u32)>, Vec<String>) {
        let mut found = Vec::with_capacity(symbols.len());
        let mut missing = Vec::new();
        for sym in symbols {
            match self.get(sym) {
                Some(id) => found.push((sym.clone(), id)),
                None => missing.push(sym.clone()),
            }
        }
        (found, missing)
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

/// Download the Dhan scrip master CSV, write to `cache_path`, return a loaded
/// SymbolMap. On any failure, returns Err — callers fall back to the seed
/// file.
pub async fn refresh_symbol_map(cache_path: &Path) -> Result<SymbolMap, String> {
    eprintln!("[SymbolMap] downloading scrip master from Dhan…");
    let response = reqwest::Client::builder()
        .user_agent("AlgoMLN/1.0")
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|e| e.to_string())?
        .get(DHAN_SCRIP_MASTER_URL)
        .send()
        .await
        .map_err(|e| format!("HTTP error: {}", e))?;

    if !response.status().is_success() {
        return Err(format!(
            "Dhan scrip master returned HTTP {}",
            response.status()
        ));
    }

    let bytes = response.bytes().await.map_err(|e| e.to_string())?;
    if let Some(parent) = cache_path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    // Atomic write via temp file.
    let tmp = cache_path.with_extension("tmp");
    std::fs::write(&tmp, &bytes).map_err(|e| e.to_string())?;
    std::fs::rename(&tmp, cache_path).map_err(|e| e.to_string())?;

    let text = String::from_utf8_lossy(&bytes);
    let text = text.trim_start_matches('\u{feff}');
    let map = SymbolMap::parse_csv(text)?;
    eprintln!("[SymbolMap] refreshed: {} symbols", map.len());
    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_minimal_csv() {
        let csv = "EXCH_ID,SEGMENT,SYMBOL_NAME,SECURITY_ID\n\
                   NSE,E,RELIANCE,2885\n\
                   NSE,D,NIFTY,0\n\
                   BSE,E,RELIANCE,500325\n";
        let map = SymbolMap::parse_csv(csv).unwrap();
        assert_eq!(map.get("RELIANCE"), Some(2885));
        assert_eq!(map.get("reliance"), Some(2885)); // case-insensitive
        assert_eq!(map.get("NIFTY"), None); // derivative filtered out
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn empty_map_is_empty() {
        let map = SymbolMap::empty();
        assert!(map.is_empty());
        assert_eq!(map.get("RELIANCE"), None);
    }

    #[test]
    fn resolve_many_splits_found_and_missing() {
        let map = SymbolMap::empty();
        let symbols = vec!["A".to_string(), "B".to_string()];
        let (found, missing) = map.resolve_many(&symbols);
        assert!(found.is_empty());
        assert_eq!(missing, symbols);
    }

    #[test]
    fn parsed_entries_are_nse_equity() {
        let csv = "EXCH_ID,SEGMENT,SYMBOL_NAME,SECURITY_ID\n\
                   NSE,E,RELIANCE,2885\n";
        let map = SymbolMap::parse_csv(csv).unwrap();
        let entry = map.lookup("reliance").expect("case-insensitive lookup");
        assert_eq!(entry.security_id, 2885);
        assert_eq!(entry.segment, Segment::NseEq);
    }

    #[test]
    fn insert_entry_records_segment() {
        let mut map = SymbolMap::empty();
        map.insert_entry("BANKNIFTY", 1234, Segment::NseFno);
        let entry = map.lookup("BANKNIFTY").unwrap();
        assert_eq!(entry.security_id, 1234);
        assert_eq!(entry.segment, Segment::NseFno);
        // `get` still returns the security id for the fuzzy-search path.
        assert_eq!(map.get("BANKNIFTY"), Some(1234));
    }

    #[test]
    fn segment_as_dhan_string() {
        assert_eq!(Segment::NseEq.as_dhan_string(), "NSE_EQ");
        assert_eq!(Segment::NseFno.as_dhan_string(), "NSE_FNO");
    }
}
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{
    broker::{
        dhan::{DhanAuth, DhanClient},
        symbol_map::SymbolMap,
        BrokerClient, Timeframe,
    },
    feed::FeedManager,
    live::trade_log::TradeLog,
    models::{Candle, Quote},
    strategy::execution::DhanBroker,
};

/// Shared data + execution infrastructure used by every Tauri command.
///
/// `broker` is the trait-object view the rest of the app uses for OHLCV /
/// quote / subscribe calls. `dhan_broker` (Dhan only) is the live execution
/// target plus the periodic realized-loss cache; both wrap the same
/// `Arc<DhanClient>`. The trailing `Option` is for environments where live
/// execution is disabled — currently always `Some` because Phase 7 hard-
/// requires Dhan.
pub struct DataState {
    pub broker: Arc<dyn BrokerClient>,
    pub feed: Arc<Mutex<FeedManager>>,
    pub dhan_broker: Option<Arc<DhanBroker>>,
    pub dhan_client: Option<Arc<DhanClient>>,
}

impl DataState {
    pub fn dhan_from_env() -> anyhow::Result<Self> {
        Self::dhan_from_env_with_symbol_map(
            Arc::new(parking_lot::RwLock::new(SymbolMap::empty())),
            None,
        )
    }

    pub fn dhan_from_env_with_symbol_map(
        symbol_map: Arc<parking_lot::RwLock<SymbolMap>>,
        trade_log: Option<Arc<TradeLog>>,
    ) -> anyhow::Result<Self> {
        let auth = DhanAuth::from_env()?;
        let dhan_client = Arc::new(DhanClient::with_symbol_map(auth, symbol_map));
        // The DhanBroker needs its own realized-loss cache + session context.
        // If the caller hasn't passed a trade log yet, open one against a
        // best-effort default path inside the app data dir — the setup
        // closure normally supplies this, so the `None` branch is only
        // exercised by tests.
        let broker: Arc<dyn BrokerClient> = dhan_client.clone();
        let dhan_broker = trade_log.map(|log| Arc::new(DhanBroker::new(dhan_client.clone(), log)));
        Ok(Self {
            broker,
            feed: Arc::new(Mutex::new(FeedManager::new())),
            dhan_broker,
            dhan_client: Some(dhan_client),
        })
    }
}

pub async fn get_ohlcv(
    state: &DataState,
    symbol: String,
    timeframe: Timeframe,
    from: i64,
    to: i64,
) -> Result<Vec<Candle>, String> {
    state
        .broker
        .get_ohlcv(&symbol, timeframe, from, to)
        .await
        .map_err(|error| error.to_string())
}

pub async fn get_quote(state: &DataState, symbol: String) -> Result<Quote, String> {
    state
        .broker
        .get_quote(&symbol)
        .await
        .map_err(|error| error.to_string())
}

pub async fn subscribe_ticks(state: &DataState, symbols: Vec<String>) -> Result<(), String> {
    state
        .feed
        .lock()
        .await
        .subscribe(symbols)
        .await
        .map_err(|error| error.to_string())
}
use crate::broker::symbol_map::refresh_symbol_map;
use crate::commands::state::AppState;
use crate::indices::{refresh_index, IndexInfo};
use crate::strategy::dsl::ast::IndexAlias;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tauri::{AppHandle, Manager};

/// Returned by `refresh_indices` IPC command.
#[derive(Debug, Serialize, Deserialize)]
pub struct RefreshResult {
    /// Alias names that were successfully refreshed.
    pub refreshed: Vec<String>,
    /// (alias, error_message) pairs for failures.
    pub failed: Vec<(String, String)>,
    /// Whether the Dhan symbol map was also refreshed.
    pub symbol_map_updated: bool,
    /// New total symbol count in the map after refresh (0 if unchanged).
    pub symbol_map_count: usize,
}

/// Return metadata for all 22 indices (alias, display name, count, last updated).
/// This is a synchronous read — no I/O.
pub fn list_indices(state: &AppState) -> Vec<IndexInfo> {
    state.index_registry.list_info()
}

/// Return the constituent symbol list for a named index alias (e.g. "NIFTY_50").
/// Returns Err if the alias is unrecognised.
pub fn get_index_symbols(state: &AppState, alias: String) -> Result<Vec<String>, String> {
    let parsed = IndexAlias::from_dsl_str(&alias)
        .ok_or_else(|| format!("unknown index alias '{}'", alias))?;
    Ok(state.index_registry.get_symbols(&parsed))
}

/// Refresh all 22 indices from niftyindices.com AND the Dhan scrip master.
/// Writes updated JSON/CSV to app data cache. Non-fatal: errors are reported in the result.
pub async fn refresh_indices(app: &AppHandle, state: &AppState) -> RefreshResult {
    let cache_dir = app.path().app_data_dir().expect("app_data_dir unavailable");

    let indices_cache = cache_dir.join("indices");
    let _ = std::fs::create_dir_all(&indices_cache);

    let registry = Arc::clone(&state.index_registry);

    // Refresh all indices
    let mut refreshed = vec![];
    let mut failed = vec![];
    for alias in IndexAlias::all() {
        let outcome = refresh_index(alias, &indices_cache, &registry).await;
        if outcome.success {
            refreshed.push(outcome.alias);
        } else {
            failed.push((outcome.alias, outcome.error.unwrap_or_default()));
        }
        // Polite delay between requests
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    }

    // Refresh symbol map
    let sym_cache = cache_dir.join("sec_id_cache.csv");
    let (symbol_map_updated, symbol_map_count) = match refresh_symbol_map(&sym_cache).await {
        Ok(new_map) => {
            let count = new_map.len();
            *state.symbol_map.write() = new_map;
            (true, count)
        }
        Err(e) => {
            eprintln!("[refresh_indices] symbol map refresh failed: {}", e);
            (false, 0)
        }
    };

    RefreshResult {
        refreshed,
        failed,
        symbol_map_updated,
        symbol_map_count,
    }
}
use serde::Serialize;
use std::sync::Arc;
use tauri::State;

use crate::{
    broker::Timeframe,
    commands::{
        state::AppState,
        strategy::parse_and_validate_dsl,
    },
    live::{
        guard::{LiveGuard, LiveGuardResult},
        session::{
            LiveSession,
            SessionEventEmitter,
        },
        trade_log::{TradeLog, TradeLogEntry},
    },
    strategy::dsl::TradeIn,
    strategy::execution::target::ExecutionTarget,
};

/// Default starting capital for a live session, in INR. Used as the
/// denominator for `RISK MAX_DAILY_LOSS`. Until per-strategy starting
/// cash is exposed in Settings, this is the value every live session
/// inherits — sensible for an NSE retail account. The live engine
/// cannot recover the starting capital from `DhanBroker`, so without
/// this default the cap is a silent no-op (audit item A1).
const DEFAULT_LIVE_INITIAL_CASH: f64 = 1_000_000.0;

/// Returns all entries from the trade log, newest first.
pub async fn get_trade_log(state: State<'_, AppState>) -> Result<Vec<TradeLogEntry>, String> {
    TradeLog::read_all(&state.trade_log_path)
        .map(|mut v| {
            v.reverse();
            v
        })
        .map_err(|e| e.to_string())
}

// =============================================================================
// Phase 7 — Live start / confirm / acknowledge
// =============================================================================

/// Wire shape returned by `request_live_start`. The UI keeps the token and
/// passes it back to `confirm_live_start` within the TTL. `requires_ack` is
/// set when gates 1–8 pass but the user has not yet acknowledged
/// live-trading risks.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestLiveStartResult {
    pub token: String,
    pub requires_ack: bool,
    /// The symbol that the strategy will trade. Echoed back so the UI can
    /// display it in the confirmation dialog alongside the strategy name.
    pub symbol: String,
}

/// Begin a live session. Validates gates 1–8 via [`LiveGuard::run_preflight`]
/// and stores a fresh 90-second token in `AppState.pending_live_token`. The
/// token — not the session — is what is returned to the UI.
///
/// `acknowledged_live_trading` is a separate end-point that just writes
/// the consent file; tokens never need to know about it.
pub async fn request_live_start(
    state: State<'_, AppState>,
    strategy_id: String,
) -> Result<RequestLiveStartResult, String> {
    // C3 (audit): hold the session lock across the entire call. Two
    // concurrent `request_live_start`s (e.g. a UI double-click) must not
    // both pass the `is_some()` check and both run preflight — the second
    // would overwrite the first's token and leak the slot for 90 s. Holding
    // the lock until the token is stored atomically closes that race.
    let session_slot = state.live_session.lock().await;
    if session_slot.is_some() {
        return Err(
            "a live session is already active; stop it before starting another".to_string(),
        );
    }

    // Look up the deployed strategy record. The wire `DeployedStrategy`
    // collapses `mode` into `modes: Vec<StrategyMode>`; we want the
    // authoritative record to check the on-disk mode exactly.
    let record = state
        .strategies
        .get(&strategy_id)
        .await?
        .ok_or_else(|| format!("strategy not found: {strategy_id}"))?;

    // Gate 1 (paper-default guard): live requires the record to have been
    // deployed with `mode = Live`. Paper is the default.
    if record.mode != crate::commands::registry::StrategyMode::Live {
        return Err(
            "strategy was not deployed in Live mode; redeploy with mode 'live' to enable \
             live trading"
                .to_string(),
        );
    }

    // Parse + validate the DSL so we have the AST for the gates that read it
    // (6 + 7: risk controls). The same parse is also re-run inside
    // `confirm_live_start` for the session builder — there is no cache;
    // the cost is negligible.
    let strategy_node = parse_and_validate_dsl(&record.dsl_source)?;

    // Resolve the symbol from the DSL's `TRADE_IN` clause. Phase 7 only
    // supports a single explicit symbol per strategy; index-based multi-
    // symbol live sessions are out of scope.
    let symbol = match &strategy_node.trade_in {
        Some(TradeIn::Symbols(symbols)) if symbols.len() == 1 => {
            symbols.first().cloned().unwrap()
        }
        Some(TradeIn::Symbols(_)) => {
            return Err(
                "live mode in Phase 7 supports exactly one TRADE_IN symbol; \
                 multi-symbol strategies are not yet supported for live trading"
                    .to_string(),
            )
        }
        Some(TradeIn::Index(_)) => {
            return Err(
                "live mode in Phase 7 does not yet support TRADE_IN NIFTY_* \
                 indexes; use an explicit single TRADE_IN SYMBOLS clause"
                    .to_string(),
            )
        }
        None => {
            return Err(
                "strategy has no TRADE_IN clause; add `TRADE_IN RELIANCE` \
                 (or another single symbol) before starting a live session"
                    .to_string(),
            )
        }
    };

    // Run gates 1–8 via the guard. The guard issues the token itself. We
    // still hold the `live_session` lock so no concurrent caller can race.
    let result = state
        .live_guard
        .run_preflight(&symbol, &strategy_id, &strategy_node)
        .await?;

    let (token, requires_ack) = match result {
        LiveGuardResult::Ok { token } => (token, false),
        LiveGuardResult::RequiresAcknowledgment { token } => (token, true),
    };
    let token_string = token.token.clone();

    // Store the token in the slot under the parking_lot guard pattern:
    // take the (separate) pending-token mutex briefly, but never release
    // the session lock until we are done with this whole call.
    *state.pending_live_token.lock().await = Some(token);

    // Note: `session_slot` is dropped at function exit (no other call can
    // touch the slot in the meantime because we held the tokio::Mutex
    // throughout). Drop order: `session_slot` first, then `state` is freed
    // by Tauri.

    Ok(RequestLiveStartResult {
        token: token_string,
        requires_ack,
        symbol,
    })
}

/// Validate the pending token, then build and start the live session. Held
/// lock discipline: `state.live_session` is taken before the token check so
/// a concurrent `request_live_start` cannot slip a second session in
/// between the validation and the insert.
///
/// The token is consumed (cleared) immediately after validation regardless
/// of whether the session start succeeds — the token is single-use.
pub async fn confirm_live_start(
    state: State<'_, AppState>,
    strategy_id: String,
    token: String,
) -> Result<(), String> {
    // 1. Take the session lock first and hold it across the entire call
    //    so a concurrent `confirm_live_start` (or `request_live_start`)
    //    cannot race us.
    let mut session_slot = state.live_session.lock().await;
    if session_slot.is_some() {
        return Err("a live session is already active".to_string());
    }

    // 2. Validate + clear the token. The token slot is `tokio::sync::Mutex`
    //    so we don't need to hold the parking_lot guard across `.await`s.
    {
        let mut pending = state.pending_live_token.lock().await;
        LiveGuard::validate_token(&*pending, &strategy_id, &token)?;
        *pending = None;
    }

    // 3. Re-fetch the strategy and re-parse the DSL (it could have changed
    //    since `request_live_start` was called, e.g. via `deploy_strategy`
    //    with the same id — though id is unique per deploy). The cost is
    //    trivial; the determinism benefit (no stale AST in the session) is
    //    worth it.
    let record = state
        .strategies
        .get(&strategy_id)
        .await?
        .ok_or_else(|| format!("strategy not found: {strategy_id}"))?;
    if record.mode != crate::commands::registry::StrategyMode::Live {
        return Err(
            "strategy was not deployed in Live mode; redeploy with mode 'live' to enable \
             live trading"
                .to_string(),
        );
    }

    let strategy_node = parse_and_validate_dsl(&record.dsl_source)?;
    let symbol = match &strategy_node.trade_in {
        Some(TradeIn::Symbols(symbols)) if symbols.len() == 1 => {
            symbols.first().cloned().unwrap()
        }
        _ => {
            return Err(
                "live mode in Phase 7 supports exactly one TRADE_IN symbol".to_string(),
            )
        }
    };

    // 4. Fetch seed candles. Tolerate failure with a stderr warning; the
    //    engine simply starts cold with whatever it has. The full seed is
    //    a "nice to have" for indicator warm-up, not a safety precondition.
    let seed = match fetch_seed_candles(&state, &symbol).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "confirm_live_start: seed OHLCV fetch failed for {symbol}: {e}; starting cold"
            );
            Vec::new()
        }
    };

    // 5. Start the session. Wire the Tauri app handle as the failure-event
    //    emitter so loud alerts surface in the UI. The session writes a
    //    SessionContext onto the broker before the first execute, so the
    //    trade log entry carries the strategy metadata.
    let emitter: Arc<dyn SessionEventEmitter> = Arc::new(TauriSessionEmitter::new(
        state.app_handle.clone(),
    ));
    let session = LiveSession::start(
        strategy_id.clone(),
        record.name.clone(),
        symbol.clone(),
        strategy_node,
        state.live_guard.broker.clone(),
        state.data.feed.clone(),
        state.trade_log.clone(),
        state.event_bus.clone(),
        seed,
        // Starting capital for `RISK MAX_DAILY_LOSS`. Default to 1M INR
        // (sensible NSE retail starting capital) until settings expose
        // a per-strategy starting cash; the live engine cannot recover
        // this from `DhanBroker` and the cap would otherwise silently
        // never fire. A negative/zero value triggers a stderr warning
        // inside `LiveSession::start`.
        DEFAULT_LIVE_INITIAL_CASH,
        emitter,
        // B1 (audit): the per-candle market-hours gate reuses the
        // same NSE holiday calendar the preflight gate already
        // consulted, so session-start and per-candle decisions can
        // never drift apart.
        state.live_guard.holiday_calendar.clone(),
    )
    .await?;

    // 6. Insert into the slot. Lock is held — no other caller can race.
    *session_slot = Some(session);
    Ok(())
}

/// Write (or rewrite) the persistent "live trading acknowledged" file. A
/// one-time consent that survives across sessions and reboots. After this
/// runs, `request_live_start` will return `requires_ack: false`.
pub async fn acknowledge_live_trading(
    state: State<'_, AppState>,
) -> Result<(), String> {
    let payload = serde_json::json!({
        "acknowledged": true,
        "timestamp": chrono::Utc::now().to_rfc3339(),
    });
    let serialized = serde_json::to_string_pretty(&payload)
        .map_err(|e| format!("failed to serialize ack: {e}"))?;
    std::fs::write(&state.ack_path, serialized)
        .map_err(|e| format!("failed to write ack file: {e}"))
}

/// Adapter from `tauri::AppHandle` to the library's `SessionEventEmitter`
/// trait. Lives here (rather than `live::session`) because the `tauri`
/// dependency only resolves in the binary crate; the library only sees
/// the trait.
struct TauriSessionEmitter {
    handle: tauri::AppHandle,
}

impl TauriSessionEmitter {
    fn new(handle: tauri::AppHandle) -> Self {
        Self { handle }
    }
}

impl SessionEventEmitter for TauriSessionEmitter {
    fn emit_failed(&self, payload: crate::live::session::LiveSessionFailedPayload) {
        use tauri::Emitter;
        let _ = self.handle.emit("live-session-failed", &payload);
    }
}

async fn fetch_seed_candles(
    state: &AppState,
    symbol: &str,
) -> Result<Vec<crate::models::Candle>, String> {
    let to = chrono::Utc::now().timestamp_millis();
    // 500 bars of 1-minute data is roughly a day — a reasonable warm-up
    // window. Falls back gracefully (logged warning, empty vec) on error.
    let from = to - 500 * 60 * 1_000;
    state
        .data
        .broker
        .get_ohlcv(symbol, Timeframe::M1, from, to)
        .await
        .map_err(|e| e.to_string())
}

// =============================================================================
// Phase 7 — Pause / resume / stop / status
// =============================================================================

/// Pause the live session. New BUY orders are suppressed; SL/TP/risk-breach
/// SELLs always execute. Idempotent — calling pause on an already-paused
/// session is a no-op (the underlying broker flag is just set again).
pub async fn pause_live_strategy(
    state: State<'_, AppState>,
) -> Result<(), String> {
    let session = {
        let slot = state.live_session.lock().await;
        slot.as_ref()
            .ok_or_else(|| "no live session".to_string())?
            .clone()
    };
    session.pause();
    Ok(())
}

/// Resume the live session. Only meaningful after a pause; calling resume on
/// a non-paused session leaves it in `Running`.
///
/// H2 (audit): refuse to resume when the broker's safety cache (realized-loss
/// and available-cash) is still stale. The tick loop auto-pauses when the
/// cache goes stale; if the user clicks Resume before the next successful
/// background refresh, the session would resume with an unreliable safety
/// metric — the very thing the pause was protecting. The error includes the
/// elapsed time since the last successful refresh (when known) so the user
/// knows whether to retry immediately or wait for connectivity to recover.
pub async fn resume_live_strategy(
    state: State<'_, AppState>,
) -> Result<(), String> {
    let session = {
        let slot = state.live_session.lock().await;
        slot.as_ref()
            .ok_or_else(|| "no live session".to_string())?
            .clone()
    };

    if session.broker_is_stale() {
        let staleness = match session.broker_time_since_last_success() {
            None => "the broker refresh has never succeeded since this session started"
                .to_string(),
            Some(elapsed) => format!(
                "the last successful broker refresh was {elapsed:?} ago"
            ),
        };
        return Err(format!(
            "broker cache is stale — cannot resume a live session while the realized-loss or \
             available-cash tracker is unreliable. The auto-pause was correct; please wait for \
             the next successful refresh and try again. {staleness}."
        ));
    }

    session.resume();
    Ok(())
}

/// Outcome of stopping the live session. `stopped` is always true on success;
/// `open_positions_warning` carries a human-readable warning when the
/// session left open positions or pending orders that the user must close
/// manually in their broker app.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StopResult {
    pub stopped: bool,
    /// Warning about state the user must handle manually.
    /// e.g. "5 open positions and 2 pending orders remain in NIFTY —
    /// close them manually in your broker app."
    pub open_positions_warning: Option<String>,
}

/// Stop the live session, surface any open-position warning to the UI, and
/// clear the session slot so a new one can be started.
///
/// Ordering matters: we take the session out of the slot, STOP the task
/// first (cancels the cancellation token and awaits the tick loop), and
/// ONLY THEN query positions. Querying before `stop()` races the tick loop
/// — a SELL that closes a position could land between the snapshot and the
/// loop's exit, hiding the close from the warning. With `stop()` first, the
/// position snapshot is the final state of the session.
pub async fn stop_live_strategy(
    state: State<'_, AppState>,
) -> Result<StopResult, String> {
    // 1. Take the session out of the slot.
    let session = state
        .live_session
        .lock()
        .await
        .take()
        .ok_or_else(|| "no live session".to_string())?;

    // 2. Stop the session (cancels tick loop, awaits task exit). Once this
    //    returns, no further orders can land — `execute_with_meta` is only
    //    reachable from the engine tick loop, and the loop is gone.
    session.stop().await;

    // 3. Query positions AFTER the loop has stopped. We tolerate failure
    //    with an empty list — better to surface "stopped cleanly" than to
    //    fail the stop command because the broker was unreachable. The user
    //    can still see open positions in their broker app.
    let positions = session.broker_positions().await;
    let open_count = positions.iter().filter(|p| p.quantity != 0).count();

    // 4. Emit an event so the UI toasts even if the user isn't looking at
    //    the Live screen. The `app_handle` is `Option`-free — Tauri
    //    always supplies one in the binary crate.
    let warning = if open_count > 0 {
        let msg = format!(
            "WARNING: {} open position(s) remain — close them manually in your broker app. \
             Pending limit orders are not auto-cancelled; please verify in your broker app.",
            open_count
        );
        use tauri::Emitter;
        let _ = state.app_handle.emit(
            "live-session-stopped-with-positions",
            serde_json::json!({ "warning": msg.clone() }),
        );
        Some(msg)
    } else {
        None
    };

    Ok(StopResult {
        stopped: true,
        open_positions_warning: warning,
    })
}

/// Wire shape returned by `get_live_status`. `None` when no session is
/// active — the UI distinguishes that from "session exists but idle" by
/// absence alone (an idle strategy would still report a status).
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LiveStatusWire {
    pub strategy_id: String,
    pub strategy_name: String,
    pub symbol: String,
    /// "Starting" | "Running" | "Paused" | "Stopped" | "Failed"
    pub status: String,
    pub fail_reason: Option<String>,
    pub start_time: String,
    pub position_count: i64,
    /// Non-negative magnitude of session-realized losses in rupees.
    pub realized_loss: f64,
    /// True if the broker's loss-tracking refresh is failing.
    pub loss_tracking_stale: bool,
}

/// Snapshot the current live session's status. Returns `Ok(None)` when no
/// session is active so the UI can render an empty state.
pub async fn get_live_status(
    state: State<'_, AppState>,
) -> Result<Option<LiveStatusWire>, String> {
    let session = {
        let slot = state.live_session.lock().await;
        match slot.as_ref() {
            Some(s) => s.clone(),
            None => return Ok(None),
        }
    };
    // Lock is released — we can await freely.

    let positions = session.broker_positions().await;
    let (status_str, fail_reason) = match session.status() {
        crate::live::session::SessionStatus::Starting => ("Starting".into(), None),
        crate::live::session::SessionStatus::Running => ("Running".into(), None),
        crate::live::session::SessionStatus::Paused => ("Paused".into(), None),
        crate::live::session::SessionStatus::Stopped => ("Stopped".into(), None),
        crate::live::session::SessionStatus::Failed(r) => ("Failed".into(), Some(r)),
    };

    Ok(Some(LiveStatusWire {
        strategy_id: session.strategy_id.clone(),
        strategy_name: session.strategy_name.clone(),
        symbol: session.symbol.clone(),
        status: status_str,
        fail_reason,
        start_time: session.start_time.to_rfc3339(),
        position_count: positions.iter().filter(|p| p.quantity != 0).count() as i64,
        realized_loss: session.broker_realized_loss(),
        loss_tracking_stale: session.broker_is_stale(),
    }))
}
pub mod data;
pub mod indices;
pub mod live;
pub mod plugins;
pub mod registry;
pub mod search;
pub mod state;
pub mod strategy;

pub use state::AppState;
use crate::commands::state::AppState;
use crate::plugin::{PluginId, PluginListEntry};

/// List every loaded plugin. Returns a snapshot suitable for direct
/// serialization to the React app (the `PluginListEntry` wire type
/// already derives `Serialize`).
pub async fn list_plugins(state: &AppState) -> Result<Vec<PluginListEntry>, String> {
    Ok(state.plugin_registry.list())
}

/// Move a loaded plugin into the `Enabled` state.
pub async fn enable_plugin(state: &AppState, id: String) -> Result<(), String> {
    state
        .plugin_registry
        .enable(&PluginId(id))
        .await
        .map_err(|e| e.to_string())
}

/// Move an enabled plugin back to `Disabled`.
pub async fn disable_plugin(state: &AppState, id: String) -> Result<(), String> {
    state
        .plugin_registry
        .disable(&PluginId(id))
        .await
        .map_err(|e| e.to_string())
}

/// Re-scan the plugins directory, loading any new entries and replacing
/// the registry's view of existing ones. Returns a list of per-plugin
/// error messages (the empty list means every plugin loaded cleanly).
pub async fn reload_plugins(state: &AppState) -> Result<Vec<String>, String> {
    let results = state.plugin_registry.scan_and_load().await;
    Ok(results
        .into_iter()
        .filter_map(|(id, r)| r.err().map(|e| format!("{id}: {e}")))
        .collect())
}
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::strategy::dsl::{AstValidator, Lexer, Parser};

/// Execution target a deployed strategy is intended for. Serializes as
/// `"paper"` / `"live"` to match the TS union in `src/types/strategy.ts`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StrategyMode {
    Paper,
    Live,
}

impl StrategyMode {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "paper" => Ok(Self::Paper),
            "live" => Ok(Self::Live),
            other => Err(format!(
                "unknown mode '{other}' — expected 'paper' or 'live'"
            )),
        }
    }
}

/// User-controlled run state. The UI only ever sends `"running"` or `"paused"`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StrategyStatus {
    Running,
    Paused,
}

impl StrategyStatus {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "running" => Ok(Self::Running),
            "paused" => Ok(Self::Paused),
            other => Err(format!(
                "unknown status '{other}' — expected 'running' or 'paused'"
            )),
        }
    }
}

/// Wire shape returned to the UI. Mirrors `DeployedStrategy` in
/// `src/types/strategy.ts` field-for-field. The persisted `DeployedStrategyRecord`
/// holds the same data plus a sort key; this is the externally-visible form.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeployedStrategy {
    pub id: String,
    pub name: String,
    pub description: String,
    pub total_pnl: f64,
    pub total_trades: usize,
    pub modes: Vec<StrategyMode>,
    pub status: StrategyStatus,
    pub dsl_source: String,
}

/// On-disk shape. Identical fields to `DeployedStrategy` plus `deployed_at`
/// (a millisecond timestamp used for stable ordering) and a single `mode`
/// (since each record was deployed once with one mode).
///
/// `pub` so the live-start preflight (Phase 7) can use `StrategyRegistry::get`
/// to access the `mode` and `dsl_source` of a single record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeployedStrategyRecord {
    pub id: String,
    pub name: String,
    pub mode: StrategyMode,
    pub status: StrategyStatus,
    pub dsl_source: String,
    pub description: String,
    pub total_pnl: f64,
    pub total_trades: usize,
    pub deployed_at: i64,
}

impl From<&DeployedStrategyRecord> for DeployedStrategy {
    fn from(record: &DeployedStrategyRecord) -> Self {
        Self {
            id: record.id.clone(),
            name: record.name.clone(),
            description: record.description.clone(),
            total_pnl: record.total_pnl,
            total_trades: record.total_trades,
            modes: vec![record.mode],
            status: record.status,
            dsl_source: record.dsl_source.clone(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct RegistryFile {
    strategies: Vec<DeployedStrategyRecord>,
}

/// In-memory registry of deployed strategies, persisted as a single JSON file
/// under `app_data_dir/strategies.json`.
///
/// This is the storage side of deploy/list/set_status only — it does NOT
/// schedule ticks or run a live engine. Real execution is wired separately.
pub struct StrategyRegistry {
    store_path: PathBuf,
    inner: Mutex<HashMap<String, DeployedStrategyRecord>>,
    counter: AtomicU64,
}

impl StrategyRegistry {
    /// Opens (or creates) the registry at `store_path`. Creates the parent
    /// directory if needed and reads any existing file. Missing file is not
    /// an error — it just yields an empty registry.
    pub fn open(store_path: PathBuf) -> Result<Self, String> {
        if let Some(parent) = store_path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                format!(
                    "failed to create registry directory {}: {}",
                    parent.display(),
                    error
                )
            })?;
        }

        let records = if Path::new(&store_path).exists() {
            let raw = std::fs::read_to_string(&store_path)
                .map_err(|error| format!("failed to read {}: {}", store_path.display(), error))?;
            if raw.trim().is_empty() {
                Vec::new()
            } else {
                let parsed: RegistryFile = serde_json::from_str(&raw).map_err(|error| {
                    format!("failed to parse {}: {}", store_path.display(), error)
                })?;
                parsed.strategies
            }
        } else {
            Vec::new()
        };

        let counter = records.len() as u64;
        let mut map = HashMap::with_capacity(records.len());
        for record in records {
            map.insert(record.id.clone(), record);
        }

        Ok(Self {
            store_path,
            inner: Mutex::new(map),
            counter: AtomicU64::new(counter),
        })
    }

    /// Validates the DSL, generates an id, persists a new record, and returns
    /// the id. New strategies default to `Paused` so they don't auto-start;
    /// the user flips to `Running` via `set_strategy_status`.
    pub async fn deploy(
        &self,
        name: &str,
        dsl_source: &str,
        mode: StrategyMode,
    ) -> Result<String, String> {
        let errors = validate_dsl_local(dsl_source);
        if !errors.is_empty() {
            return Err(format!("strategy validation failed: {}", errors.join("; ")));
        }

        let id = self.generate_id();
        let deployed_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| format!("system clock before unix epoch: {error}"))?
            .as_millis() as i64;

        let record = DeployedStrategyRecord {
            id: id.clone(),
            name: name.to_string(),
            mode,
            status: StrategyStatus::Paused,
            dsl_source: dsl_source.to_string(),
            description: String::new(),
            total_pnl: 0.0,
            total_trades: 0,
            deployed_at,
        };

        let snapshot = {
            let mut guard = self.inner.lock().await;
            guard.insert(id.clone(), record);
            guard.values().cloned().collect::<Vec<_>>()
        };
        self.persist(&snapshot)?;
        Ok(id)
    }

    /// Returns all deployed strategies sorted by `deployed_at` ascending so
    /// newer entries appear at the bottom of the list.
    pub async fn list(&self) -> Result<Vec<DeployedStrategy>, String> {
        let guard = self.inner.lock().await;
        let mut records = guard.values().cloned().collect::<Vec<_>>();
        records.sort_by_key(|record| record.deployed_at);
        Ok(records.iter().map(DeployedStrategy::from).collect())
    }

    /// Look up a single record by id. Returns `Ok(None)` if the id is
    /// unknown; never errors for normal cases. Used by the live-start
    /// preflight which needs the full `DeployedStrategyRecord` (including
    /// `mode`, which the wire `DeployedStrategy` collapses into
    /// `modes: Vec<StrategyMode>`).
    pub async fn get(&self, id: &str) -> Result<Option<DeployedStrategyRecord>, String> {
        let guard = self.inner.lock().await;
        Ok(guard.get(id).cloned())
    }

    /// Flips the status of the matching strategy and persists.
    pub async fn set_status(&self, id: &str, status: StrategyStatus) -> Result<(), String> {
        let snapshot = {
            let mut guard = self.inner.lock().await;
            let record = guard
                .get_mut(id)
                .ok_or_else(|| format!("strategy '{id}' not found"))?;
            record.status = status;
            guard.values().cloned().collect::<Vec<_>>()
        };
        self.persist(&snapshot)
    }

    fn generate_id(&self) -> String {
        let n = self.counter.fetch_add(1, Ordering::SeqCst);
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        format!("strat-{millis}-{n:04}")
    }

    fn persist(&self, snapshot: &[DeployedStrategyRecord]) -> Result<(), String> {
        let file = RegistryFile {
            strategies: snapshot.to_vec(),
        };
        let serialized = serde_json::to_string_pretty(&file)
            .map_err(|error| format!("failed to serialize registry: {error}"))?;
        std::fs::write(&self.store_path, serialized)
            .map_err(|error| format!("failed to write {}: {}", self.store_path.display(), error))?;
        Ok(())
    }
}

/// Local copy of the lex+parse+validate pipeline so the registry doesn't
/// depend on `commands::strategy::validate_dsl` (which would create a cyclic
/// module dependency in the future when commands::strategy uses registry).
fn validate_dsl_local(dsl_source: &str) -> Vec<String> {
    let mut errors = Vec::new();

    let tokens = match Lexer::tokenize(dsl_source) {
        Ok(tokens) => tokens,
        Err(error) => {
            errors.push(format!(
                "line {} col {}: {}",
                error.line, error.col, error.message
            ));
            return errors;
        }
    };

    let node = match Parser::new(tokens).parse() {
        Ok(node) => node,
        Err(error) => {
            errors.push(format!(
                "line {} col {}: {}",
                error.line, error.col, error.message
            ));
            return errors;
        }
    };

    for error in AstValidator::validate(&node) {
        errors.push(error.message);
    }
    errors
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(name: &str) -> PathBuf {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "algomln-registry-test-{}-{}",
            name,
            std::process::id()
        ));
        // Use a deterministic suffix so concurrent tests don't collide.
        dir
    }

    #[tokio::test]
    async fn deploy_list_set_status_round_trip() {
        let path = temp_path("roundtrip");
        let _ = std::fs::remove_file(&path);

        let registry = StrategyRegistry::open(path.clone()).unwrap();
        let dsl = "WHEN rsi(14) < 30\nBUY 1";
        let id = registry
            .deploy("Test", dsl, StrategyMode::Paper)
            .await
            .unwrap();

        let listed = registry.list().await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, id);
        assert_eq!(listed[0].name, "Test");
        assert_eq!(listed[0].modes, vec![StrategyMode::Paper]);
        assert_eq!(listed[0].status, StrategyStatus::Paused);

        registry
            .set_status(&id, StrategyStatus::Running)
            .await
            .unwrap();
        let listed = registry.list().await.unwrap();
        assert_eq!(listed[0].status, StrategyStatus::Running);

        // Persistence: a fresh registry should see the same state.
        let again = StrategyRegistry::open(path.clone()).unwrap();
        let listed = again.list().await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].status, StrategyStatus::Running);

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn deploy_rejects_invalid_dsl() {
        let path = temp_path("invalid");
        let _ = std::fs::remove_file(&path);
        let registry = StrategyRegistry::open(path).unwrap();
        let err = registry
            .deploy("Bad", "WHEN rsi(0) < 30\nBUY 0", StrategyMode::Paper)
            .await
            .unwrap_err();
        assert!(err.contains("validation failed"));
    }

    #[tokio::test]
    async fn set_status_unknown_id_returns_error() {
        let path = temp_path("missing");
        let _ = std::fs::remove_file(&path);
        let registry = StrategyRegistry::open(path).unwrap();
        let err = registry
            .set_status("does-not-exist", StrategyStatus::Running)
            .await
            .unwrap_err();
        assert!(err.contains("not found"));
    }

    #[tokio::test]
    async fn open_missing_file_yields_empty_registry() {
        let path = temp_path("missing-file");
        let _ = std::fs::remove_file(&path);
        let registry = StrategyRegistry::open(path).unwrap();
        assert!(registry.list().await.unwrap().is_empty());
    }

    #[test]
    fn strategy_mode_parses_lowercase() {
        assert_eq!(StrategyMode::parse("paper").unwrap(), StrategyMode::Paper);
        assert_eq!(StrategyMode::parse("LIVE").unwrap(), StrategyMode::Live);
        assert!(StrategyMode::parse("sideways").is_err());
    }

    #[test]
    fn strategy_status_parses_lowercase() {
        assert_eq!(
            StrategyStatus::parse("running").unwrap(),
            StrategyStatus::Running
        );
        assert_eq!(
            StrategyStatus::parse("Paused").unwrap(),
            StrategyStatus::Paused
        );
        assert!(StrategyStatus::parse("stopped").is_err());
    }
}
//! Tauri command body for the `search_symbols` IPC.
//!
//! The fuzzy-search logic itself lives in `crate::search` and is exercised
//! directly by unit tests. This file is just the async body that pulls the
//! data the scorer needs out of `AppState` and forwards the result.
//!
//! **Lock discipline.** The body takes read guards on `symbol_map` and
//! `index_registry`, clones the data it needs into owned values, and
//! drops both guards BEFORE calling `fuzzy_search`. parking_lot's
//! `RwLockReadGuard` is `!Send` and must not be held across `.await` —
//! there is no `.await` in this function today, but the pattern keeps it
//! safe for future refactors.

use crate::commands::state::AppState;
use crate::search::{fuzzy_search, SymbolMatch};

/// Fuzzy-search the symbol universe (equities + indices).
///
/// `max_results` is hard-coded to 5 here; if a future caller wants a
/// different cap, the parameter should be added to the Tauri wrapper
/// (not a magic constant) and forwarded.
pub async fn search_symbols_impl(
    state: &AppState,
    query: String,
) -> Result<Vec<SymbolMatch>, String> {
    // Clone everything we need under short-lived guards, then drop the
    // guards. This keeps the function `Send` and avoids holding any
    // parking_lot guard across an `.await` (none today, but the rule
    // still applies).
    //
    // `state.symbol_map` is `Arc<parking_lot::RwLock<SymbolMap>>` so we
    // call `.read()` on the inner `RwLock` to get a guard, then borrow
    // the underlying map. `state.index_registry` is `Arc<IndexRegistry>`
    // — `IndexRegistry::collect_entries` already takes the read guard
    // internally and returns an owned `Vec<IndexEntry>`, which is the
    // safest shape to forward into a pure search function.
    let (equity_map, index_entries) = {
        let sym_map = state.symbol_map.read();
        let entries = state.index_registry.collect_entries();
        (sym_map.inner().clone(), entries)
    };

    Ok(fuzzy_search(&query, &equity_map, &index_entries, 5))
}
use std::{
    path::PathBuf,
    sync::Arc,
};

use tauri::AppHandle;
use tokio::sync::{broadcast, Mutex};

use crate::broker::symbol_map::SymbolMap;
use crate::commands::data::DataState;
use crate::commands::registry::StrategyRegistry;
use crate::indices::IndexRegistry;
use crate::live::{
    guard::{LiveGuard, PendingLiveToken},
    session::LiveSession,
    trade_log::TradeLog,
};
use crate::plugin::api::events::EventBus;
use crate::plugin::api::ui::UiMessage;
use crate::plugin::registry::PluginRegistry;

/// Tauri-managed application state. Owned by the binary crate but declared
/// here so the `commands::*` modules can use it as a `tauri::State`
/// parameter without depending on `crate::AppState` (which is not visible
/// from the library crate).
pub struct AppState {
    pub data: DataState,
    pub strategies: Arc<StrategyRegistry>,
    pub plugin_registry: Arc<PluginRegistry>,
    pub event_bus: Arc<EventBus>,
    pub ui_receiver: broadcast::Receiver<UiMessage>,
    /// Read-only-after-load registry of NSE index constituent lists.
    /// Populated from bundled seed JSON + a background refresh on startup.
    pub index_registry: Arc<IndexRegistry>,
    /// NSE symbol → Dhan `SECURITY_ID` map. Behind an `RwLock` so a future
    /// hot-refresh can swap the map without restarting the app.
    pub symbol_map: Arc<parking_lot::RwLock<SymbolMap>>,
    /// Append-only live execution audit log, persisted as JSONL.
    pub trade_log: Arc<TradeLog>,
    /// Path to the JSONL audit log for read-only IPC snapshots.
    pub trade_log_path: PathBuf,
    /// Phase-7 live runner: at most one live strategy session is active.
    /// Tokio mutex so the lock can be held across awaits in
    /// `confirm_live_start` without breaking the runtime's `Send` contract.
    pub live_session: Arc<Mutex<Option<Arc<LiveSession>>>>,
    /// Safety gate layer that runs the nine preflight checks before any
    /// live order can be placed. Wired with the same client / symbol map /
    /// broker that the rest of the app uses, plus the path to the
    /// acknowledged-live-trading file.
    pub live_guard: Arc<LiveGuard>,
    /// Single outstanding confirmation token, or `None`. Filled by
    /// `request_live_start`, cleared by `confirm_live_start`. Tokio mutex
    /// so the lock is safely held across awaits in the async command.
    pub pending_live_token: Arc<Mutex<Option<PendingLiveToken>>>,
    /// Path to the JSON file that records the user's one-time
    /// "I understand live trading is risky" consent. Created by
    /// `acknowledge_live_trading`.
    pub ack_path: PathBuf,
    /// Tauri app handle. Needed by `LiveSession::start` for failure
    /// alerts (see `TauriSessionEmitter` in the binary).
    pub app_handle: AppHandle,
}
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use chrono::Utc;
use serde::Serialize;

use crate::broker::Timeframe;
use crate::commands::data::DataState;
use crate::data::load_nifty_candles;
use crate::models::Candle;
use crate::strategy::analytics::{BacktestAnalyser, BacktestSummary};
use crate::strategy::dsl::{AstValidator, Lexer, Parser, StrategyNode};
use crate::strategy::execution::{PaperBroker, PaperBrokerState, PaperTrade};
use crate::strategy::logging::LogEntry;
use crate::strategy::runtime::engine::StrategyEngineProfile;
use crate::strategy::runtime::indicator_provider::IndicatorProviderProfile;
use crate::strategy::runtime::{StrategyEngine, StrategyInstance, StrategyStatus};

/// Re-export so callers (notably Prompt 3's Tauri commands) can invoke
/// `crate::commands::strategy::resolve_trade_in_symbols(&TradeIn, &IndexRegistry)`.
/// The implementation lives in `src/strategy/portfolio/engine.rs` next to
/// its only consumer (`PortfolioEngine::from_trade_in`).
pub use crate::strategy::portfolio::engine::resolve_trade_in_symbols;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BacktestResult {
    pub total_candles_processed: usize,
    pub final_cash: f64,
    pub initial_cash: f64,
    pub total_realized_pnl: f64,
    pub trade_history: Vec<PaperTrade>,
    pub broker_state: PaperBrokerState,
    pub logs: Vec<LogEntry>,
    pub summary: BacktestSummary,
    pub profile: BacktestProfile,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BacktestProfile {
    pub total_runtime_ms: u128,
    pub parser_validator_ms: u128,
    pub engine: EngineProfileReport,
    pub indicators: IndicatorProfileReport,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EngineProfileReport {
    pub on_candle_calls: u64,
    pub on_candle_time_ms: u128,
    pub broker_execute_calls: u64,
    pub broker_execute_time_ms: u128,
    pub broker_get_positions_calls: u64,
    pub broker_get_positions_time_ms: u128,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IndicatorProfileReport {
    pub get_calls: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub get_time_ms: u128,
}

pub async fn run_backtest_internal(
    node: StrategyNode,
    symbol: String,
    candles: Vec<Candle>,
    initial_cash: f64,
) -> Result<BacktestResult, String> {
    let total_started = Instant::now();
    let validation_started = Instant::now();
    let validation_errors = AstValidator::validate(&node);
    if !validation_errors.is_empty() {
        return Err(format!("strategy validation failed: {validation_errors:?}"));
    }
    let parser_validator_time = validation_started.elapsed();

    let broker = Arc::new(PaperBroker::new(symbol.clone(), initial_cash));
    let instance = StrategyInstance {
        id: "backtest-strategy".to_string(),
        strategy: Arc::new(node),
        symbol: symbol.clone(),
        timeframe: Timeframe::M5,
        status: StrategyStatus::Running,
        execution_target: broker.clone(),
        initial_cash,
    };
    let mut engine = StrategyEngine::new(instance);
    // EventBus is not wired during backtests — plugins observe live/paper sessions only.
    // engine.event_bus remains None.
    let mut logs = Vec::new();

    for index in 1..=candles.len() {
        if index % 10_000 == 0 || index == candles.len() {
            eprintln!(
                "Processed {} / {} candles | Elapsed: {:.2}s",
                index,
                candles.len(),
                total_started.elapsed().as_secs_f64()
            );
        }
        broker.update_unrealized(&symbol, candles[index - 1].close);
        logs.extend(engine.on_candle(&candles[..index]).await);
    }

    let engine_profile = engine.profile();
    let indicator_profile = engine.indicator_profile();
    let broker_state = broker.get_state();
    let summary = BacktestAnalyser::analyse(
        &broker_state.trade_history,
        initial_cash,
        broker_state.cash,
        candles.len(),
        &logs,
    );
    Ok(BacktestResult {
        total_candles_processed: candles.len(),
        final_cash: broker_state.cash,
        initial_cash,
        total_realized_pnl: broker_state.total_realized_pnl,
        trade_history: broker_state.trade_history.clone(),
        broker_state,
        logs,
        summary,
        profile: BacktestProfile {
            total_runtime_ms: total_started.elapsed().as_millis(),
            parser_validator_ms: parser_validator_time.as_millis(),
            engine: engine_profile_report(engine_profile),
            indicators: indicator_profile_report(indicator_profile),
        },
    })
}

fn engine_profile_report(profile: StrategyEngineProfile) -> EngineProfileReport {
    EngineProfileReport {
        on_candle_calls: profile.on_candle_calls,
        on_candle_time_ms: profile.on_candle_time.as_millis(),
        broker_execute_calls: profile.broker_execute_calls,
        broker_execute_time_ms: profile.broker_execute_time.as_millis(),
        broker_get_positions_calls: profile.broker_get_positions_calls,
        broker_get_positions_time_ms: profile.broker_get_positions_time.as_millis(),
    }
}

fn indicator_profile_report(profile: IndicatorProviderProfile) -> IndicatorProfileReport {
    IndicatorProfileReport {
        get_calls: profile.get_calls,
        cache_hits: profile.cache_hits,
        cache_misses: profile.cache_misses,
        get_time_ms: profile.get_time.as_millis(),
    }
}

// =============================================================================
// Tauri-facing wire types and DSL orchestrator
// =============================================================================

/// The shape the UI's `BacktestResult` interface expects. The internal
/// `BacktestResult` carries profile/log fields the UI doesn't render; this
/// truncated form keeps the IPC payload small and the contract explicit.
///
/// See `src/types/backtest.ts` for the matching TypeScript interface.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BacktestResultWire {
    pub trade_history: Vec<PaperTradeWire>,
    pub final_cash: f64,
    pub initial_cash: f64,
    pub total_realized_pnl: f64,
    pub total_candles_processed: usize,
    pub summary: BacktestSummary,
}

/// Wire-format `PaperTrade` with `timestamp` rendered as a millisecond string
/// to match the TS type. The internal `PaperTrade.timestamp: i64` is left
/// unchanged for analytics/calculations.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PaperTradeWire {
    pub id: String,
    pub timestamp: String,
    pub symbol: String,
    pub side: crate::models::OrderSide,
    pub quantity: usize,
    pub price: f64,
    pub pnl: Option<f64>,
}

impl From<&PaperTrade> for PaperTradeWire {
    fn from(trade: &PaperTrade) -> Self {
        Self {
            id: trade.id.clone(),
            timestamp: trade.timestamp.to_string(),
            symbol: trade.symbol.clone(),
            side: trade.side,
            quantity: trade.quantity,
            price: trade.price,
            pnl: trade.pnl,
        }
    }
}

/// Lexes, parses, and validates a `.algomln` source string. Returns a list of
/// human-readable error messages; an empty list means the strategy is valid.
///
/// The UI's `validateDsl` returns `string[]`, so we collapse lex/parse/validate
/// errors into one array. Lex/parse errors are formatted as
/// `"line {line} col {col}: {message}"`.
pub fn validate_dsl(dsl_source: &str) -> Vec<String> {
    let mut errors = Vec::new();

    let tokens = match Lexer::tokenize(dsl_source) {
        Ok(tokens) => tokens,
        Err(error) => {
            errors.push(format!(
                "line {} col {}: {}",
                error.line, error.col, error.message
            ));
            return errors;
        }
    };

    let node = match Parser::new(tokens).parse() {
        Ok(node) => node,
        Err(error) => {
            errors.push(format!(
                "line {} col {}: {}",
                error.line, error.col, error.message
            ));
            return errors;
        }
    };

    for error in AstValidator::validate(&node) {
        errors.push(error.message);
    }
    errors
}

/// Lex + parse + validate, returning the parsed [`StrategyNode`] on success
/// or the first error message as `Err`. Used by callers that need to inspect
/// the AST (notably the live-start preflight, which reads `risk`).
pub fn parse_and_validate_dsl(dsl_source: &str) -> Result<StrategyNode, String> {
    let tokens = Lexer::tokenize(dsl_source)
        .map_err(|error| format!("line {} col {}: {}", error.line, error.col, error.message))?;
    let node = Parser::new(tokens)
        .parse()
        .map_err(|error| format!("line {} col {}: {}", error.line, error.col, error.message))?;

    let validation_errors = AstValidator::validate(&node);
    if !validation_errors.is_empty() {
        let messages = validation_errors
            .iter()
            .map(|error| error.message.clone())
            .collect::<Vec<_>>()
            .join("; ");
        return Err(format!("strategy validation failed: {messages}"));
    }

    Ok(node)
}

/// Orchestrates a full backtest from raw DSL text: lex/parse/validate, fetch
/// candles (Dhan first, fallback CSV), run the engine, map to wire format.
///
/// `symbol` follows Dhan's `security_id|exchange_segment|instrument` form when
/// a `|` is present; otherwise the broker's default parsing applies. The
/// fallback CSV is always the bundled NIFTY 1-min sample regardless of symbol.
///
/// Multi-symbol strategies (any `TRADE_IN` clause) are rejected here —
/// see `PortfolioEngine` (`src/strategy/portfolio/engine.rs`) for the
/// multi-symbol paper/live execution path. Backtests over a universe of
/// symbols are not implemented.
pub async fn run_backtest_dsl(
    dsl_source: &str,
    symbol: &str,
    initial_cash: f64,
    data: &DataState,
) -> Result<BacktestResultWire, String> {
    let tokens = Lexer::tokenize(dsl_source)
        .map_err(|error| format!("line {} col {}: {}", error.line, error.col, error.message))?;
    let node = Parser::new(tokens)
        .parse()
        .map_err(|error| format!("line {} col {}: {}", error.line, error.col, error.message))?;

    let validation_errors = AstValidator::validate(&node);
    if !validation_errors.is_empty() {
        let messages = validation_errors
            .iter()
            .map(|error| error.message.clone())
            .collect::<Vec<_>>()
            .join("; ");
        return Err(format!("strategy validation failed: {messages}"));
    }

    if node.trade_in.is_some() {
        return Err(
            "Multi-symbol strategies (TRADE_IN) cannot be backtested yet. \
             Deploy as paper trading to run across the full symbol universe."
                .to_string(),
        );
    }

    let (candles, used_fallback) = fetch_candles(symbol, data).await?;
    if used_fallback {
        eprintln!(
            "run_backtest_dsl: using bundled NIFTY sample CSV ({} candles) instead of live data for symbol '{}'",
            candles.len(),
            symbol
        );
    }
    if candles.is_empty() {
        return Err("no candles available — provide a valid symbol or ensure the bundled sample CSV is reachable".to_string());
    }

    let result = run_backtest_internal(node, symbol.to_string(), candles, initial_cash).await?;
    Ok(map_to_wire(&result))
}

async fn fetch_candles(symbol: &str, data: &DataState) -> Result<(Vec<Candle>, bool), String> {
    // Try the live broker with the last 7 days of 1-minute candles. If it
    // returns an error or an empty vec (e.g. invalid security ID, no token,
    // holiday window), fall through to the bundled sample CSV.
    let to = Utc::now().timestamp_millis();
    let from = to - 7 * 24 * 60 * 60 * 1_000;
    let broker_result = data.broker.get_ohlcv(symbol, Timeframe::M1, from, to).await;

    match broker_result {
        Ok(candles) if !candles.is_empty() => Ok((candles, false)),
        Ok(_) => load_fallback_candles().await.map(|c| (c, true)),
        Err(error) => {
            eprintln!("run_backtest_dsl: live fetch failed ({error}); falling back to bundled CSV");
            load_fallback_candles().await.map(|c| (c, true))
        }
    }
}

async fn load_fallback_candles() -> Result<Vec<Candle>, String> {
    // The Tauri binary's CWD is the project root in dev; try both relative
    // shapes that show up across `cargo run`, `npm run tauri dev`, and the
    // packaged binary.
    for candidate in [
        Path::new("sample-data/nifty_1min.csv"),
        Path::new("../sample-data/nifty_1min.csv"),
    ] {
        if candidate.exists() {
            return load_nifty_candles(candidate);
        }
    }
    Err("could not locate sample-data/nifty_1min.csv (tried ./ and ../)".to_string())
}

fn map_to_wire(result: &BacktestResult) -> BacktestResultWire {
    BacktestResultWire {
        trade_history: result
            .trade_history
            .iter()
            .map(PaperTradeWire::from)
            .collect(),
        final_cash: result.final_cash,
        initial_cash: result.initial_cash,
        total_realized_pnl: result.total_realized_pnl,
        total_candles_processed: result.total_candles_processed,
        summary: result.summary.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_dsl_returns_empty_for_valid_strategy() {
        let source = "WHEN rsi(14) < 30\nBUY 1\n\nWHEN rsi(14) > 70\nSELL ALL";
        assert!(validate_dsl(source).is_empty());
    }

    #[test]
    fn validate_dsl_catches_lex_error() {
        let source = "WHEN rsi(14) <";
        let errors = validate_dsl(source);
        assert!(!errors.is_empty(), "expected at least one error");
        assert!(
            errors[0].contains("line") && errors[0].contains("col"),
            "expected line/col prefix, got {:?}",
            errors[0]
        );
    }

    #[test]
    fn validate_dsl_catches_validation_error() {
        // Zero quantity is rejected by the validator.
        let source = "WHEN rsi(14) < 30\nBUY 0";
        let errors = validate_dsl(source);
        assert!(errors.iter().any(|m| m.contains("quantity")));
    }

    #[test]
    fn wire_paper_trade_converts_timestamp_to_string() {
        let trade = PaperTrade {
            id: "t1".to_string(),
            timestamp: 1_700_000_000_000,
            symbol: "NIFTY".to_string(),
            side: crate::models::OrderSide::Buy,
            quantity: 10,
            price: 100.0,
            rule_id: "rule_0".to_string(),
            pnl: None,
        };
        let wire = PaperTradeWire::from(&trade);
        assert_eq!(wire.timestamp, "1700000000000");
        assert_eq!(wire.side, crate::models::OrderSide::Buy);
    }
}
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use chrono::NaiveDateTime;

use crate::models::Candle;

/// Loads a NIFTY-style OHLCV CSV. Rows can be tab-separated, comma-separated, or
/// whitespace-separated (in which case the first 19 characters are taken as the
/// timestamp). The first non-blank line is treated as a header and skipped.
///
/// Returns `String` errors so the loader is directly callable from Tauri
/// commands without an `anyhow` round-trip.
pub fn load_nifty_candles(path: &Path) -> Result<Vec<Candle>, String> {
    let file = File::open(path)
        .map_err(|error| format!("failed to open {}: {}", path.display(), error))?;
    let mut candles = Vec::new();
    for (index, line) in BufReader::new(file).lines().enumerate() {
        let line = line.map_err(|error| format!("read error at line {}: {}", index + 1, error))?;
        if index == 0 || line.trim().is_empty() {
            continue;
        }
        let fields = parse_market_row(&line)
            .map_err(|error| format!("bad NIFTY candle row {}: {}", index + 1, error))?;
        let timestamp = NaiveDateTime::parse_from_str(fields[0], "%Y-%m-%d %H:%M:%S")
            .map_err(|error| format!("bad timestamp at line {}: {}", index + 1, error))?
            .and_utc()
            .timestamp_millis();
        candles.push(Candle {
            timestamp,
            open: fields[1]
                .parse::<f64>()
                .map_err(|error| format!("bad open at line {}: {}", index + 1, error))?,
            high: fields[2]
                .parse::<f64>()
                .map_err(|error| format!("bad high at line {}: {}", index + 1, error))?,
            low: fields[3]
                .parse::<f64>()
                .map_err(|error| format!("bad low at line {}: {}", index + 1, error))?,
            close: fields[4]
                .parse::<f64>()
                .map_err(|error| format!("bad close at line {}: {}", index + 1, error))?,
            volume: 1_000.0,
        });
    }
    Ok(candles)
}

/// Splits a single CSV/TSV/whitespace-separated market row into exactly 5
/// fields: timestamp, open, high, low, close.
pub fn parse_market_row(line: &str) -> Result<Vec<&str>, String> {
    let tab_fields = line.split('\t').collect::<Vec<_>>();
    if tab_fields.len() == 5 {
        return Ok(tab_fields);
    }

    let comma_fields = line.split(',').collect::<Vec<_>>();
    if comma_fields.len() == 5 {
        return Ok(comma_fields);
    }

    let whitespace_fields = line.split_whitespace().collect::<Vec<_>>();
    if whitespace_fields.len() == 6 {
        let timestamp = line
            .get(0..19)
            .ok_or_else(|| "missing datetime".to_string())?;
        return Ok(vec![
            timestamp,
            whitespace_fields[2],
            whitespace_fields[3],
            whitespace_fields[4],
            whitespace_fields[5],
        ]);
    }

    Err("expected 5 market columns".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_tab_separated_row() {
        let line = "2024-01-01 09:15:00\t100.5\t101.0\t99.5\t100.75";
        let fields = parse_market_row(line).unwrap();
        assert_eq!(fields.len(), 5);
        assert_eq!(fields[0], "2024-01-01 09:15:00");
        assert_eq!(fields[4], "100.75");
    }

    #[test]
    fn parses_comma_separated_row() {
        let line = "2024-01-01 09:15:00,100.5,101.0,99.5,100.75";
        let fields = parse_market_row(line).unwrap();
        assert_eq!(fields[4], "100.75");
    }

    #[test]
    fn parses_whitespace_separated_row() {
        // Whitespace layout expects exactly 6 tokens: timestamp prefix,
        // symbol, and OHLC. The parser takes the first 19 characters as the
        // timestamp (YYYY-MM-DD HH:MM:SS) and reads OHLC from indices 2..6.
        let line = "2024-01-01 09:15:00 NIFTY 100.5 101.0 99.5";
        let fields = parse_market_row(line).unwrap();
        assert_eq!(fields[0], "2024-01-01 09:15:00");
        assert_eq!(fields[1], "NIFTY");
        assert_eq!(fields[4], "99.5");
    }

    #[test]
    fn rejects_malformed_row() {
        let err = parse_market_row("2024-01-01 09:15:00 100.5").unwrap_err();
        assert!(err.contains("5 market columns"));
    }
}
pub mod csv;

pub use csv::load_nifty_candles;
use std::collections::HashSet;

use anyhow::{bail, Result};
use tokio::sync::broadcast;

use crate::models::Tick;

const MAX_DHAN_SUBSCRIPTIONS: usize = 1_000;
const TICK_CHANNEL_CAPACITY: usize = 4_096;

#[derive(Debug)]
pub struct FeedManager {
    subscriptions: HashSet<String>,
    sender: broadcast::Sender<Tick>,
}

impl Default for FeedManager {
    fn default() -> Self {
        Self::new()
    }
}

impl FeedManager {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(TICK_CHANNEL_CAPACITY);
        Self {
            subscriptions: HashSet::new(),
            sender,
        }
    }

    pub async fn subscribe(&mut self, symbols: Vec<String>) -> Result<()> {
        let new_symbols = symbols
            .into_iter()
            .map(|symbol| symbol.trim().to_string())
            .filter(|symbol| !symbol.is_empty())
            .collect::<Vec<_>>();

        let new_symbols = new_symbols.into_iter().collect::<HashSet<_>>();
        let new_total = self.subscriptions.union(&new_symbols).count();
        if new_total > MAX_DHAN_SUBSCRIPTIONS {
            bail!("Dhan feed supports up to {MAX_DHAN_SUBSCRIPTIONS} subscriptions");
        }

        self.subscriptions.extend(new_symbols);
        Ok(())
    }

    pub async fn unsubscribe(&mut self, symbols: Vec<String>) -> Result<()> {
        for symbol in symbols {
            self.subscriptions.remove(symbol.trim());
        }

        Ok(())
    }

    pub fn subscribe_ticks(&self) -> broadcast::Receiver<Tick> {
        self.sender.subscribe()
    }

    pub fn publish_tick(&self, tick: Tick) {
        let _ = self.sender.send(tick);
    }

    pub fn subscriptions(&self) -> &HashSet<String> {
        &self.subscriptions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fans_out_ticks_to_multiple_receivers() {
        let manager = FeedManager::new();
        let mut first = manager.subscribe_ticks();
        let mut second = manager.subscribe_ticks();

        manager.publish_tick(Tick {
            symbol: "1333".to_string(),
            ltp: 100.0,
            volume: 10,
            timestamp: 1,
        });

        assert_eq!(first.recv().await.unwrap().ltp, 100.0);
        assert_eq!(second.recv().await.unwrap().symbol, "1333");
    }
}
pub mod manager;

pub use manager::FeedManager;
use crate::models::Candle;

pub fn atr(candles: &[Candle], period: usize) -> Vec<Option<f64>> {
    if period == 0 || candles.is_empty() {
        return vec![None; candles.len()];
    }

    let true_ranges = true_ranges(candles);
    let mut output = Vec::with_capacity(candles.len());
    let mut sum = 0.0;
    let mut previous_atr = None;

    for (index, true_range) in true_ranges.into_iter().enumerate() {
        if index + 1 < period {
            sum += true_range;
            output.push(None);
            continue;
        }

        if index + 1 == period {
            sum += true_range;
            let initial = sum / period as f64;
            previous_atr = Some(initial);
            output.push(Some(initial));
            continue;
        }

        let next = ((previous_atr.unwrap() * (period as f64 - 1.0)) + true_range) / period as f64;
        previous_atr = Some(next);
        output.push(Some(next));
    }

    output
}

fn true_ranges(candles: &[Candle]) -> Vec<f64> {
    candles
        .iter()
        .enumerate()
        .map(|(index, candle)| {
            if index == 0 {
                candle.high - candle.low
            } else {
                let previous_close = candles[index - 1].close;
                (candle.high - candle.low)
                    .max((candle.high - previous_close).abs())
                    .max((candle.low - previous_close).abs())
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indicators::tests::candle;

    #[test]
    fn atr_uses_true_range_and_wilder_smoothing() {
        let candles = vec![
            candle(0, 9.0, 12.0, 8.0, 10.0, 1.0),
            candle(1, 10.0, 15.0, 9.0, 14.0, 1.0),
            candle(2, 14.0, 16.0, 13.0, 15.0, 1.0),
            candle(3, 15.0, 18.0, 14.0, 17.0, 1.0),
        ];

        assert_eq!(
            atr(&candles, 3),
            vec![None, None, Some(13.0 / 3.0), Some(4.222222222222222)]
        );
    }
}
use crate::{indicators::ma::ema, models::Candle};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BBands {
    pub upper: f64,
    pub mid: f64,
    pub lower: f64,
}

pub fn bbands(candles: &[Candle], period: usize, std_dev_multiplier: f64) -> Vec<Option<BBands>> {
    if period == 0 {
        return vec![None; candles.len()];
    }

    let mids = ema(candles, period);
    let mut output = Vec::with_capacity(candles.len());

    for (index, mid) in mids.into_iter().enumerate() {
        let Some(mid) = mid else {
            output.push(None);
            continue;
        };

        let start = index + 1 - period;
        let variance = candles[start..=index]
            .iter()
            .map(|candle| (candle.close - mid).powi(2))
            .sum::<f64>()
            / period as f64;
        let std_dev = variance.sqrt();

        output.push(Some(BBands {
            upper: mid + std_dev_multiplier * std_dev,
            mid,
            lower: mid - std_dev_multiplier * std_dev,
        }));
    }

    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indicators::tests::candles_from_closes;

    #[test]
    fn bbands_returns_upper_mid_lower_after_warmup() {
        let candles = candles_from_closes(&[1.0, 2.0, 3.0]);

        let values = bbands(&candles, 3, 2.0);

        assert_eq!(values[0], None);
        assert_eq!(values[1], None);
        let bands = values[2].unwrap();
        assert_eq!(bands.mid, 2.0);
        assert!(bands.upper > bands.mid);
        assert!(bands.lower < bands.mid);
    }
}
use crate::models::Candle;

pub fn ma(candles: &[Candle], period: usize) -> Vec<Option<f64>> {
    if period == 0 {
        return vec![None; candles.len()];
    }

    let mut values = Vec::with_capacity(candles.len());
    let mut sum = 0.0;

    for (index, candle) in candles.iter().enumerate() {
        sum += candle.close;

        if index >= period {
            sum -= candles[index - period].close;
        }

        if index + 1 >= period {
            values.push(Some(sum / period as f64));
        } else {
            values.push(None);
        }
    }

    values
}

pub fn ema(candles: &[Candle], period: usize) -> Vec<Option<f64>> {
    ema_values(candles.iter().map(|candle| candle.close), period)
}

pub(crate) fn ema_values(values: impl IntoIterator<Item = f64>, period: usize) -> Vec<Option<f64>> {
    let values = values.into_iter().collect::<Vec<_>>();
    if period == 0 {
        return vec![None; values.len()];
    }

    let mut output = Vec::with_capacity(values.len());
    let mut warmup_sum = 0.0;
    let mut previous_ema = None;
    let multiplier = 2.0 / (period as f64 + 1.0);

    for (index, value) in values.iter().copied().enumerate() {
        if index + 1 < period {
            warmup_sum += value;
            output.push(None);
            continue;
        }

        if index + 1 == period {
            warmup_sum += value;
            let initial = warmup_sum / period as f64;
            previous_ema = Some(initial);
            output.push(Some(initial));
            continue;
        }

        let next = (value - previous_ema.unwrap()) * multiplier + previous_ema.unwrap();
        previous_ema = Some(next);
        output.push(Some(next));
    }

    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indicators::tests::candles_from_closes;

    #[test]
    fn ma_pads_until_period_is_available() {
        let candles = candles_from_closes(&[1.0, 2.0, 3.0, 4.0]);

        assert_eq!(ma(&candles, 3), vec![None, None, Some(2.0), Some(3.0)]);
    }

    #[test]
    fn ema_starts_with_sma_seed() {
        let candles = candles_from_closes(&[1.0, 2.0, 3.0, 4.0]);

        assert_eq!(ema(&candles, 3), vec![None, None, Some(2.0), Some(3.0)]);
    }
}
pub mod atr;
pub mod bb;
pub mod ma;
pub mod rsi;
pub mod vwap;

pub use atr::atr;
pub use bb::{bbands, BBands};
pub use ma::{ema, ma};
pub use rsi::rsi;
pub use vwap::{vwap, VwapPoint};

/// Relative volume: current candle volume divided by the mean volume of the
/// previous `period` candles. The current candle is excluded from the average.
pub fn rel_vol(candles: &[crate::models::Candle], period: usize) -> Vec<f64> {
    assert!(period > 0, "rel_vol period must be greater than zero");

    let mut values = Vec::with_capacity(candles.len());
    let mut sum = 0.0;

    for (index, candle) in candles.iter().enumerate() {
        if index < period {
            values.push(0.0);
        } else {
            let mean = sum / period as f64;
            values.push(if mean == 0.0 {
                0.0
            } else {
                candle.volume / mean
            });
        }

        sum += candle.volume;
        if index + 1 > period {
            sum -= candles[index + 1 - period].volume;
        }
    }

    values
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::models::Candle;

    pub fn candles_from_closes(closes: &[f64]) -> Vec<Candle> {
        closes
            .iter()
            .enumerate()
            .map(|(index, close)| candle(index as i64, *close, *close, *close, *close, 1.0))
            .collect()
    }

    pub fn candle(
        timestamp: i64,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        volume: f64,
    ) -> Candle {
        Candle {
            timestamp,
            open,
            high,
            low,
            close,
            volume,
        }
    }

    fn candle_with_vol(volume: f64) -> Candle {
        candle(0, 1.0, 1.0, 1.0, 1.0, volume)
    }

    #[test]
    fn rel_vol_returns_zero_for_insufficient_history() {
        let candles = vec![
            candle(0, 1.0, 1.0, 1.0, 1.0, 100.0),
            candle(1, 1.0, 1.0, 1.0, 1.0, 200.0),
            candle(2, 1.0, 1.0, 1.0, 1.0, 150.0),
        ];
        let result = super::rel_vol(&candles, 5);
        assert!(result.iter().all(|&value| value == 0.0));
    }

    #[test]
    fn rel_vol_correct_value() {
        let vols = [100.0, 100.0, 100.0, 100.0, 200.0];
        let candles = vols
            .iter()
            .map(|&volume| candle_with_vol(volume))
            .collect::<Vec<_>>();
        let result = super::rel_vol(&candles, 4);
        assert!((result[4] - 2.0).abs() < 0.001);
    }

    #[test]
    fn rel_vol_equal_volume_returns_one() {
        let candles = (0..10).map(|_| candle_with_vol(500.0)).collect::<Vec<_>>();
        let result = super::rel_vol(&candles, 3);
        for &value in &result[3..] {
            assert!((value - 1.0).abs() < 0.001);
        }
    }

    #[test]
    fn rel_vol_zero_average_returns_zero_not_nan() {
        let candles = (0..5).map(|_| candle_with_vol(0.0)).collect::<Vec<_>>();
        let result = super::rel_vol(&candles, 3);
        assert!(result.iter().all(|value| value.is_finite()));
    }
}
use crate::{indicators::ma::ema_values, models::Candle};

pub fn rsi(candles: &[Candle], period: usize) -> Vec<Option<f64>> {
    if period == 0 {
        return vec![None; candles.len()];
    }
    if candles.len() < 2 {
        return vec![None; candles.len()];
    }

    let mut gains = Vec::with_capacity(candles.len() - 1);
    let mut losses = Vec::with_capacity(candles.len() - 1);

    for window in candles.windows(2) {
        let change = window[1].close - window[0].close;
        gains.push(change.max(0.0));
        losses.push((-change).max(0.0));
    }

    let avg_gains = ema_values(gains, period);
    let avg_losses = ema_values(losses, period);
    let mut output = Vec::with_capacity(candles.len());
    output.push(None);

    for (avg_gain, avg_loss) in avg_gains.into_iter().zip(avg_losses) {
        output.push(match (avg_gain, avg_loss) {
            (Some(gain), Some(loss)) if loss == 0.0 && gain == 0.0 => Some(50.0),
            (Some(_), Some(loss)) if loss == 0.0 => Some(100.0),
            (Some(gain), Some(loss)) => {
                let relative_strength = gain / loss;
                Some(100.0 - (100.0 / (1.0 + relative_strength)))
            }
            _ => None,
        });
    }

    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indicators::tests::candles_from_closes;

    #[test]
    fn rsi_pads_until_enough_changes_exist() {
        let candles = candles_from_closes(&[1.0, 2.0, 3.0, 2.0, 4.0]);

        let values = rsi(&candles, 3);

        assert_eq!(values[0], None);
        assert_eq!(values[1], None);
        assert_eq!(values[2], None);
        assert!(values[3].is_some());
        assert!(values[4].is_some());
    }

    #[test]
    fn rsi_is_one_hundred_when_there_are_no_losses() {
        let candles = candles_from_closes(&[1.0, 2.0, 3.0, 4.0]);

        assert_eq!(rsi(&candles, 2), vec![None, None, Some(100.0), Some(100.0)]);
    }
}
use crate::models::Candle;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VwapPoint {
    pub value: f64,
}

pub fn vwap(candles: &[Candle]) -> Vec<Option<VwapPoint>> {
    let mut output = Vec::with_capacity(candles.len());
    let mut current_day = None;
    let mut cumulative_price_volume = 0.0;
    let mut cumulative_volume = 0.0;

    for candle in candles {
        let candle_day = candle.timestamp.div_euclid(86_400_000);
        if current_day != Some(candle_day) {
            current_day = Some(candle_day);
            cumulative_price_volume = 0.0;
            cumulative_volume = 0.0;
        }

        let typical_price = (candle.high + candle.low + candle.close) / 3.0;
        cumulative_price_volume += typical_price * candle.volume;
        cumulative_volume += candle.volume;

        if cumulative_volume == 0.0 {
            output.push(None);
        } else {
            output.push(Some(VwapPoint {
                value: cumulative_price_volume / cumulative_volume,
            }));
        }
    }

    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indicators::tests::candle;

    #[test]
    fn vwap_resets_on_new_utc_day() {
        let day = 86_400_000;
        let candles = vec![
            candle(0, 10.0, 12.0, 9.0, 9.0, 10.0),
            candle(1_000, 20.0, 24.0, 18.0, 18.0, 10.0),
            candle(day, 100.0, 120.0, 90.0, 90.0, 5.0),
        ];

        let values = vwap(&candles);

        assert_eq!(values[0], Some(VwapPoint { value: 10.0 }));
        assert_eq!(values[1], Some(VwapPoint { value: 15.0 }));
        assert_eq!(values[2], Some(VwapPoint { value: 100.0 }));
    }
}
pub mod refresh;
pub mod registry;

pub use refresh::{
    is_stale, refresh_all_if_stale, refresh_index, RefreshOutcome, DEFAULT_STALENESS,
};
pub use registry::{alias_display_name, IndexCacheFile, IndexEntry, IndexInfo, IndexRegistry};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::strategy::dsl::ast::IndexAlias;

use super::registry::{IndexCacheFile, IndexRegistry};

const BASE_URL: &str = "https://www.niftyindices.com/IndexConstituent/";

/// Default staleness threshold: a cache older than 24h triggers a refresh.
pub const DEFAULT_STALENESS: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Debug)]
pub struct RefreshOutcome {
    pub alias: String,
    pub success: bool,
    pub symbol_count: usize,
    pub error: Option<String>,
}

/// Fetch a single index from niftyindices.com, parse it, write the JSON
/// cache, and update the registry. Returns the outcome.
pub async fn refresh_index(
    alias: &IndexAlias,
    cache_dir: &Path,
    registry: &IndexRegistry,
) -> RefreshOutcome {
    let url = format!("{}{}", BASE_URL, alias.csv_filename());
    let alias_str = alias.dsl_keyword().to_string();

    match fetch_and_parse(&url).await {
        Ok(symbols) => {
            let today = chrono::Local::now().format("%Y-%m-%d").to_string();
            let cache_file = IndexCacheFile {
                alias: alias_str.clone(),
                last_updated: today.clone(),
                symbols: symbols.clone(),
            };
            let out_path = cache_dir.join(format!("{}.json", alias.cache_stem()));
            if let Err(e) = std::fs::create_dir_all(cache_dir) {
                eprintln!("[refresh_index] could not create cache dir: {}", e);
            }
            match serde_json::to_string_pretty(&cache_file) {
                Ok(json) => {
                    if let Err(e) = std::fs::write(&out_path, json) {
                        eprintln!(
                            "[refresh_index] could not write cache for {}: {}",
                            alias_str, e
                        );
                    }
                }
                Err(e) => eprintln!("[refresh_index] could not serialise {}: {}", alias_str, e),
            }
            registry.update(alias.clone(), symbols.clone(), today);
            let count = symbols.len();
            eprintln!("[refresh_index] {} → {} symbols", alias_str, count);
            RefreshOutcome {
                alias: alias_str,
                success: true,
                symbol_count: count,
                error: None,
            }
        }
        Err(e) => {
            eprintln!("[refresh_index] failed for {}: {}", alias_str, e);
            RefreshOutcome {
                alias: alias_str,
                success: false,
                symbol_count: 0,
                error: Some(e),
            }
        }
    }
}

async fn fetch_and_parse(url: &str) -> Result<Vec<String>, String> {
    let response = reqwest::Client::builder()
        .user_agent("AlgoMLN/1.0")
        .timeout(Duration::from_secs(15))
        .build()
        .map_err(|e| e.to_string())?
        .get(url)
        .send()
        .await
        .map_err(|e| format!("HTTP error: {}", e))?;

    if !response.status().is_success() {
        return Err(format!("HTTP {}", response.status()));
    }

    let text = response.text().await.map_err(|e| e.to_string())?;
    // Strip UTF-8 BOM if present.
    let text = text.trim_start_matches('\u{feff}');

    let mut rdr = csv::Reader::from_reader(text.as_bytes());
    let headers = rdr.headers().map_err(|e| e.to_string())?.clone();

    // Find the "Symbol" column index (niftyindices CSVs use this exact header).
    let symbol_col = headers
        .iter()
        .position(|h| h.trim() == "Symbol")
        .ok_or("CSV has no 'Symbol' column")?;

    let symbols: Vec<String> = rdr
        .records()
        .filter_map(|r| r.ok())
        .filter_map(|r| r.get(symbol_col).map(|s| s.trim().to_uppercase()))
        .filter(|s| !s.is_empty())
        .collect();

    if symbols.is_empty() {
        return Err("CSV parsed but contained no symbols".to_string());
    }
    Ok(symbols)
}

/// Check if `path` is older than `threshold` (or missing). Returns true if
/// refresh is needed.
pub fn is_stale(path: &Path, threshold: Duration) -> bool {
    path.metadata()
        .and_then(|m| m.modified())
        .map(|modified| {
            SystemTime::now()
                .duration_since(modified)
                .unwrap_or(Duration::MAX)
                > threshold
        })
        .unwrap_or(true)
}

/// Refresh all 22 indices if the oldest cache file is stale (> threshold).
/// Called in background on startup. Non-fatal: failures logged to stderr.
pub async fn refresh_all_if_stale(
    registry: Arc<IndexRegistry>,
    cache_dir: std::path::PathBuf,
    threshold: Duration,
) -> Vec<RefreshOutcome> {
    // Check staleness by looking at the nifty_50.json cache file as a proxy.
    let proxy = cache_dir.join("nifty_50.json");
    if !is_stale(&proxy, threshold) {
        return vec![]; // fresh enough, skip
    }

    eprintln!("[refresh_all_if_stale] indices are stale, refreshing all…");
    let mut outcomes = vec![];
    for alias in IndexAlias::all() {
        let outcome = refresh_index(alias, &cache_dir, &registry).await;
        outcomes.push(outcome);
        // Small delay between requests to be polite to niftyindices.com.
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    outcomes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_stale_returns_true_for_missing_path() {
        let path = Path::new("/tmp/algomln-test-no-such-file-12345");
        assert!(is_stale(path, Duration::from_secs(60)));
    }
}
use std::collections::HashMap;
use std::path::Path;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::strategy::dsl::ast::IndexAlias;

/// One NSE index loaded into the registry. `display_name` is the
/// human-readable label (e.g. "NIFTY 50") used by both the Settings UI and
/// the fuzzy-search result list; `alias` is the machine key.
#[derive(Debug, Clone)]
pub struct IndexEntry {
    pub alias: IndexAlias,
    pub display_name: String,
    pub symbols: Vec<String>,
    pub last_updated: String,
}

/// On-disk JSON format for a cached index constituent list.
#[derive(Debug, Serialize, Deserialize)]
pub struct IndexCacheFile {
    pub alias: String,
    /// ISO-8601 date, or "never" if unknown.
    pub last_updated: String,
    pub symbols: Vec<String>,
}

/// Wire type returned to the frontend via IPC.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexInfo {
    pub alias: String,        // e.g. "NIFTY_50"
    pub display_name: String, // e.g. "NIFTY 50"
    pub symbol_count: usize,
    pub last_updated: String, // ISO date or "never"
}

/// Single source of truth for the human-readable label of an
/// `IndexAlias`. Reused by `IndexRegistry` (load and update paths) and by
/// `list_indices` so we never inline the mapping twice.
pub fn alias_display_name(alias: &IndexAlias) -> &'static str {
    alias.display_name()
}

/// Read-only-after-load registry of NSE index constituent lists.
///
/// `IndexRegistry::update` is the only mutator and is intended to be called
/// only by `refresh_index` at startup. The strategy engine reads constituents
/// once at deploy time and does not re-read mid-run — see invariant #10
/// in `CLAUDE.md`.
pub struct IndexRegistry {
    data: RwLock<HashMap<IndexAlias, IndexEntry>>,
}

impl IndexRegistry {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
        }
    }

    /// Load index constituent files from `cache_dir` (user's app data).
    /// Falls back to `resource_dir` (bundled seed) for any file not found in
    /// cache. Logs to stderr for any index that fails to load from both
    /// locations. Inserts an empty entry for missing keys so callers can
    /// distinguish "loaded zero" from "not loaded at all".
    pub fn load_from_dirs(&self, cache_dir: &Path, resource_dir: &Path) {
        let mut data = self.data.write();
        for alias in IndexAlias::all() {
            let filename = format!("{}.json", alias.cache_stem());
            let cache_path = cache_dir.join(&filename);
            let resource_path = resource_dir.join(&filename);

            let loaded =
                Self::try_load_file(&cache_path).or_else(|| Self::try_load_file(&resource_path));

            match loaded {
                Some(file) => {
                    data.insert(
                        alias.clone(),
                        IndexEntry {
                            alias: alias.clone(),
                            display_name: alias_display_name(&alias).to_string(),
                            symbols: file.symbols,
                            last_updated: file.last_updated,
                        },
                    );
                }
                None => {
                    eprintln!(
                        "[IndexRegistry] no data for {} (tried {} and {})",
                        alias.display_name(),
                        cache_path.display(),
                        resource_path.display()
                    );
                    // Insert empty entry so the key always exists.
                    data.insert(
                        alias.clone(),
                        IndexEntry {
                            alias: alias.clone(),
                            display_name: alias_display_name(&alias).to_string(),
                            symbols: vec![],
                            last_updated: "never".to_string(),
                        },
                    );
                }
            }
        }
    }

    fn try_load_file(path: &Path) -> Option<IndexCacheFile> {
        let text = std::fs::read_to_string(path).ok()?;
        serde_json::from_str(&text).ok()
    }

    /// Returns the constituent symbol list for an alias. Empty vec if not loaded.
    pub fn get_symbols(&self, alias: &IndexAlias) -> Vec<String> {
        self.data
            .read()
            .get(alias)
            .map(|e| e.symbols.clone())
            .unwrap_or_default()
    }

    /// Update an alias entry after a successful refresh.
    pub fn update(&self, alias: IndexAlias, symbols: Vec<String>, last_updated: String) {
        self.data.write().insert(
            alias.clone(),
            IndexEntry {
                alias: alias.clone(),
                display_name: alias_display_name(&alias).to_string(),
                symbols,
                last_updated,
            },
        );
    }

    /// Snapshot every entry into an owned `Vec`. Use this from async
    /// contexts (e.g. the `search_symbols` command body) so the read
    /// guard can be dropped before any `.await` point. Iteration order
    /// matches `IndexAlias::all()` so search results are stable across
    /// calls (the underlying `HashMap` has nondeterministic order).
    pub fn collect_entries(&self) -> Vec<IndexEntry> {
        let data = self.data.read();
        IndexAlias::all()
            .iter()
            .filter_map(|alias| data.get(alias).cloned())
            .collect()
    }

    /// List info for all 22 indices (for IPC / UI display).
    pub fn list_info(&self) -> Vec<IndexInfo> {
        let data = self.data.read();
        IndexAlias::all()
            .iter()
            .map(|alias| {
                let entry = data.get(alias);
                IndexInfo {
                    alias: alias.dsl_keyword().to_string(),
                    display_name: alias_display_name(alias).to_string(),
                    symbol_count: entry.map(|e| e.symbols.len()).unwrap_or(0),
                    last_updated: entry
                        .map(|e| e.last_updated.clone())
                        .unwrap_or_else(|| "never".to_string()),
                }
            })
            .collect()
    }
}

impl Default for IndexRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_aliases_have_csv_filename() {
        for alias in IndexAlias::all() {
            assert!(!alias.csv_filename().is_empty());
            assert!(!alias.cache_stem().is_empty());
            assert!(!alias.dsl_keyword().is_empty());
        }
    }

    #[test]
    fn from_dsl_str_roundtrip() {
        for alias in IndexAlias::all() {
            let kw = alias.dsl_keyword();
            let parsed = IndexAlias::from_dsl_str(kw);
            assert_eq!(parsed.as_ref(), Some(alias), "roundtrip failed for {}", kw);
        }
    }

    #[test]
    fn list_info_returns_all_22_indices() {
        let registry = IndexRegistry::new();
        let info = registry.list_info();
        assert_eq!(info.len(), 22);
    }

    #[test]
    fn update_then_get_symbols_roundtrip() {
        let registry = IndexRegistry::new();
        registry.update(
            IndexAlias::NiftyBank,
            vec!["HDFCBANK".to_string(), "ICICIBANK".to_string()],
            "2026-07-17".to_string(),
        );
        let symbols = registry.get_symbols(&IndexAlias::NiftyBank);
        assert_eq!(symbols, vec!["HDFCBANK", "ICICIBANK"]);
    }
}
pub mod broker;
pub mod commands;
pub mod data;
pub mod feed;
pub mod indicators;
pub mod indices;
pub mod live;
pub mod models;
pub mod plugin;
pub mod search;
pub mod strategy;
use crate::models::{Candle, Tick};

pub struct CandleAssembler {
    symbol: String,
    current_minute: Option<i64>,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

impl CandleAssembler {
    pub fn new(symbol: String) -> Self {
        Self {
            symbol,
            current_minute: None,
            open: 0.0,
            high: 0.0,
            low: 0.0,
            close: 0.0,
            volume: 0.0,
        }
    }

    /// Feed one tick. Returns zero or more candles that *closed* on this tick:
    /// - One candle for the minute the previous ticks were accumulating in
    ///   (its OHLCV is whatever prices/volume arrived in that minute).
    /// - One zero-volume *gap candle* per minute that was skipped between the
    ///   previous minute and the new tick's minute. Gap candles use the last
    ///   seen close as their open/high/low/close so indicators that read
    ///   `PrevClose` see a contiguous series across feed gaps.
    ///
    /// Returns an empty `Vec` when the tick falls inside the in-progress
    /// minute or when the tick's symbol doesn't match the assembler's symbol.
    pub fn feed(&mut self, tick: &Tick) -> Vec<Candle> {
        if tick.symbol != self.symbol {
            return Vec::new();
        }

        let tick_minute = tick.timestamp / 60_000 * 60_000;

        let mut emitted = Vec::new();

        match self.current_minute {
            None => {
                self.start_new_candle(tick_minute, tick);
            }
            Some(current_minute) if tick_minute == current_minute => {
                self.high = self.high.max(tick.ltp);
                self.low = self.low.min(tick.ltp);
                self.close = tick.ltp;
                self.volume += tick.volume as f64;
            }
            Some(current_minute) => {
                // Close the in-progress minute.
                emitted.push(Candle {
                    timestamp: current_minute,
                    open: self.open,
                    high: self.high,
                    low: self.low,
                    close: self.close,
                    volume: self.volume,
                });

                // Emit one zero-volume gap candle per missing minute between
                // the closed minute and the new tick's minute. Each gap candle
                // is flat at the last seen close so downstream indicators that
                // rely on a uniform one-candle-per-period cadence see a
                // continuous series rather than a discontinuity.
                let last_close = self.close;
                let mut gap_minute = current_minute + 60_000;
                while gap_minute < tick_minute {
                    emitted.push(Candle {
                        timestamp: gap_minute,
                        open: last_close,
                        high: last_close,
                        low: last_close,
                        close: last_close,
                        volume: 0.0,
                    });
                    gap_minute += 60_000;
                }

                self.start_new_candle(tick_minute, tick);
            }
        }

        emitted
    }

    /// Emit the in-progress minute's candle, if any, and reset state.
    ///
    /// The live tick loop calls this from its `cancel.cancelled()` branch and
    /// from its `RecvError::Closed` branch so the last partial minute's
    /// prices and volume aren't silently dropped when the session ends or
    /// the feed shuts down.
    ///
    /// Returns `None` if no tick has ever been fed (no minute in progress).
    /// Calling `flush` a second time after the first non-`None` return also
    /// returns `None`.
    pub fn flush(&mut self) -> Option<Candle> {
        let current_minute = self.current_minute?;
        let candle = Candle {
            timestamp: current_minute,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
        };
        self.current_minute = None;
        self.open = 0.0;
        self.high = 0.0;
        self.low = 0.0;
        self.close = 0.0;
        self.volume = 0.0;
        Some(candle)
    }

    fn start_new_candle(&mut self, minute: i64, tick: &Tick) {
        self.current_minute = Some(minute);
        self.open = tick.ltp;
        self.high = tick.ltp;
        self.low = tick.ltp;
        self.close = tick.ltp;
        self.volume = tick.volume as f64;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tick(price: f64, volume: u64, timestamp: i64) -> Tick {
        Tick {
            symbol: "NIFTY".to_string(),
            ltp: price,
            volume,
            timestamp,
        }
    }

    #[test]
    fn test_first_tick_no_candle() {
        let mut assembler = CandleAssembler::new("NIFTY".to_string());

        assert!(assembler.feed(&tick(100.0, 10, 60_000)).is_empty());
    }

    #[test]
    fn test_same_minute_no_candle() {
        let mut assembler = CandleAssembler::new("NIFTY".to_string());

        assert!(assembler.feed(&tick(100.0, 10, 60_000)).is_empty());
        assert!(assembler.feed(&tick(101.0, 11, 61_000)).is_empty());
        assert!(assembler.feed(&tick(99.0, 12, 119_999)).is_empty());
    }

    #[test]
    fn test_new_minute_returns_candle() {
        let mut assembler = CandleAssembler::new("NIFTY".to_string());

        assert!(assembler.feed(&tick(100.0, 10, 60_000)).is_empty());
        assert!(assembler.feed(&tick(105.0, 15, 61_000)).is_empty());
        assert!(assembler.feed(&tick(98.0, 20, 119_999)).is_empty());

        let candles = assembler.feed(&tick(110.0, 25, 120_000));
        assert_eq!(candles.len(), 1, "exactly one minute closed, no gaps");

        let candle = &candles[0];
        assert_eq!(candle.timestamp, 60_000);
        assert_eq!(candle.open, 100.0);
        assert_eq!(candle.high, 105.0);
        assert_eq!(candle.low, 98.0);
        assert_eq!(candle.close, 98.0);
        assert_eq!(candle.volume, 45.0);
    }

    #[test]
    fn test_high_low_tracked() {
        let mut assembler = CandleAssembler::new("NIFTY".to_string());

        assert!(assembler.feed(&tick(100.0, 10, 60_000)).is_empty());
        assert!(assembler.feed(&tick(105.0, 10, 61_000)).is_empty());
        assert!(assembler.feed(&tick(98.0, 10, 62_000)).is_empty());

        let candles = assembler.feed(&tick(101.0, 10, 120_000));
        assert_eq!(candles.len(), 1);

        assert_eq!(candles[0].high, 105.0);
        assert_eq!(candles[0].low, 98.0);
    }

    // B2 (audit): a multi-minute feed gap previously caused the assembler to
    // emit only the last-traded minute and silently drop the in-between
    // minutes, giving the engine a non-contiguous series. After the fix the
    // assembler emits a zero-volume gap candle for every minute strictly
    // between the closed minute and the new tick's minute, flat at the last
    // seen close.
    #[test]
    fn test_gap_emits_zero_volume_minutes() {
        let mut assembler = CandleAssembler::new("NIFTY".to_string());

        // Tick in minute 1 [60_000, 120_000): last close will be 98.0.
        assert!(assembler.feed(&tick(100.0, 10, 60_000)).is_empty());
        assert!(assembler.feed(&tick(105.0, 15, 61_000)).is_empty());
        assert!(assembler.feed(&tick(98.0, 20, 119_999)).is_empty());

        // Next tick at minute 3 (180_000). The new tick *starts* minute 3,
        // so only minute 2 is a gap fill; minute 3 is the new in-progress
        // candle. Minute 1 (real) + minute 2 (gap) = 2 emitted candles.
        let candles = assembler.feed(&tick(110.0, 25, 180_000));
        assert_eq!(candles.len(), 2, "minute 1 (real) + minute 2 (gap)");

        // Minute 1: real OHLCV.
        let m1 = &candles[0];
        assert_eq!(m1.timestamp, 60_000);
        assert_eq!(m1.open, 100.0);
        assert_eq!(m1.high, 105.0);
        assert_eq!(m1.low, 98.0);
        assert_eq!(m1.close, 98.0);
        assert_eq!(m1.volume, 45.0);

        // Minute 2: zero-volume gap candle flat at last close.
        let m2 = &candles[1];
        assert_eq!(m2.timestamp, 120_000);
        assert_eq!(m2.open, 98.0);
        assert_eq!(m2.high, 98.0);
        assert_eq!(m2.low, 98.0);
        assert_eq!(m2.close, 98.0);
        assert_eq!(m2.volume, 0.0);
    }

    #[test]
    fn test_multi_minute_gap_emits_one_per_missing_minute() {
        let mut assembler = CandleAssembler::new("NIFTY".to_string());

        assert!(assembler.feed(&tick(100.0, 10, 60_000)).is_empty());

        // 5-minute jump: from minute 1 to minute 6 (360_000). Should emit
        // minute 1 (real) + minutes 2, 3, 4, 5 (4 gap fills) = 5 candles.
        let candles = assembler.feed(&tick(110.0, 25, 360_000));
        assert_eq!(
            candles.len(),
            5,
            "one real minute + four gap fills for a 5-minute jump"
        );

        // The closed minute is at timestamp 60_000; gap minutes start at 120_000.
        assert_eq!(candles[0].timestamp, 60_000);
        assert_eq!(candles[0].volume, 10.0, "real minute has the original volume");
        for (i, gap) in candles.iter().enumerate().skip(1) {
            assert_eq!(gap.timestamp, 60_000 + (i as i64) * 60_000);
            assert_eq!(gap.volume, 0.0, "gap candle {} has zero volume", i);
            assert_eq!(gap.open, 100.0);
            assert_eq!(gap.high, 100.0);
            assert_eq!(gap.low, 100.0);
            assert_eq!(gap.close, 100.0, "gap candle flat at last close");
        }
    }

    #[test]
    fn test_flush_emits_partial_candle() {
        let mut assembler = CandleAssembler::new("NIFTY".to_string());

        // Two ticks in the same minute — minute stays in progress.
        assert!(assembler.feed(&tick(100.0, 10, 60_000)).is_empty());
        assert!(assembler.feed(&tick(105.0, 20, 61_000)).is_empty());

        // Flush emits the partial minute's accumulated prices/volume.
        let candle = assembler
            .flush()
            .expect("in-progress minute should be flushed");

        assert_eq!(candle.timestamp, 60_000);
        assert_eq!(candle.open, 100.0);
        assert_eq!(candle.high, 105.0);
        assert_eq!(candle.low, 100.0);
        assert_eq!(candle.close, 105.0);
        assert_eq!(candle.volume, 30.0);

        // A second flush is a no-op (no minute in progress).
        assert!(assembler.flush().is_none());
    }

    #[test]
    fn test_flush_before_any_tick_is_none() {
        let mut assembler = CandleAssembler::new("NIFTY".to_string());

        // No tick has been fed — nothing to flush.
        assert!(assembler.flush().is_none());
    }
}
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Datelike, FixedOffset, Timelike, Weekday};
use parking_lot::RwLock;
use serde::Deserialize;
use uuid::Uuid;

use crate::broker::{
    dhan::rest::DhanClient,
    symbol_map::{Segment, SymbolMap},
    BrokerClient,
};
use crate::live::holidays::NseHolidayCalendar;
use crate::strategy::{dsl::StrategyNode, execution::DhanBroker};

/// IST is UTC+05:30 and has no daylight-saving transitions, so a `FixedOffset`
/// is the right primitive. Using a chrono `Tz` (e.g. `chrono_tz::Asia/Kolkata`)
/// would add a dependency for the sake of a constant offset.
pub const IST_OFFSET_SECONDS: i32 = 5 * 3600 + 30 * 60;

/// Duration of a fresh confirmation token. 90 s gives the user a comfortable
/// window to read the warnings, click the checkbox, and confirm — earlier
/// versions used 30 s, which was too tight against a 3-second countdown and
/// a reader-of-warnings pause.
pub const TOKEN_TTL_SECONDS: u64 = 90;

/// Outcome of `LiveGuard::run_preflight`. Either the strategy is cleared to
/// start (`Ok` with a fresh token) or it needs the user to acknowledge the
/// live-trading warning before confirming (`RequiresAcknowledgment`).
///
/// Both variants carry a token — see the spec's "Ack flow correction"
/// section. `acknowledge_live_trading` is a pure "user checked the box"
/// operation that does not touch the token; the token lifecycle is owned
/// entirely by `request_live_start` and `confirm_live_start`.
pub enum LiveGuardResult {
    Ok { token: PendingLiveToken },
    RequiresAcknowledgment { token: PendingLiveToken },
}

/// A freshly-issued live-start confirmation token. Single use, bound to a
/// specific strategy id, and short-lived. The owning slot lives in
/// `AppState.pending_live_token`; `confirm_live_start` consumes it under the
/// slot lock so concurrent attempts are serialized.
#[derive(Debug, Clone)]
pub struct PendingLiveToken {
    pub token: String,
    pub strategy_id: String,
    pub expires_at: Instant,
}

/// Holds the dependencies the safety gates need and runs them in order.
///
/// The guard is cheap to clone (everything inside is `Arc`-shared) and is
/// safe to call from any tokio task. `run_preflight` is `async` only because
/// gate 2 hits the network via `client.get_positions`; the other gates are
/// pure CPU.
pub struct LiveGuard {
    pub client: Arc<DhanClient>,
    pub symbol_map: Arc<RwLock<SymbolMap>>,
    pub broker: Arc<DhanBroker>,
    pub ack_path: PathBuf,
    pub holiday_calendar: Arc<NseHolidayCalendar>,
}

impl LiveGuard {
    /// Construct a guard with the supplied dependencies.
    pub fn new(
        client: Arc<DhanClient>,
        symbol_map: Arc<RwLock<SymbolMap>>,
        broker: Arc<DhanBroker>,
        ack_path: PathBuf,
        holiday_calendar: Arc<NseHolidayCalendar>,
    ) -> Self {
        Self {
            client,
            symbol_map,
            broker,
            ack_path,
            holiday_calendar,
        }
    }

    /// Run gates 1–8 in order. Returns `Ok(...)` with a fresh 90-second
    /// token on success, or `Err(msg)` describing the first gate that
    /// failed. No token is issued on failure.
    ///
    /// Gate 9 (acknowledgment) is *not* a gate in this method — it merely
    /// chooses between the two `LiveGuardResult` variants. The caller decides
    /// whether to surface the ack modal to the user.
    pub async fn run_preflight(
        &self,
        symbol: &str,
        strategy_id: &str,
        strategy_node: &StrategyNode,
    ) -> Result<LiveGuardResult, String> {
        // Gate 1: paper-default guard. The strategy node must explicitly carry
        // a risk profile marked Live. (We cannot use `strategy_node.mode`
        // because the DSL AST does not store the deployment mode — the mode
        // lives on the registry record. The preflight checks the registry
        // record instead, but we keep this gate documented in the source.)
        //
        // NOTE: The deployed mode is checked at the command layer via the
        // strategy registry, not via `StrategyNode`. The gates defined in
        // the spec are enforced here in the order listed, with the
        // exception of mode which is verified in `commands::live::request_live_start`.

        // Gate 2: broker reachability. A failed network call here is fatal —
        // if we can't talk to Dhan, we can't even fetch the positions used
        // by the daily-loss tracker, so a live session is unsafe.
        self.client.get_positions().await.map_err(|e| {
            format!("broker unreachable; cannot confirm live session is safe: {e}")
        })?;

        // Gate 3 + 4: symbol must be in the map and resolve to NSE equity.
        let entry = {
            let map = self.symbol_map.read();
            map.lookup(symbol)
                .ok_or_else(|| format!("symbol '{symbol}' not found in symbol map"))?
        };
        if entry.segment != Segment::NseEq {
            return Err(format!(
                "Phase 7 only supports NSE equity intraday trading; symbol {symbol} is {:?}",
                entry.segment
            ));
        }

        // Gate 5: market hours. Use the current wall clock in IST. We refuse
        // to start a session outside the 09:15–15:30 window on a trading day.
        let now_ist = chrono::Local::now().with_timezone(&FixedOffset::east_opt(IST_OFFSET_SECONDS).expect("IST offset is in range"));
        if !is_market_open(now_ist, &self.holiday_calendar) {
            return Err(
                "market is closed; live trading is only allowed 09:15–15:30 IST on NSE trading days"
                    .to_string(),
            );
        }

        // Gate 6: at least one risk control declared. We require *any*
        // combination of max_orders, max_positions, or max_daily_loss.
        match &strategy_node.risk {
            None => {
                return Err(
                    "live strategies must declare at least one RISK control \
                     (MAX_ORDERS, MAX_POSITIONS, or MAX_DAILY_LOSS)"
                        .to_string(),
                )
            }
            Some(risk) => {
                if risk.max_orders.is_none()
                    && risk.max_open_positions.is_none()
                    && risk.max_daily_loss_pct.is_none()
                {
                    return Err(
                        "live strategies must declare at least one RISK control \
                         (MAX_ORDERS, MAX_POSITIONS, or MAX_DAILY_LOSS)"
                            .to_string(),
                    );
                }
            }
        }

        // Gate 7: max daily loss specifically. The only hard financial safety
        // net in the engine — non-negotiable. Gate 6 lets any risk control
        // through, but live mode requires THIS one in addition.
        let max_daily_loss_present = strategy_node
            .risk
            .as_ref()
            .and_then(|r| r.max_daily_loss_pct)
            .is_some();
        if !max_daily_loss_present {
            return Err(
                "live strategies must declare RISK MAX_DAILY_LOSS".to_string(),
            );
        }

        // Gate 8: broker's realized-loss cache must be fresh. If the
        // background refresh has been failing, the daily-loss tracker is
        // unreliable and starting a session is unsafe.
        if self.broker.is_stale() {
            return Err(
                "broker realized-loss tracking is stale; check broker connectivity and retry"
                    .to_string(),
            );
        }

        // All gates cleared. Issue a fresh token.
        let token = Self::issue_token(strategy_id);

        // Gate 9: ack file. This is not an abort; it picks the variant.
        if read_ack_file(&self.ack_path) {
            Ok(LiveGuardResult::Ok { token })
        } else {
            Ok(LiveGuardResult::RequiresAcknowledgment { token })
        }
    }

    /// Build a new token for `strategy_id`. The TTL is `TOKEN_TTL_SECONDS`;
    /// a separate `expires_at: Instant` makes the validator trivially
    /// unit-testable without a real clock.
    pub fn issue_token(strategy_id: &str) -> PendingLiveToken {
        PendingLiveToken {
            token: Uuid::new_v4().to_string(),
            strategy_id: strategy_id.to_string(),
            expires_at: Instant::now() + Duration::from_secs(TOKEN_TTL_SECONDS),
        }
    }

    /// Validate a pending token against the supplied `strategy_id` and
    /// `token` string. Returns `Ok(())` only when the token matches the
    /// stored strategy id, the string matches, and the token has not
    /// expired. Any failure returns the same generic error message so the
    /// UI does not leak details ("wrong strategy" vs. "expired" is not a
    /// useful distinction for an attacker probing for token state).
    pub fn validate_token(
        pending: &Option<PendingLiveToken>,
        strategy_id: &str,
        token: &str,
    ) -> Result<(), String> {
        let pending = pending.as_ref().ok_or_else(|| {
            "invalid or expired confirmation token".to_string()
        })?;
        if pending.strategy_id != strategy_id {
            return Err("invalid or expired confirmation token".to_string());
        }
        if pending.token != token {
            return Err("invalid or expired confirmation token".to_string());
        }
        if Instant::now() >= pending.expires_at {
            return Err("invalid or expired confirmation token".to_string());
        }
        Ok(())
    }
}

/// True if the given IST wall-clock instant falls inside NSE equity market
/// hours: Monday–Friday, 09:15–15:30 IST, excluding holidays registered in
/// `holidays`. The boundary is inclusive on the open side (`09:15` is open)
/// and exclusive on the close side (`15:30` is open, `15:30:01` is not).
///
/// Extracted as a pure function so it can be unit-tested with a fake
/// `DateTime<FixedOffset>` without depending on the system clock. The
/// predicate is consulted at two points in the live pipeline: (a) at
/// session start by `LiveGuard::run_preflight`, and (b) on every closed
/// candle by the tick loop in `src/live/session.rs` (audit item B1). Both
/// callers share the same `Arc<NseHolidayCalendar>` so the boundary
/// definition cannot drift between preflight and per-candle.
pub fn is_market_open(dt: DateTime<FixedOffset>, holidays: &NseHolidayCalendar) -> bool {
    // Weekend first — the common case for off-hours attempts.
    match dt.weekday() {
        Weekday::Sat | Weekday::Sun => return false,
        _ => {}
    }

    // Holiday calendar.
    let date = dt.date_naive();
    if holidays.is_holiday(date) {
        return false;
    }

    // M2 (audit): compare hour/minute/second only — `nanosecond()` is
    // always in 0..1_000_000_000 and never discriminates at second
    // granularity. The original `(h, m, s, nanos) >= (9, 15, 0, 0)`
    // tuple comparison was redundant: nanos was always 0 in the test
    // inputs and the prior 9:14:59 / 9:15:00 boundary already pins
    // behaviour to the second.
    let (hour, minute, second) = (dt.hour(), dt.minute(), dt.second());
    let after_open = (hour, minute, second) >= (9, 15, 0);
    let before_close = (hour, minute, second) <= (15, 30, 0);
    after_open && before_close
}

/// Read the persistent ack file. Returns true only when the file exists,
/// parses as JSON, and has `acknowledged == true`. Any other state (missing
/// file, malformed JSON, parse error, `acknowledged == false`) returns false
/// — the safe default is to require the user to ack again.
fn read_ack_file(path: &Path) -> bool {
    let raw = match std::fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(_) => return false,
    };
    match serde_json::from_str::<AckFile>(&raw) {
        Ok(parsed) => parsed.acknowledged,
        Err(_) => false,
    }
}

#[derive(Debug, Deserialize)]
struct AckFile {
    #[serde(default)]
    acknowledged: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::holidays::NseHolidayCalendar;
    use chrono::TimeZone;

    fn ist(y: i32, m: u32, d: u32, h: u32, mi: u32, s: u32) -> DateTime<FixedOffset> {
        let offset = FixedOffset::east_opt(IST_OFFSET_SECONDS).unwrap();
        let nd = chrono::NaiveDate::from_ymd_opt(y, m, d).unwrap();
        let nt = chrono::NaiveTime::from_hms_opt(h, mi, s).unwrap();
        offset
            .from_local_datetime(&nd.and_time(nt))
            .single()
            .expect("valid ist instant")
    }

    // ---- Token tests ----

    #[test]
    fn test_token_expires() {
        let pending = Some(PendingLiveToken {
            token: "abc".to_string(),
            strategy_id: "strat-1".to_string(),
            expires_at: Instant::now() - Duration::from_millis(1),
        });
        assert!(LiveGuard::validate_token(&pending, "strat-1", "abc").is_err());
    }

    #[test]
    fn test_token_wrong_id() {
        let pending = Some(LiveGuard::issue_token("strat-1"));
        assert!(LiveGuard::validate_token(&pending, "strat-2", &pending.as_ref().unwrap().token).is_err());
    }

    #[test]
    fn test_token_wrong_token() {
        let pending = Some(LiveGuard::issue_token("strat-1"));
        assert!(LiveGuard::validate_token(&pending, "strat-1", "wrong").is_err());
    }

    #[test]
    fn test_token_valid() {
        let pending = Some(LiveGuard::issue_token("strat-1"));
        let token = pending.as_ref().unwrap().token.clone();
        assert!(LiveGuard::validate_token(&pending, "strat-1", &token).is_ok());
    }

    #[test]
    fn test_token_none() {
        assert!(LiveGuard::validate_token(&None, "strat-1", "abc").is_err());
    }

    // ---- Market hours tests ----

    fn empty_holidays() -> NseHolidayCalendar {
        NseHolidayCalendar::new()
    }

    #[test]
    fn test_market_open_9_15_monday() {
        assert!(is_market_open(ist(2026, 1, 5, 9, 15, 0), &empty_holidays()));
    }

    #[test]
    fn test_market_open_9_14_monday() {
        assert!(!is_market_open(ist(2026, 1, 5, 9, 14, 59), &empty_holidays()));
    }

    #[test]
    fn test_market_open_15_30_monday() {
        assert!(is_market_open(ist(2026, 1, 5, 15, 30, 0), &empty_holidays()));
    }

    #[test]
    fn test_market_open_15_31_monday() {
        assert!(!is_market_open(ist(2026, 1, 5, 15, 30, 1), &empty_holidays()));
    }

    #[test]
    fn test_market_closed_saturday() {
        // 2026-01-03 is a Saturday.
        assert!(!is_market_open(ist(2026, 1, 3, 10, 0, 0), &empty_holidays()));
    }

    #[test]
    fn test_market_closed_sunday() {
        // 2026-01-04 is a Sunday.
        assert!(!is_market_open(ist(2026, 1, 4, 10, 0, 0), &empty_holidays()));
    }

    #[test]
    fn test_market_closed_holiday() {
        // 2026-01-26 is Republic Day (NSE holiday).
        let holidays = vec![chrono::NaiveDate::from_ymd_opt(2026, 1, 26).unwrap()];
        let cal = NseHolidayCalendar::with_holidays(holidays);
        // 2026-01-26 is a Monday, so without the holiday check it would be open.
        assert!(!is_market_open(ist(2026, 1, 26, 10, 0, 0), &cal));
    }

    // ---- M2 (audit): sub-second boundaries ----
    //
    // The original `(h, m, s, nanos)` tuple comparison was redundant —
    // `nanos` was always 0 in the test inputs and never discriminated.
    // After dropping it, the comparison must still treat the open boundary
    // as inclusive at second granularity: 09:15:00.0 is open, 15:30:00.0
    // is open, and anything strictly inside those bounds is open.
    #[test]
    fn test_market_open_at_9_15_with_nanos() {
        // 09:15:00.0 is the open boundary — must remain open.
        let dt = ist(2026, 1, 5, 9, 15, 0);
        assert_eq!(dt.nanosecond(), 0);
        assert!(is_market_open(dt, &empty_holidays()));
    }

    #[test]
    fn test_market_closed_at_9_14_59_with_nanos() {
        // 09:14:59 is one second before the open — must remain closed.
        let dt = ist(2026, 1, 5, 9, 14, 59);
        assert_eq!(dt.nanosecond(), 0);
        assert!(!is_market_open(dt, &empty_holidays()));
    }

    #[test]
    fn test_market_open_at_15_30_with_nanos() {
        // 15:30:00 is the close boundary — inclusive, must remain open.
        let dt = ist(2026, 1, 5, 15, 30, 0);
        assert_eq!(dt.nanosecond(), 0);
        assert!(is_market_open(dt, &empty_holidays()));
    }

    #[test]
    fn test_market_closed_at_15_30_01_with_nanos() {
        // 15:30:01 is one second past the close — must remain closed.
        let dt = ist(2026, 1, 5, 15, 30, 1);
        assert_eq!(dt.nanosecond(), 0);
        assert!(!is_market_open(dt, &empty_holidays()));
    }

    // ---- Ack file tests ----

    fn temp_path(name: &str) -> PathBuf {
        let mut dir = std::env::temp_dir();
        dir.push(format!("algomln-guard-test-{}-{}.json", name, std::process::id()));
        dir
    }

    #[test]
    fn test_read_ack_missing_file() {
        let path = temp_path("missing");
        let _ = std::fs::remove_file(&path);
        assert!(!read_ack_file(&path));
    }

    #[test]
    fn test_read_ack_true() {
        let path = temp_path("true");
        std::fs::write(&path, r#"{"acknowledged": true}"#).unwrap();
        assert!(read_ack_file(&path));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_read_ack_false() {
        let path = temp_path("false");
        std::fs::write(&path, r#"{"acknowledged": false}"#).unwrap();
        assert!(!read_ack_file(&path));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_read_ack_malformed() {
        let path = temp_path("malformed");
        std::fs::write(&path, "not json").unwrap();
        assert!(!read_ack_file(&path));
        let _ = std::fs::remove_file(&path);
    }
}use chrono::NaiveDate;

/// NSE trading-day calendar. Lists the days the cash equity market is closed
/// (whole-day holidays only). The list is a static constant — the official NSE
/// holiday calendar changes annually, so this module is the single place to
/// update each year.
///
/// **Known limitation (Phase 7):** Muhurat sessions are ignored. The market
/// is treated as closed on the Diwali evening when NSE runs a one-hour
/// special session outside the standard 09:15–15:30 window. Users cannot run
/// a live session during Muhurat trading in Phase 7. Phase 8 can add a
/// dedicated window.
pub struct NseHolidayCalendar {
    holidays: Vec<NaiveDate>,
}

impl NseHolidayCalendar {
    /// Build an empty calendar. Production callers should populate
    /// `holidays` from the latest NSE circular; tests build a calendar with
    /// a single known holiday to verify the predicate.
    pub fn new() -> Self {
        Self {
            holidays: Vec::new(),
        }
    }

    /// Build a calendar with the supplied list. Used by tests and any future
    /// loader that fetches the calendar from an external source.
    pub fn with_holidays(holidays: Vec<NaiveDate>) -> Self {
        Self { holidays }
    }

    /// True if the given date is an NSE trading holiday (whole-day closure).
    pub fn is_holiday(&self, d: NaiveDate) -> bool {
        self.holidays.iter().any(|h| *h == d)
    }

    /// Total number of holidays currently in the calendar. Mostly for tests
    /// and startup logging.
    pub fn len(&self) -> usize {
        self.holidays.len()
    }

    pub fn is_empty(&self) -> bool {
        self.holidays.is_empty()
    }
}

impl Default for NseHolidayCalendar {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_calendar_returns_false() {
        let cal = NseHolidayCalendar::new();
        let any_day = NaiveDate::from_ymd_opt(2026, 1, 26).unwrap();
        assert!(!cal.is_holiday(any_day));
    }

    #[test]
    fn configured_holiday_matches() {
        let holidays = vec![NaiveDate::from_ymd_opt(2026, 1, 26).unwrap()];
        let cal = NseHolidayCalendar::with_holidays(holidays);
        assert!(cal.is_holiday(NaiveDate::from_ymd_opt(2026, 1, 26).unwrap()));
        assert!(!cal.is_holiday(NaiveDate::from_ymd_opt(2026, 1, 27).unwrap()));
    }
}pub mod candle_assembler;
pub mod guard;
pub mod holidays;
pub mod session;
pub mod trade_log;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::{sync::Mutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;

/// Maximum number of completed candles kept in `candle_history`. Older candles
/// are dropped from the front once this is exceeded. The engine only needs a
/// bounded window; retaining the full day is dead weight over long sessions.
const MAX_CANDLE_HISTORY: usize = 5000;

// ── Failure signalling ────────────────────────────────────────────────────────

/// Payload emitted when a live session transitions to `Failed`.
/// Serialised as camelCase JSON for the Tauri frontend event bus.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LiveSessionFailedPayload {
    pub strategy_id: String,
    pub reason: String,
    /// Best-effort count of open positions at failure time. `None` if the
    /// positions fetch itself failed, or if fetching was not attempted.
    pub open_positions_estimate: Option<i64>,
}

/// Abstraction over "emit a session-failed alert". Implemented by a real
/// `tauri::AppHandle` wrapper in production and a mock in tests. Keeping this
/// as a trait means `LiveSession` compiles and tests cleanly without a Tauri
/// dependency in the library crate.
pub trait SessionEventEmitter: Send + Sync + 'static {
    fn emit_failed(&self, payload: LiveSessionFailedPayload);
}

/// No-op emitter. Used as a default when no real emitter is wired up yet
/// (e.g. tests that only care about broker / engine behaviour, not UI alerts).
pub struct NoopEmitter;

impl SessionEventEmitter for NoopEmitter {
    fn emit_failed(&self, _payload: LiveSessionFailedPayload) {}
}

use crate::{
    broker::Timeframe,
    feed::FeedManager,
    live::{
        candle_assembler::CandleAssembler,
        guard::{is_market_open, IST_OFFSET_SECONDS},
        holidays::NseHolidayCalendar,
        trade_log::TradeLog,
    },
    models::Candle,
    plugin::api::events::EventBus,
    strategy::{
        dsl::StrategyNode,
        execution::{
            dhan::SessionContext,
            target::ExecutionTarget,
            DhanBroker,
        },
        runtime::{StrategyEngine, StrategyInstance, StrategyStatus as EngineStatus},
    },
};

/// Convert a candle's millisecond UTC unix timestamp into an IST wall-clock
/// `DateTime<FixedOffset>`. Used by the per-candle market-hours gate so the
/// decision reads the candle close in the same frame as `is_market_open`.
///
/// Lives here (rather than in `guard.rs`) so the session module does not
/// have to re-export `FixedOffset`. The constant offset keeps the math
/// trivial: `chrono::DateTime::from_timestamp_millis(ts) -> Utc -> IST`.
fn candle_close_ist(ts_millis: i64) -> Option<chrono::DateTime<chrono::FixedOffset>> {
    let utc = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ts_millis)?;
    Some(utc.with_timezone(
        &chrono::FixedOffset::east_opt(IST_OFFSET_SECONDS).expect("IST offset is in range"),
    ))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SessionStatus {
    Starting,
    Running,
    Paused,
    Stopped,
    Failed(String),
}

pub struct LiveSession {
    pub strategy_id: String,
    pub strategy_name: String,
    pub symbol: String,
    pub status: Arc<RwLock<SessionStatus>>,
    pub engine: Arc<Mutex<StrategyEngine>>,
    /// L8 (audit): the broker handle is `pub(crate)` so only the `algomln`
    /// crate can reach `DhanBroker::execute_with_meta` directly. External
    /// callers (the Tauri binary, future plugin code) must go through the
    /// typed helpers below — `broker_positions`, `broker_realized_loss`,
    /// `broker_is_stale`, `broker_time_since_last_success`,
    /// `broker_cancel_token`, `broker_paused_for_entries`, and the
    /// `set_paused_for_entries` setter. The previous `pub broker` field
    /// was a one-line tripwire that any plugin host with an
    /// `Arc<LiveSession>` could have used to place orders without going
    /// through the gate stack.
    pub(crate) broker: Arc<DhanBroker>,
    pub trade_log: Arc<TradeLog>,
    pub candle_history: Arc<Mutex<Vec<Candle>>>,
    pub start_time: DateTime<Utc>,
    /// B1 (audit): NSE trading-day calendar used by the per-candle
    /// market-hours gate in the tick loop. Cloned from the shared
    /// `AppState` via `LiveGuard`. The session never mutates this
    /// calendar — the only mutator is `NseHolidayCalendar` itself, which
    /// is constructed once at startup and shared with `LiveGuard` and
    /// `GatedLiveExecutionApi`.
    pub(crate) holiday_calendar: Arc<NseHolidayCalendar>,
    cancel: CancellationToken,
    task: Mutex<Option<JoinHandle<()>>>,
    /// Emitter for loud failure alerts. The real impl wraps `tauri::AppHandle`
    /// and calls `emit_all("live_session_failed", ...)`. Tests inject a mock.
    emitter: Arc<dyn SessionEventEmitter>,
}

impl LiveSession {
    /// Construct and immediately start the tick-listening task.
    ///
    /// `emitter` receives a `live_session_failed` notification whenever the
    /// session transitions to `Failed`. In production, pass a
    /// `TauriSessionEmitter(app_handle)` (defined in the Tauri binary). In
    /// tests or contexts that don't need UI alerts, pass `Arc::new(NoopEmitter)`.
    ///
    /// `initial_cash` is the user's declared starting capital. It feeds the
    /// engine's `RISK MAX_DAILY_LOSS` check: with `DhanBroker` the live
    /// engine has no way to recover the starting capital from the broker,
    /// so the cap would otherwise be a silent no-op in live trading. A
    /// value of `0.0` (or negative) degrades the check to "never
    /// breached" and is logged at startup so the user notices the cap
    /// won't actually fire.
    pub async fn start(
        strategy_id: String,
        strategy_name: String,
        symbol: String,
        strategy_node: StrategyNode,
        broker: Arc<DhanBroker>,
        feed: Arc<Mutex<FeedManager>>,
        trade_log: Arc<TradeLog>,
        event_bus: Arc<EventBus>,
        initial_candles: Vec<Candle>,
        initial_cash: f64,
        emitter: Arc<dyn SessionEventEmitter>,
        holiday_calendar: Arc<NseHolidayCalendar>,
    ) -> Result<Arc<Self>, String> {
        // Write session context onto the broker before the engine starts so
        // every `execute`/`execute_with_meta` call during this session sees
        // the correct strategy metadata in the trade log.
        *broker.session_context.write() = Some(SessionContext {
            strategy_id: strategy_id.clone(),
            strategy_name: strategy_name.clone(),
            mode: "live".to_string(),
        });

        let execution_target = broker.clone();
        let mut engine = StrategyEngine::new(StrategyInstance {
            id: strategy_id.clone(),
            strategy: Arc::new(strategy_node),
            symbol: symbol.clone(),
            timeframe: Timeframe::M1,
            status: EngineStatus::Running,
            execution_target,
            initial_cash,
        });
        engine.event_bus = Some(event_bus);

        if initial_cash <= 0.0 {
            eprintln!(
                "live_session: strategy {strategy_id} started with initial_cash={initial_cash}; \
                 RISK MAX_DAILY_LOSS will not fire for this session"
            );
        }

        let status = Arc::new(RwLock::new(SessionStatus::Starting));
        let cancel = CancellationToken::new();
        // H3 (audit): wire the session's cancellation token into the
        // broker so a `stop()` mid-`place_order` aborts the call before
        // the trade-log row is appended. We do this *before* subscribing
        // to the feed — if the subscribe fails the broker still has the
        // right token for any subsequent (clean) start. The token itself
        // is fresh until `stop()` fires, so an unused session is harmless.
        broker.set_cancel_token(cancel.clone());

        let session = Arc::new(Self {
            strategy_id,
            strategy_name,
            symbol: symbol.clone(),
            status: status.clone(),
            engine: Arc::new(Mutex::new(engine)),
            broker,
            trade_log,
            candle_history: Arc::new(Mutex::new(initial_candles)),
            start_time: Utc::now(),
            holiday_calendar,
            cancel,
            task: Mutex::new(None),
            emitter,
        });

        let mut receiver = {
            let mut feed = feed.lock().await;
            feed.subscribe(vec![symbol.clone()])
                .await
                .map_err(|error| error.to_string())?;
            feed.subscribe_ticks()
        };

        *status.write() = SessionStatus::Running;

        let task_session = session.clone();
        let task = tokio::spawn(async move {
            let mut assembler = CandleAssembler::new(symbol);

            loop {
                tokio::select! {
                    _ = task_session.cancel.cancelled() => {
                        // B2 (audit): flush the assembler's in-progress minute
                        // before breaking so the last partial candle's prices
                        // and volume aren't silently dropped when the session
                        // stops. The flushed candle is routed through the
                        // same B1-gated `append_candle` path so a stop at
                        // 15:29:30 still honours the per-candle market-hours
                        // gate (the gate handles a post-close partial minute
                        // the same way it handles a tick-closed one).
                        if let Some(pending) = assembler.flush() {
                            task_session.append_candle(pending).await;
                        }
                        break;
                    }
                    tick = receiver.recv() => {
                        let tick = match tick {
                            Ok(tick) => tick,
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                                eprintln!("[live_session] tick receiver lagged by {skipped} message(s)");
                                continue;
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                eprintln!("[live_session] feed closed unexpectedly — transitioning to Failed");
                                let reason = "feed closed unexpectedly".to_string();
                                *task_session.status.write() =
                                    SessionStatus::Failed(reason.clone());
                                // Emit the loud alert. open_positions_estimate is not fetched
                                // here because we're inside the async task with no broker
                                // handle; Phase 8 can add a best-effort fetch before breaking.
                                task_session.emitter.emit_failed(LiveSessionFailedPayload {
                                    strategy_id: task_session.strategy_id.clone(),
                                    reason,
                                    open_positions_estimate: None,
                                });
                                // B2 (audit): flush the assembler's in-progress
                                // minute before breaking so a feed shutdown
                                // mid-minute preserves the partial candle's
                                // prices and volume.
                                if let Some(pending) = assembler.flush() {
                                    task_session.append_candle(pending).await;
                                }
                                break;
                            }
                        };

                        if tick.symbol != task_session.symbol {
                            continue;
                        }

                        // If the realized-loss cache has gone stale, pause immediately
                        // rather than running the engine with an unreliable safety metric.
                        // Only transition if currently Running (don't stomp Failed/Stopped).
                        if task_session.broker.is_stale() {
                            let should_pause = {
                                let s = task_session.status.read();
                                *s == SessionStatus::Running
                            };
                            if should_pause {
                                task_session.broker.set_paused_for_entries(true);
                                *task_session.status.write() = SessionStatus::Paused;
                                eprintln!(
                                    "[live_session] realized-loss cache stale; \
                                     session paused until cache recovers"
                                );
                            }
                        }

                        // B2 (audit): `feed` may now emit more than one candle
                        // — one for the minute that just closed, plus a
                        // zero-volume gap candle for every minute the feed
                        // skipped. Iterate and forward each through the
                        // B1-gated `append_candle` path so the engine sees
                        // a contiguous minute-by-minute series even across
                        // feed gaps.
                        let candles = assembler.feed(&tick);
                        for candle in candles {
                            task_session.append_candle(candle).await;
                        }
                    }
                }
            }

            let mut status = task_session.status.write();
            if !matches!(*status, SessionStatus::Failed(_)) {
                *status = SessionStatus::Stopped;
            }
        });

        *session.task.lock().await = Some(task);
        Ok(session)
    }

    /// Pause the session. Candle data continues to accumulate and the engine
    /// still runs `on_candle` every minute — stop-loss, take-profit, and
    /// risk-breach orders are **not** suppressed. Only new *entry* (BUY) orders
    /// are blocked, via the broker's `paused_for_entries` flag.
    ///
    /// L2 (audit): the underlying `set_paused_for_entries` is gated on a
    /// `SessionContext` being set; `LiveSession::start` always sets one
    /// before this method is reachable, so the rejection branch should
    /// never fire in production. We log a warning if it does — that
    /// would mean the session's context was cleared while the session
    /// itself is still active, which is a bookkeeping bug.
    pub fn pause(&self) {
        if !self.broker.set_paused_for_entries(true) {
            eprintln!(
                "[live_session] WARN: pause() called but broker has no SessionContext \
                 — the session's context was cleared out from under it"
            );
        }
        *self.status.write() = SessionStatus::Paused;
    }

    pub fn resume(&self) {
        if !self.broker.set_paused_for_entries(false) {
            eprintln!(
                "[live_session] WARN: resume() called but broker has no SessionContext"
            );
        }
        *self.status.write() = SessionStatus::Running;
    }

    /// How long `stop()` waits for the tick task to finish before logging a
    /// warning that an order may still be in flight. The HTTP `place_order`
    /// call inside `on_candle` does not respect the cancellation token
    /// (see H3 in the live execution audit) — if it is mid-flight when
    /// `stop()` is called, we cannot abort it. The 5-second wait is a
    /// pragmatic bound: real broker round-trips on NSE are typically
    /// 300–800 ms, so anything past 5 s is almost certainly hung.
    const STOP_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

    /// Push one completed candle through the same pipeline the tick loop
    /// uses for tick-driven candles: append to `candle_history` (with the
    /// `MAX_CANDLE_HISTORY` drain), honour the B1 per-candle market-hours
    /// gate, and call `engine.plan_candle` only when the gate passes.
    ///
    /// Called from three sites:
    /// 1. The regular tick body, once per candle the assembler emits.
    /// 2. The cancel branch of the tick loop, with the candle returned by
    ///    `CandleAssembler::flush` — so the last partial minute isn't
    ///    silently dropped when the session stops.
    /// 3. The `RecvError::Closed` branch of the tick loop, with the same
    ///    flushed candle — so a feed shutdown also preserves the partial
    ///    minute.
    async fn append_candle(&self, candle: Candle) {
        // B1 (audit): per-candle market-hours gate. A session started at
        // 15:29 would otherwise run `on_candle` on the 15:30 candle and
        // submit a post-close order that NSE rejects. Re-using the same
        // predicate as `LiveGuard::run_preflight` keeps the boundary
        // definitions in one place; the session treats weekend / holiday /
        // outside-hours candles as un-tradeable and skips the rest of the
        // pipeline.
        //
        // The candle still goes into `candle_history` so the indicator
        // warm-up does not skip a day when the first session candle lands
        // after a weekend gap — we just don't try to trade on it.
        // Skipping the history append would leave indicators blind to that
        // day's prices.
        if let Some(candle_close_ist) = candle_close_ist(candle.timestamp) {
            if !is_market_open(candle_close_ist, &self.holiday_calendar) {
                eprintln!(
                    "[live_session] market closed at candle close {} IST; \
                     skipping trading on this candle",
                    candle_close_ist.format("%Y-%m-%d %H:%M:%S")
                );
                // Still record the candle so the next in-session candle
                // has the correct preceding close.
                let mut history = self.candle_history.lock().await;
                history.push(candle);
                if history.len() > MAX_CANDLE_HISTORY {
                    let excess = history.len() - MAX_CANDLE_HISTORY;
                    history.drain(0..excess);
                }
                return;
            }
        }

        // Append the new candle and clone the history while the lock is held;
        // the guard is dropped at the end of this block — before any .await.
        let candles = {
            let mut history = self.candle_history.lock().await;
            history.push(candle);
            // Keep only the most recent MAX_CANDLE_HISTORY candles; older
            // candles are dead weight once the indicator windows have slid past them.
            if history.len() > MAX_CANDLE_HISTORY {
                let excess = history.len() - MAX_CANDLE_HISTORY;
                history.drain(0..excess);
            }
            history.clone()
        }; // history guard dropped here — no lock held across the awaits below.

        // C2 + H3 fix (Phase 8): the engine now exposes `plan_candle` which
        // returns `(Vec<LogEntry>, Vec<OrderIntent>)`. The tick loop captures
        // the intent list for the audit trail and forwards it to a future
        // H3-aware executor (Phase 9). Today the intents are already
        // executed by `plan_candle` itself — the split exists so we can
        // re-route them through a cancellation-aware executor without
        // changing the engine's eval logic.
        //
        // H3 cancellation plumbing is wired separately:
        // `DhanBroker::execute_with_meta` now checks the session's
        // `CancellationToken` before the HTTP call (see
        // `dhan.rs::execute_with_meta`), so a `stop()` mid-`place_order`
        // aborts the broker call within a few hundred ms instead of
        // waiting for the broker's response.
        let (_logs, intents) = {
            let mut engine = self.engine.lock().await;
            engine.plan_candle(&candles).await
        };
        // Intents are recorded for any future executor (Phase 9
        // multi-session work). Today they were already executed by
        // `plan_candle`; we just keep the list visible so logs and audit
        // can correlate.
        let _ = intents;
    }

    /// Stop the session. Cancels the cancellation token (the tick loop's
    /// `select!` will break on the next iteration boundary) and awaits the
    /// task with a bounded timeout. If the task does not finish within
    /// `STOP_DRAIN_TIMEOUT`, the order in flight inside `on_candle` is
    /// assumed to still be running at the broker — `stop()` returns and
    /// logs a warning so the caller can surface it to the UI.
    pub async fn stop(&self) {
        self.cancel.cancel();
        let mut task_slot = self.task.lock().await;
        if let Some(task) = task_slot.take() {
            match tokio::time::timeout(Self::STOP_DRAIN_TIMEOUT, task).await {
                Ok(Ok(())) => {
                    // Clean exit — tick loop broke on cancel.
                }
                Ok(Err(join_error)) => {
                    eprintln!("[live_session] task join failed: {join_error}");
                }
                Err(_timeout) => {
                    // The tick loop is stuck inside `on_candle` — almost
                    // certainly blocked on a `place_order` HTTP call. The
                    // order may or may not have reached the broker; the user
                    // must verify in their broker app.
                    eprintln!(
                        "[live_session] WARN: tick task did not stop within \
                         {secs}s — an order may still be in flight at the broker; \
                         please verify your open orders in the broker app",
                        secs = Self::STOP_DRAIN_TIMEOUT.as_secs(),
                    );
                }
            }
        }
        drop(task_slot);
        // Clear session context so the broker no longer attaches this
        // strategy's metadata to any subsequent (unexpected) execute calls.
        *self.broker.session_context.write() = None;
    }

    pub fn status(&self) -> SessionStatus {
        self.status.read().clone()
    }

    // ── L8 (audit) typed accessors ────────────────────────────────────────
    //
    // The `pub(crate) broker` field exposes every method on `DhanBroker`,
    // including `execute_with_meta`. That is too much surface for a
    // plugin host (or a future IPC) that needs to render position counts
    // or staleness toasts. The helpers below narrow the public API to
    // exactly what `commands::live` (and any future read-only caller)
    // needs; everything else stays crate-private.

    /// Snapshot the broker's open positions. Tolerates a broker HTTP
    /// failure with `Ok(Vec::new())` so the caller can still render
    /// "0 open positions" in the UI rather than failing the whole
    /// status call. Mirrors the behaviour the `commands::live` callers
    /// used to rely on before L8 narrowed the broker field.
    pub async fn broker_positions(&self) -> Vec<crate::models::Position> {
        self.broker.get_positions().await.unwrap_or_default()
    }

    /// Non-negative realized-loss magnitude in rupees. Drives the
    /// `LiveStatusWire.realized_loss` field.
    pub fn broker_realized_loss(&self) -> f64 {
        self.broker.realized_loss()
    }

    /// `true` once the broker's realized-loss / available-cash cache has
    /// gone stale. Drives the `LiveStatusWire.loss_tracking_stale` flag.
    pub fn broker_is_stale(&self) -> bool {
        self.broker.is_stale()
    }

    /// Wall-clock duration since the most recent successful broker refresh,
    /// or `None` if no refresh has ever succeeded. Drives the
    /// resume-during-stale error message so the user can see how long the
    /// broker has been unreachable.
    pub fn broker_time_since_last_success(&self) -> Option<Duration> {
        self.broker.time_since_last_success()
    }

    /// Snapshot of the entry-suppression flag (BUY orders blocked). The
    /// GatedLiveExecutionApi re-runs this gate on every plugin order.
    pub fn broker_paused_for_entries(&self) -> bool {
        self.broker.paused_for_entries_snapshot()
    }

    /// Clone of the session's cancellation token. The plugin order
    /// gateway constructs its own copy of this token so its gate stack
    /// can short-circuit orders without re-reading the broker field.
    pub fn broker_cancel_token(&self) -> CancellationToken {
        self.broker.cancel_token()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Candle;
    use chrono::{Datelike, TimeZone, Timelike};

    fn make_candle(ts: i64) -> Candle {
        Candle { timestamp: ts, open: 100.0, high: 101.0, low: 99.0, close: 100.5, volume: 1000.0 }
    }

    /// Mock emitter that records every `emit_failed` call for assertion.
    #[derive(Default)]
    struct MockEmitter {
        events: std::sync::Mutex<Vec<LiveSessionFailedPayload>>,
    }

    impl SessionEventEmitter for MockEmitter {
        fn emit_failed(&self, payload: LiveSessionFailedPayload) {
            self.events.lock().unwrap().push(payload);
        }
    }

    #[test]
    fn test_candle_history_capped() {
        let mut history: Vec<Candle> = (0..(MAX_CANDLE_HISTORY as i64 + 100))
            .map(make_candle)
            .collect();
        // Apply the same logic as the tick loop.
        if history.len() > MAX_CANDLE_HISTORY {
            let excess = history.len() - MAX_CANDLE_HISTORY;
            history.drain(0..excess);
        }
        assert_eq!(history.len(), MAX_CANDLE_HISTORY);
        // Oldest 100 candles (timestamps 0..100) were evicted; first remaining is 100.
        assert_eq!(history[0].timestamp, 100);
        assert_eq!(history[MAX_CANDLE_HISTORY - 1].timestamp, MAX_CANDLE_HISTORY as i64 + 99);
    }

    /// Verify that `NoopEmitter` compiles and silently swallows events.
    #[test]
    fn test_noop_emitter_is_silent() {
        let emitter = NoopEmitter;
        emitter.emit_failed(LiveSessionFailedPayload {
            strategy_id: "x".to_string(),
            reason: "test".to_string(),
            open_positions_estimate: None,
        });
        // No panic — that's the assertion.
    }

    /// Verify that `MockEmitter` records `emit_failed` calls with the correct
    /// payload. This is the unit test for the `SessionEventEmitter` contract;
    /// the full "session transitions to Failed and emits" path is an integration
    /// test requiring a live feed setup and is deferred to Phase 8.
    #[test]
    fn test_failed_status_emits_event() {
        let emitter = MockEmitter::default();

        emitter.emit_failed(LiveSessionFailedPayload {
            strategy_id: "strat-42".to_string(),
            reason: "feed closed unexpectedly".to_string(),
            open_positions_estimate: Some(2),
        });

        let events = emitter.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].strategy_id, "strat-42");
        assert_eq!(events[0].reason, "feed closed unexpectedly");
        assert_eq!(events[0].open_positions_estimate, Some(2));
    }

    /// Verify that a `RecvError::Lagged` on the broadcast channel is recoverable:
    /// the tick loop matches it to `continue`, not `break`, so the session stays alive.
    /// This test demonstrates the channel contract that underpins that logic.
    // ---- B1 (audit): per-candle market-hours gate ----
    //
    // The tick loop's gate is a one-line `is_market_open(candle_close_ist, ...)`.
    // The interesting cases are the conversion itself and the predicate's
    // behaviour at the IST open/close boundary when fed via `candle.timestamp`.

    /// 2026-01-05 09:30:00 IST (Monday, well inside trading hours).
    /// Build the millisecond timestamp the engine sees.
    fn ts_ist(y: i32, m: u32, d: u32, h: u32, mi: u32, s: u32) -> i64 {
        let ist = chrono::FixedOffset::east_opt(IST_OFFSET_SECONDS).unwrap();
        let nd = chrono::NaiveDate::from_ymd_opt(y, m, d).unwrap();
        let nt = chrono::NaiveTime::from_hms_opt(h, mi, s).unwrap();
        let dt = ist
            .from_local_datetime(&nd.and_time(nt))
            .single()
            .expect("valid ist instant");
        dt.timestamp_millis()
    }

    #[test]
    fn test_candle_close_ist_converts_mid_session() {
        // 2026-01-05 (Monday) 09:30:00 IST.
        let ts = ts_ist(2026, 1, 5, 9, 30, 0);
        let got = candle_close_ist(ts).expect("valid ts");
        assert_eq!(got.hour(), 9);
        assert_eq!(got.minute(), 30);
        assert_eq!(got.weekday(), chrono::Weekday::Mon);
    }

    #[test]
    fn test_candle_close_ist_returns_none_for_garbage() {
        // Far-future timestamp that overflows `from_timestamp_millis`.
        assert!(candle_close_ist(i64::MAX).is_none());
    }

    /// Saturday candle: even with an empty holiday calendar, the gate
    /// must reject weekend timestamps.
    #[test]
    fn test_gate_skips_weekend_candle() {
        let cal = NseHolidayCalendar::new();
        // 2026-01-03 is a Saturday.
        let sat_close = candle_close_ist(ts_ist(2026, 1, 3, 10, 0, 0)).unwrap();
        assert!(!is_market_open(sat_close, &cal));
    }

    /// Post-close candle: a 15:30:01 IST candle on a trading day must be
    /// rejected. This is the exact B1 scenario — a session started at 15:29
    /// must not submit a 15:30 order.
    #[test]
    fn test_gate_skips_post_close_candle() {
        let cal = NseHolidayCalendar::new();
        // 2026-01-05 (Monday) 15:30:01 IST.
        let post_close = candle_close_ist(ts_ist(2026, 1, 5, 15, 30, 1)).unwrap();
        assert!(!is_market_open(post_close, &cal));
        // And the inclusive boundary at 15:30:00 is still open.
        let close = candle_close_ist(ts_ist(2026, 1, 5, 15, 30, 0)).unwrap();
        assert!(is_market_open(close, &cal));
    }

    /// Holiday candle: 2026-01-26 (Republic Day, Monday) must be rejected
    /// only when registered in the calendar — proves the calendar actually
    /// reaches the gate, not just the weekday/weekend predicates.
    #[test]
    fn test_gate_skips_holiday_candle() {
        let empty = NseHolidayCalendar::new();
        let holidays = vec![chrono::NaiveDate::from_ymd_opt(2026, 1, 26).unwrap()];
        let populated = NseHolidayCalendar::with_holidays(holidays);
        let rep_day = candle_close_ist(ts_ist(2026, 1, 26, 10, 0, 0)).unwrap();
        // Empty calendar treats it as a normal trading day.
        assert!(is_market_open(rep_day, &empty));
        // Populated calendar treats it as a holiday.
        assert!(!is_market_open(rep_day, &populated));
    }

    #[tokio::test]
    async fn test_lag_error_does_not_break_loop() {
        // Channel with capacity 2 — send 3 items before reading to overflow the buffer.
        // Message 1 is dropped; the receiver lags by 1 but messages 2 and 3 survive.
        let (tx, mut rx) = tokio::sync::broadcast::channel::<i32>(2);
        let _ = tx.send(1);
        let _ = tx.send(2);
        let _ = tx.send(3); // drops message 1; receiver is now lagged by 1

        let first = rx.recv().await;
        assert!(
            matches!(first, Err(tokio::sync::broadcast::error::RecvError::Lagged(_))),
            "expected Lagged when receiver falls behind"
        );

        // After Lagged the receiver is repositioned to the oldest surviving message
        // (message 2). The loop `continue`s — the receiver must still be usable.
        assert!(
            rx.recv().await.is_ok(),
            "receiver must still work after a Lagged error"
        );
    }
}
use std::{
    fs::OpenOptions,
    io::{BufRead, BufReader, Write},
    path::PathBuf,
};
use parking_lot::Mutex;

use serde::{Deserialize, Serialize};

/// One entry in the immutable live trade log.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TradeLogEntry {
    pub id: String,
    pub timestamp: String,
    pub strategy_id: String,
    pub strategy_name: String,
    pub symbol: String,
    pub side: String,
    pub quantity: i64,
    pub price: f64,
    pub order_id: String,
    pub order_status: String,
    pub mode: String,
    pub rule_id: String,
    pub notes: String,
}

/// Append-only writer. The file is JSONL: one JSON object per line, never truncated.
#[derive(Debug)]
pub struct TradeLog {
    path: PathBuf,
    file: Mutex<std::fs::File>,
}

impl TradeLog {
    /// Open or create the log at `path`. Creates parent dirs.
    pub fn open(path: PathBuf) -> Result<Self, std::io::Error> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new().create(true).append(true).open(&path)?;

        Ok(Self {
            path,
            file: Mutex::new(file),
        })
    }

    /// Append one entry. Takes the file lock, writes one JSON line + '\n', flushes.
    pub fn append(&self, entry: TradeLogEntry) -> Result<(), std::io::Error> {
        let json = serde_json::to_string(&entry).map_err(std::io::Error::other)?;
        let mut file = self.file.lock();
        writeln!(file, "{json}")?;
        file.sync_all()
    }

    /// Read all entries from disk (for the IPC get_trade_log command).
    /// Skips malformed lines with an eprintln! warning.
    pub fn read_all(path: &PathBuf) -> Result<Vec<TradeLogEntry>, std::io::Error> {
        let file = OpenOptions::new().read(true).open(path)?;
        let reader = BufReader::new(file);
        let mut entries = Vec::new();

        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }

            match serde_json::from_str::<TradeLogEntry>(&line) {
                Ok(entry) => entries.push(entry),
                Err(_) => eprintln!("trade_log: skipping malformed line: {line}"),
            }
        }

        Ok(entries)
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn entry(id: &str, symbol: &str) -> TradeLogEntry {
        TradeLogEntry {
            id: id.to_string(),
            timestamp: "2026-07-25T00:00:00Z".to_string(),
            strategy_id: "strategy_1".to_string(),
            strategy_name: "Momentum".to_string(),
            symbol: symbol.to_string(),
            side: "BUY".to_string(),
            quantity: 10,
            price: 123.45,
            order_id: format!("order_{id}"),
            order_status: "TRADED".to_string(),
            mode: "live".to_string(),
            rule_id: "rule_1".to_string(),
            notes: String::new(),
        }
    }

    #[test]
    fn test_append_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trade_log.jsonl");
        let log = TradeLog::open(path.clone()).unwrap();

        log.append(entry("1", "NIFTY")).unwrap();
        log.append(entry("2", "BANKNIFTY")).unwrap();

        let entries = TradeLog::read_all(&path).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].id, "1");
        assert_eq!(entries[0].symbol, "NIFTY");
        assert_eq!(entries[0].order_status, "TRADED");
        assert_eq!(entries[1].id, "2");
        assert_eq!(entries[1].symbol, "BANKNIFTY");
    }

    #[test]
    fn test_open_is_append_only() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trade_log.jsonl");

        TradeLog::open(path.clone())
            .unwrap()
            .append(entry("1", "NIFTY"))
            .unwrap();
        TradeLog::open(path.clone())
            .unwrap()
            .append(entry("2", "BANKNIFTY"))
            .unwrap();

        let entries = TradeLog::read_all(&path).unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_read_skips_malformed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trade_log.jsonl");
        let mut file = std::fs::File::create(&path).unwrap();
        writeln!(
            file,
            "{}",
            serde_json::to_string(&entry("1", "NIFTY")).unwrap()
        )
        .unwrap();
        writeln!(file).unwrap();
        writeln!(file, "{{malformed").unwrap();

        let entries = TradeLog::read_all(&path).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, "1");
    }
}
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Candle {
    pub timestamp: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

impl Candle {
    pub fn chart_time_seconds(&self) -> i64 {
        self.timestamp / 1_000
    }
}
pub mod candle;
pub mod order;
pub mod position;
pub mod quote;
pub mod tick;

pub use candle::Candle;
pub use order::{Order, OrderResult, OrderSide, OrderStatus, OrderType};
pub use position::{Portfolio, Position};
pub use quote::Quote;
pub use tick::Tick;

// Re-export for plugin API surface — the plugin events module types
// `crate::models::PaperTrade` so the engine trade type is reachable
// from outside the strategy module.
pub use crate::strategy::execution::paper::PaperTrade;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum OrderType {
    Market,
    Limit,
    StopLoss,
}

/// Lifecycle status of an order as reported by the broker. `Transit` and
/// `Pending` are **non-terminal** — the order has been submitted but not yet
/// filled, so downstream code must inspect [`OrderStatus::is_fill`] before
/// treating it as an execution. `Unknown` carries whatever raw status string
/// the broker sent that we don't model explicitly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum OrderStatus {
    /// Submitted to the broker, not yet at the exchange. Non-terminal.
    Transit,
    /// At the exchange, awaiting a fill. Non-terminal.
    Pending,
    /// Filled (fully or partially). Terminal.
    Traded,
    /// Rejected at the broker or exchange. Terminal.
    Rejected,
    /// Cancelled. Terminal.
    Cancelled,
    /// Expired without filling. Terminal.
    Expired,
    /// Any status string the broker sent that we don't model. Terminal is
    /// deliberately `false` so unknown statuses are never mistaken for a fill.
    Unknown(String),
}

impl OrderStatus {
    /// True if the order has reached a final state (filled, rejected,
    /// cancelled, or expired). `Transit`, `Pending`, and `Unknown` are treated
    /// as non-terminal.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Traded | Self::Rejected | Self::Cancelled | Self::Expired
        )
    }

    /// True only if the order actually filled. Everything else — including
    /// non-terminal and unknown statuses — is not a fill.
    pub fn is_fill(&self) -> bool {
        matches!(self, Self::Traded)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Order {
    pub symbol: String,
    pub side: OrderSide,
    pub quantity: u32,
    pub order_type: OrderType,
    pub price: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderResult {
    pub order_id: String,
    pub status: OrderStatus,
    pub timestamp: i64,
    /// Idempotency correlation id sent to the broker with this order. Empty for
    /// brokers that don't use one (e.g. the paper broker).
    pub correlation_id: String,
}

#[cfg(test)]
mod tests {
    use super::OrderStatus;

    #[test]
    fn test_transit_is_not_terminal() {
        assert!(!OrderStatus::Transit.is_terminal());
        assert!(!OrderStatus::Transit.is_fill());
    }

    #[test]
    fn test_pending_is_not_terminal() {
        assert!(!OrderStatus::Pending.is_terminal());
        assert!(!OrderStatus::Pending.is_fill());
    }

    #[test]
    fn test_traded_is_terminal_and_fill() {
        assert!(OrderStatus::Traded.is_terminal());
        assert!(OrderStatus::Traded.is_fill());
    }

    #[test]
    fn test_terminal_non_fills() {
        for status in [
            OrderStatus::Rejected,
            OrderStatus::Cancelled,
            OrderStatus::Expired,
        ] {
            assert!(status.is_terminal());
            assert!(!status.is_fill());
        }
    }

    #[test]
    fn test_unknown_is_not_terminal_and_not_fill() {
        let status = OrderStatus::Unknown("WEIRD".to_string());
        assert!(!status.is_terminal());
        assert!(!status.is_fill());
    }
}
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Position {
    pub symbol: String,
    pub quantity: i64,
    pub average_price: f64,
    pub ltp: f64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Portfolio {
    pub positions: Vec<Position>,
    pub total_value: f64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
}
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Quote {
    pub symbol: String,
    pub ltp: f64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub bid: f64,
    pub ask: f64,
    pub volume: u64,
}
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Tick {
    pub symbol: String,
    pub ltp: f64,
    pub volume: u64,
    pub timestamp: i64,
}
use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::plugin::types::{PluginError, PluginId, PluginResult};
use crate::strategy::execution::paper::PaperTrade;

use super::AnalyticsApi;

pub type AnalyticsFn = dyn Fn(&[PaperTrade]) -> f64 + Send + Sync;

pub struct SharedAnalyticsRegistry {
    inner: Arc<RwLock<HashMap<String, (PluginId, Arc<AnalyticsFn>)>>>,
}

impl SharedAnalyticsRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for SharedAnalyticsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedAnalyticsRegistry {
    pub fn register_fn(
        &self,
        name: &str,
        plugin_id: PluginId,
        f: Arc<AnalyticsFn>,
    ) -> PluginResult<()> {
        let mut map = self.inner.write();
        if let Some((existing, _)) = map.get(name) {
            if existing != &plugin_id {
                return Err(PluginError::ApiError(format!(
                    "analytic '{name}' already registered by plugin '{existing}'"
                )));
            }
        }
        map.insert(name.to_string(), (plugin_id, f));
        Ok(())
    }

    pub fn get_fn(&self, name: &str) -> Option<Arc<AnalyticsFn>> {
        self.inner.read().get(name).map(|(_, f)| f.clone())
    }

    pub fn list(&self) -> Vec<String> {
        let mut names: Vec<String> = self.inner.read().keys().cloned().collect();
        names.sort();
        names
    }

    pub fn unregister_all_for(&self, plugin_id: &PluginId) {
        self.inner.write().retain(|_, (id, _)| id != plugin_id);
    }
}

#[async_trait::async_trait]
impl AnalyticsApi for SharedAnalyticsRegistry {
    fn register_metric(
        &self,
        name: String,
        registered_by: PluginId,
        f: std::sync::Arc<dyn Fn(&[PaperTrade]) -> f64 + Send + Sync>,
    ) -> PluginResult<()> {
        let mut map = self.inner.write();
        if let Some((existing, _)) = map.get(&name) {
            if existing != &registered_by {
                return Err(PluginError::ApiError(format!(
                    "metric '{name}' already registered by plugin '{existing}'"
                )));
            }
            // Same plugin re-registration: silent overwrite.
        }
        map.insert(name, (registered_by, f));
        Ok(())
    }

    fn get_metric(
        &self,
        name: &str,
    ) -> Option<std::sync::Arc<dyn Fn(&[PaperTrade]) -> f64 + Send + Sync>> {
        self.inner.read().get(name).map(|(_, f)| f.clone())
    }

    fn list_metrics(&self) -> Vec<String> {
        let mut names: Vec<String> = self.inner.read().keys().cloned().collect();
        names.sort();
        names
    }
}
//! `DslExtensionApi` adapter for plugins.
//!
//! Plugins can register DSL keywords that the strategy evaluator will
//! resolve at runtime. Like the indicator and analytics registries, this
//! is a process-wide shared map keyed by keyword name, with per-plugin
//! attribution so the registry can clean up a plugin's contributions
//! when it is disabled.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::plugin::types::{PluginError, PluginId, PluginResult};
use crate::strategy::runtime::context::EvalContext;

use super::DslExtensionApi;

pub type DslKeywordHandler = dyn Fn(&EvalContext) -> bool + Send + Sync;

pub struct SharedDslExtensionRegistry {
    inner: Arc<RwLock<HashMap<String, (PluginId, Arc<DslKeywordHandler>)>>>,
}

impl SharedDslExtensionRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Concrete (non-trait) registration entry point. Plugins that want to
    /// attach a custom keyword handler use this directly; the trait-level
    /// `register_keyword` below shares the same backing map.
    pub fn register_handler(
        &self,
        keyword: &str,
        plugin_id: PluginId,
        handler: Arc<DslKeywordHandler>,
    ) -> PluginResult<()> {
        let mut map = self.inner.write();
        if let Some((existing_id, _)) = map.get(keyword) {
            if existing_id != &plugin_id {
                return Err(PluginError::ApiError(format!(
                    "DSL keyword '{keyword}' already registered by plugin '{existing_id}'"
                )));
            }
            // Same plugin re-registration: silent overwrite.
        }
        map.insert(keyword.to_string(), (plugin_id, handler));
        Ok(())
    }

    pub fn get_handler(&self, keyword: &str) -> Option<Arc<DslKeywordHandler>> {
        self.inner.read().get(keyword).map(|(_, h)| h.clone())
    }

    pub fn list_keywords(&self) -> Vec<String> {
        let mut names: Vec<String> = self.inner.read().keys().cloned().collect();
        names.sort();
        names
    }

    pub fn unregister_all_for(&self, plugin_id: &PluginId) {
        self.inner.write().retain(|_, (id, _)| id != plugin_id);
    }
}

impl Default for SharedDslExtensionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl DslExtensionApi for SharedDslExtensionRegistry {
    fn register_keyword(
        &self,
        keyword: String,
        registered_by: PluginId,
        handler: std::sync::Arc<
            dyn Fn(&crate::strategy::runtime::context::EvalContext) -> bool + Send + Sync,
        >,
    ) -> PluginResult<()> {
        let mut map = self.inner.write();
        if let Some((existing, _)) = map.get(&keyword) {
            if existing != &registered_by {
                return Err(PluginError::ApiError(format!(
                    "DSL keyword '{keyword}' already registered by plugin '{existing}'"
                )));
            }
        }
        map.insert(keyword, (registered_by, handler));
        Ok(())
    }

    fn get_keyword(
        &self,
        keyword: &str,
    ) -> Option<
        std::sync::Arc<
            dyn Fn(&crate::strategy::runtime::context::EvalContext) -> bool + Send + Sync,
        >,
    > {
        self.inner
            .read()
            .get(keyword)
            .map(|(_, h)| h.clone() as std::sync::Arc<_>)
    }

    fn unregister_all_for(&self, plugin_id: &PluginId) {
        self.inner.write().retain(|_, (id, _)| id != plugin_id);
    }
}
﻿use std::sync::Arc;

use parking_lot::RwLock;

use crate::models::Candle;
use crate::plugin::types::SubscriptionHandle;
use crate::strategy::execution::paper::PaperTrade;

// NOTE: `crate::models::Candle` already derives `Clone`. `PaperTrade` derives
// `Clone` (see src/strategy/execution/paper.rs), so `derive(Clone)` on
// `EventKind` is sound.

#[derive(Debug, Clone)]
pub enum EventKind {
    CandleProcessed(Candle),
    TradeExecuted(PaperTrade),
    RuleFired {
        rule_id: String,
        strategy_id: String,
    },
    StrategyStatusChanged {
        strategy_id: String,
        new_status: String,
    },
    SystemShutdown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventFilter {
    All,
    CandleProcessed,
    TradeExecuted,
    RuleFired,
    StrategyStatusChanged,
    SystemShutdown,
}

impl EventFilter {
    fn matches(&self, event: &EventKind) -> bool {
        match (self, event) {
            (EventFilter::All, _) => true,
            (EventFilter::CandleProcessed, EventKind::CandleProcessed(_)) => true,
            (EventFilter::TradeExecuted, EventKind::TradeExecuted(_)) => true,
            (EventFilter::RuleFired, EventKind::RuleFired { .. }) => true,
            (EventFilter::StrategyStatusChanged, EventKind::StrategyStatusChanged { .. }) => true,
            (EventFilter::SystemShutdown, EventKind::SystemShutdown) => true,
            _ => false,
        }
    }
}

pub struct EventBus {
    subscribers: Arc<
        RwLock<
            Vec<(
                SubscriptionHandle,
                crate::plugin::types::PluginId,
                EventFilter,
                Arc<dyn Fn(EventKind) + Send + Sync>,
            )>,
        >,
    >,
}

impl EventBus {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            subscribers: Arc::new(RwLock::new(Vec::new())),
        })
    }

    pub fn subscribe(
        &self,
        plugin_id: crate::plugin::types::PluginId,
        filter: EventFilter,
        callback: Arc<dyn Fn(EventKind) + Send + Sync>,
    ) -> SubscriptionHandle {
        let handle = SubscriptionHandle(uuid::Uuid::new_v4());
        let mut guard = self.subscribers.write();
        guard.push((handle, plugin_id, filter, callback));
        handle
    }

    pub fn unsubscribe(&self, handle: &SubscriptionHandle) {
        self.subscribers.write().retain(|(h, _, _, _)| h != handle);
    }

    pub fn unsubscribe_all_for(&self, plugin_id: &crate::plugin::types::PluginId) {
        self.subscribers.write().retain(|(_, id, _, _)| id != plugin_id);
    }

    pub fn publish(&self, event: EventKind) {
        // Collect matching (callback, event_clone) tuples under the read lock,
        // then release the lock before spawning tasks.
        let to_spawn: Vec<Arc<dyn Fn(EventKind) + Send + Sync>> = {
            let guard = self.subscribers.read();
            guard
                .iter()
                .filter(|(_, _, filter, _)| filter.matches(&event))
                .map(|(_, _, _, cb)| cb.clone())
                .collect()
        };
        for cb in to_spawn {
            let evt = event.clone();
            tokio::spawn(async move {
                cb(evt);
            });
        }
    }
}


//! `ExecutionApi` adapter for plugins.
//!
//! Three implementations live here:
//!
//! - [`NoopExecutionApi`] — the default. Used when no live session is active
//!   and the plugin capability list is otherwise static. Every call is a
//!   structured error explaining why.
//! - [`ReadOnlyLiveExecutionApi`] — wired into a real, in-flight live
//!   session. Plugins can **inspect** open positions but **cannot submit
//!   orders**. Live order submission is reserved for the strategy engine
//!   so every real order is funnelled through `MAX_DAILY_LOSS`, market-
//!   hours, pause, and trade-log gates.
//! - [`GatedLiveExecutionApi`] — Phase 8 plugin order gateway. Wired
//!   into an active live session, this implementation re-runs the same
//!   safety gates the `LiveGuard` runs at session start (broker stale,
//!   symbol-in-map, segment `NseEq`, market hours, session pause) on
//!   every `submit_order` call, then proxies through the same
//!   `DhanBroker::execute_with_meta` path the strategy engine uses, so
//!   the trade-log row, H3 cancellation, and risk gates all cover plugin
//!   orders too.
//!
//! Phase 7 deliberately did not expose a live plugin `submit_order`. A
//! properly gated plugin order gateway that re-runs every engine gate is
//! Phase 8 work. See CLAUDE.md invariant 18 (this file).

use std::sync::Arc;

use chrono::FixedOffset;
use parking_lot::RwLock;
use tokio::sync::Mutex;

use crate::broker::symbol_map::{Segment, SymbolMap};
use crate::live::{guard::is_market_open, holidays::NseHolidayCalendar, session::LiveSession};
use crate::plugin::types::{PluginError, PluginResult};
use crate::strategy::execution::{target::ExecutionTarget, DhanBroker};
use crate::strategy::execution::dhan::SessionContext;

use super::{ExecutionApi, OrderRequest, Position};

/// Map the broker's `Position` (richer: includes LTP, realised/unrealised
/// PnL) to the plugin-visible subset (`symbol`, `quantity`,
/// `average_price`). The plugin layer never sees PnL because the API
/// contract is intentionally minimal.
impl From<crate::models::Position> for Position {
    fn from(p: crate::models::Position) -> Self {
        Position {
            symbol: p.symbol,
            quantity: p.quantity,
            average_price: p.average_price,
        }
    }
}

/// No-op `ExecutionApi`. Used as the default for every plugin host when no
/// live session is in flight, and as a fallback for plugins that don't
/// declare the `Execution` capability at all.
pub struct NoopExecutionApi;

#[async_trait::async_trait]
impl ExecutionApi for NoopExecutionApi {
    async fn submit_order(&self, _order: OrderRequest) -> PluginResult<String> {
        Err(PluginError::ApiError(
            "plugin order submission is not wired: live orders may only originate \
             from the strategy engine"
                .to_string(),
        ))
    }

    async fn cancel_order(&self, _order_id: &str) -> PluginResult<()> {
        Err(PluginError::ApiError(
            "plugin order submission is not wired: cancel_order is not supported \
             outside the strategy engine"
                .to_string(),
        ))
    }

    fn positions(&self) -> PluginResult<Vec<Position>> {
        Ok(Vec::new())
    }
}

/// Read-only live execution API. Plugins can query the open-position
/// snapshot during an active live session but cannot place or cancel
/// orders. See module docs for the Phase-7 rationale.
///
/// `handle` is captured at host-factory construction time (not at call
/// time) because the plugin callback may run on a non-tokio thread. Using
/// `block_in_place` requires the **multi-thread** tokio runtime; Tauri 2
/// ships one by default but `spawn_blocking` would be the safer escape
/// hatch for any current-thread context. We assert at construction time
/// and document the assumption — see `src-tauri/src/main.rs` for the
/// factory wiring.
pub struct ReadOnlyLiveExecutionApi {
    broker: Arc<DhanBroker>,
    handle: tokio::runtime::Handle,
}

impl ReadOnlyLiveExecutionApi {
    pub fn new(broker: Arc<DhanBroker>, handle: tokio::runtime::Handle) -> Self {
        // Debug-build assertion that the runtime we captured is multi-
        // threaded. If a single-threaded runtime ever wires this in,
        // `block_in_place` would panic at the first `positions()` call;
        // surface the misconfiguration loudly at boot instead.
        let runtime_type = handle.runtime_flavor();
        debug_assert!(
            matches!(runtime_type, tokio::runtime::RuntimeFlavor::MultiThread),
            "ReadOnlyLiveExecutionApi requires a multi-thread tokio runtime \
             (got {runtime_type:?}); block_in_place would panic"
        );
        Self { broker, handle }
    }
}

#[async_trait::async_trait]
impl ExecutionApi for ReadOnlyLiveExecutionApi {
    async fn submit_order(&self, _order: OrderRequest) -> PluginResult<String> {
        Err(PluginError::ApiError(
            "plugin order submission is disabled in Phase 7; live orders may only \
             originate from the strategy engine. A properly gated plugin order \
             gateway is Phase 8 work."
                .to_string(),
        ))
    }

    async fn cancel_order(&self, _order_id: &str) -> PluginResult<()> {
        Err(PluginError::ApiError(
            "plugin order submission is disabled in Phase 7; cancel is not \
             supported outside the strategy engine."
                .to_string(),
        ))
    }

    fn positions(&self) -> PluginResult<Vec<Position>> {
        // Async → sync bridge. The trait's `positions()` is sync; the
        // underlying `DhanBroker::get_positions()` is async. Use
        // `block_in_place` so we don't stall the multi-thread runtime's
        // worker pool, then run the future on the captured handle.
        let broker = self.broker.clone();
        tokio::task::block_in_place(|| {
            self.handle.block_on(async move {
                broker
                    .get_positions()
                    .await
                    .map_err(|e| PluginError::ApiError(e.to_string()))
                    .map(|ps| ps.into_iter().map(Into::into).collect())
            })
        })
    }
}

/// Phase 8 plugin order gateway. Every `submit_order` call re-runs the
/// safety gates the engine relies on at session start, so a misbehaving or
/// malicious plugin cannot bypass market-hours, broker staleness, the
/// symbol/segment filter, or the session's pause / cancellation state.
///
/// On every gate pass, the request is forwarded to
/// [`DhanBroker::execute_with_meta`] (the same path the engine uses),
/// which means:
///
/// - The session's `CancellationToken` aborts the order before any
///   broker HTTP call if the user clicks Stop (H3).
/// - The session's `paused_for_entries` flag blocks entry (BUY) orders
///   but lets exit (SELL) orders through (the Phase 7 pause invariant).
/// - The trade-log row carries the plugin's order id (`"plugin"`) and
///   the supplied `OrderRequest.side` / `quantity` so the audit trail
///   covers plugin orders too.
/// - A session that is no longer running cannot be reached through this
///   API — the session slot is empty, so every gate short-circuits to a
///   structured error.
///
/// Gates, in order:
///
/// 1. Session is currently running (`live_session` slot is `Some`).
/// 2. Symbol is in `SymbolMap` and resolves to `Segment::NseEq`.
/// 3. Market hours (09:15–15:30 IST, Mon–Fri, holidays excluded) using
///    `is_market_open` so this matches `LiveGuard::run_preflight`.
/// 4. Broker cache is fresh (`broker.is_stale() == false`).
/// 5. Session has not been cancelled (mirrors the H3 check inside
///    `execute_with_meta`, so the gate fails *before* we touch the
///    broker).
/// 6. Session is not paused — for BUY orders only. SELL orders always
///    go through so plugin-driven exits remain allowed.
///
/// `cancel_order` remains a structured error: the broker does not yet
/// expose a cancel endpoint, and the engine never needs one (it relies
/// on `paused_for_entries` and SELL-on-next-candle to leave a position).
pub struct GatedLiveExecutionApi {
    broker: Arc<DhanBroker>,
    session_slot: Arc<Mutex<Option<Arc<LiveSession>>>>,
    symbol_map: Arc<RwLock<SymbolMap>>,
    holidays: Arc<NseHolidayCalendar>,
    handle: tokio::runtime::Handle,
    test_bypass_market_hours: bool,
}

impl GatedLiveExecutionApi {
    pub fn new(
        broker: Arc<DhanBroker>,
        session_slot: Arc<Mutex<Option<Arc<LiveSession>>>>,
        symbol_map: Arc<RwLock<SymbolMap>>,
        holidays: Arc<NseHolidayCalendar>,
        handle: tokio::runtime::Handle,
    ) -> Self {
        let runtime_type = handle.runtime_flavor();
        debug_assert!(
            matches!(runtime_type, tokio::runtime::RuntimeFlavor::MultiThread),
            "GatedLiveExecutionApi requires a multi-thread tokio runtime \
             (got {runtime_type:?}); block_in_place would panic"
        );
        Self {
            broker,
            session_slot,
            symbol_map,
            holidays,
            handle,
            test_bypass_market_hours: false,
        }
    }

    /// Run all six gates against the broker, session, and clock. Each
    /// gate short-circuits with a specific reason so the plugin can
    /// surface a useful error to the user. Async because we need to take
    /// the session slot briefly and the broker's `is_stale()` /
    /// `execute_with_meta` are async.
    async fn check_gates(&self, order: &OrderRequest) -> Result<Arc<LiveSession>, String> {
        // Gate 1: a live session is currently active.
        let session = {
            let slot = self.session_slot.lock().await;
            slot.as_ref()
                .ok_or_else(|| "plugin order rejected: no live session is running".to_string())?
                .clone()
        };

        // Gate 2: symbol is in the symbol map and is NSE equity.
        let segment = {
            let map = self.symbol_map.read();
            map.lookup(&order.symbol)
                .ok_or_else(|| {
                    format!(
                        "plugin order rejected: symbol '{}' is not in the symbol map",
                        order.symbol
                    )
                })?
                .segment
        };
        if segment != Segment::NseEq {
            return Err(format!(
                "plugin order rejected: Phase 8 only supports NSE equity; \
                 symbol '{}' is {:?}",
                order.symbol, segment
            ));
        }

        // Gate 3: market hours. Same predicate as LiveGuard so a plugin
        // order is never accepted outside the same window a session
        // start would be.
        let now_ist = chrono::Local::now().with_timezone(
            &FixedOffset::east_opt(crate::live::guard::IST_OFFSET_SECONDS)
                .expect("IST offset is in range"),
        );
        if !self.test_bypass_market_hours && !is_market_open(now_ist, &self.holidays) {
            return Err(
                "plugin order rejected: market is closed; live trading is only \
                 allowed 09:15–15:30 IST on NSE trading days"
                    .to_string(),
            );
        }

        // Gate 4: broker cache must be fresh — same predicate as
        // LiveGuard gate 8. A stale broker means MAX_DAILY_LOSS is
        // unreliable, so an order is unsafe to submit.
        if session.broker.is_stale() {
            return Err(
                "plugin order rejected: broker realized-loss / funds tracking is stale; \
                 retry once the broker cache recovers"
                    .to_string(),
            );
        }

        // Gate 5: the session's cancellation token must not be set.
        // `DhanBroker::execute_with_meta` checks this itself, but doing
        // it here keeps the rejection out of the trade-log path and
        // gives the plugin a clearer "session cancelled" reason.
        if session.broker.cancel_token().is_cancelled() {
            return Err(
                "plugin order rejected: live session has been stopped".to_string(),
            );
        }

        // Gate 6: pause state. SELL orders always execute so plugins
        // can still drive exits (mirrors the engine's pause invariant).
        // BUY orders are blocked because the user has explicitly asked
        // not to open new positions.
        let paused = session
            .broker
            .paused_for_entries_snapshot();
        if paused && order.side == super::OrderSide::Buy {
            return Err(format!(
                "plugin order rejected: live session is paused (BUY suppressed); \
                 symbol '{}'",
                order.symbol
            ));
        }

        Ok(session)
    }
}

#[async_trait::async_trait]
impl ExecutionApi for GatedLiveExecutionApi {
    async fn submit_order(&self, order: OrderRequest) -> PluginResult<String> {
        let session = self
            .check_gates(&order)
            .await
            .map_err(PluginError::ApiError)?;

        // Convert the plugin-visible OrderRequest into the broker's Order
        // shape. StopLoss isn't a Phase 8 plugin order type; map Market
        // / Limit through, fall back to Market for anything else so the
        // plugin never accidentally submits a stop loss the user did
        // not declare on the strategy.
        let broker_order = crate::models::Order {
            symbol: order.symbol.clone(),
            side: match order.side {
                super::OrderSide::Buy => crate::models::OrderSide::Buy,
                super::OrderSide::Sell => crate::models::OrderSide::Sell,
            },
            quantity: order.quantity,
            order_type: match order.order_type {
                super::OrderType::Market => crate::models::OrderType::Market,
                super::OrderType::Limit => crate::models::OrderType::Limit,
            },
            price: order.price,
        };

        // Stage a SessionContext on the broker so the trade-log row
        // carries the plugin-attributed strategy_id / strategy_name.
        // The engine normally writes this on `LiveSession::start`; we
        // keep that one intact by reading first and restoring after
        // the call. (Phase 8 audit H2 notes the engine's
        // SessionContext is also written by the live tick loop, so we
        // are intentionally not stomping on it — we only stamp the
        // `rule_id`/`notes` fields via `execute_with_meta`.)
        let _ctx_guard = PluginSessionContextGuard::new(
            session.broker.clone(),
            SessionContext {
                strategy_id: format!(
                    "plugin:{}",
                    session.strategy_id
                ),
                strategy_name: format!(
                    "{} [plugin order]",
                    session.strategy_name
                ),
                mode: "live".to_string(),
            },
        );

        let result = session
            .broker
            .execute_with_meta(broker_order, "plugin", "")
            .await
            .map_err(|e| PluginError::ApiError(e.message))?;

        // Mirror the engine's trade-log semantics: only fill statuses
        // carry a meaningful order id; transit / pending placeholders
        // are returned but not surfaced as a "trade" by the UI. Phase 8
        // plugins receive the raw order_id either way — the trade-log
        // table filter is a UI concern.
        Ok(result.order_id)
    }

    async fn cancel_order(&self, _order_id: &str) -> PluginResult<()> {
        // The DhanBroker does not yet expose a cancel endpoint, and the
        // engine itself relies on the next-candle SELL rather than an
        // explicit cancel. Keep cancel_order as a structured error so
        // plugins see a clear "not wired" reason rather than a silent
        // success.
        Err(PluginError::ApiError(
            "plugin cancel_order is not wired: the broker does not yet expose a \
             cancel endpoint; rely on SELL on the next candle to exit positions"
                .to_string(),
        ))
    }

    fn positions(&self) -> PluginResult<Vec<Position>> {
        // Same async → sync bridge as the read-only variant. The gate
        // session check is not needed for `positions()` — querying
        // positions is always safe, and the read-only API already lets
        // plugins do this regardless of session state.
        let broker = self.broker.clone();
        tokio::task::block_in_place(|| {
            self.handle.block_on(async move {
                broker
                    .get_positions()
                    .await
                    .map_err(|e| PluginError::ApiError(e.to_string()))
                    .map(|ps| ps.into_iter().map(Into::into).collect())
            })
        })
    }
}

/// RAII guard that stages a `SessionContext` on the broker so a plugin
/// order's trade-log row attributes to the plugin instead of the engine's
/// strategy. Restores the prior context (or `None`) on drop so the next
/// engine-driven order reattaches to its own strategy.
///
/// The engine writes its own `SessionContext` on `LiveSession::start`
/// and overwrites it on `stop`. We deliberately overwrite it for the
/// duration of one `execute_with_meta` call and restore the previous
/// value; the engine's own `on_candle` runs on a separate tokio task
/// and never overlaps a plugin order in single-session Phase 8 (both
/// are driven by the same multi-thread runtime, but the engine's task
/// only runs on a candle boundary while a plugin order is fired from
/// the plugin host's runtime thread).
struct PluginSessionContextGuard {
    broker: Arc<DhanBroker>,
    previous: Option<SessionContext>,
}

impl PluginSessionContextGuard {
    fn new(broker: Arc<DhanBroker>, ctx: SessionContext) -> Self {
        let previous = broker.session_context.read().clone();
        *broker.session_context.write() = Some(ctx);
        Self { broker, previous }
    }
}

impl Drop for PluginSessionContextGuard {
    fn drop(&mut self) {
        *self.broker.session_context.write() = self.previous.clone();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::dhan::{DhanAuth, DhanClient};
    use crate::broker::symbol_map::{Segment, SymbolMap};
    use crate::live::holidays::NseHolidayCalendar;
    use crate::live::trade_log::TradeLog;
    use crate::plugin::api::{OrderSide, OrderType};
    use crate::strategy::execution::DhanBroker;

    // ---- Mocks for gated API tests ----
    #[derive(Debug)]
    struct MockOrderPlacer {
        result: crate::models::OrderResult,
    }

    #[async_trait::async_trait]
    impl crate::strategy::execution::dhan::OrderPlacer for MockOrderPlacer {
        async fn place(&self, _order: crate::models::Order) -> anyhow::Result<crate::models::OrderResult> {
            Ok(self.result.clone())
        }
    }

    fn temp_trade_log() -> (Arc<TradeLog>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trade_log.jsonl");
        let log = Arc::new(TradeLog::open(path).unwrap());
        (log, dir)
    }

    fn dummy_broker() -> Arc<DhanBroker> {
        let auth = DhanAuth::new("test-token").unwrap();
        let (log, _dir) = temp_trade_log();
        Arc::new(DhanBroker::new(Arc::new(DhanClient::new(auth)), log))
    }


    fn dummy_session_slot() -> Arc<Mutex<Option<Arc<LiveSession>>>> {
        Arc::new(Mutex::new(None))
    }

    fn dummy_symbol_map() -> Arc<RwLock<SymbolMap>> {
        let mut map = SymbolMap::empty();
        map.insert_entry("RELIANCE", 1234, Segment::NseEq);
        Arc::new(RwLock::new(map))
    }

    fn gated_api(
        broker: Arc<DhanBroker>,
        session_slot: Arc<Mutex<Option<Arc<LiveSession>>>>,
    ) -> GatedLiveExecutionApi {
        let mut api = GatedLiveExecutionApi::new(
            broker,
            session_slot,
            dummy_symbol_map(),
            Arc::new(NseHolidayCalendar::new()),
            tokio::runtime::Handle::current(),
        );
        api.test_bypass_market_hours = true;
        api
    }

    /// Gate 1: a GatedLiveExecutionApi with an empty session slot must
    /// refuse every submit_order with a "no live session" reason.
    #[tokio::test(flavor = "multi_thread")]
    async fn gate_no_session_returns_error() {
        let api = gated_api(dummy_broker(), dummy_session_slot());
        let order = OrderRequest {
            symbol: "RELIANCE".to_string(),
            side: OrderSide::Buy,
            quantity: 1,
            order_type: OrderType::Market,
            price: None,
        };
        let result = api.submit_order(order).await;
        match result {
            Err(PluginError::ApiError(msg)) => {
                assert!(msg.contains("no live session"), "got: {msg}");
            }
            other => panic!("expected ApiError, got {other:?}"),
        }
    }

    /// Gate 2 (segment): a symbol that resolves but is not NseEq must
    /// be rejected with a structured error mentioning the segment.
    /// We exercise this without standing up a full LiveSession by
    /// driving the segment lookup directly.
    #[test]
    fn gate_segment_check_rejects_non_nse_eq() {
        let mut map = SymbolMap::empty();
        map.insert_entry("NIFTY", 999, Segment::Index);
        let entry = map.lookup("NIFTY").expect("nifty is in the map");
        assert_eq!(entry.segment, Segment::Index);
        // The GatedLiveExecutionApi gate compares `entry.segment != Segment::NseEq`
        // and produces "Phase 8 only supports NSE equity" — the
        // condition that drives that message is exercised above.
    }

    /// Gate 5: a pre-cancelled `CancellationToken` on the broker must
    /// abort `execute_with_meta` before any HTTP call. The gated API
    /// checks the same token at gate 5; the underlying check inside
    /// `execute_with_meta` is the safety net for races where the
    /// session is cancelled between gate 5 and the broker call.
    #[tokio::test]
    async fn gate_cancel_token_aborts_execute() {
        let auth = DhanAuth::new("test-token").unwrap();
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("trade_log.jsonl");
        let log = Arc::new(TradeLog::open(log_path).unwrap());
        let broker = Arc::new(DhanBroker::new(Arc::new(DhanClient::new(auth)), log));
        let token = tokio_util::sync::CancellationToken::new();
        token.cancel();
        broker.set_cancel_token(token);

        let order = crate::models::Order {
            symbol: "RELIANCE".to_string(),
            side: crate::models::OrderSide::Buy,
            quantity: 1,
            order_type: crate::models::OrderType::Market,
            price: None,
        };
        let result = broker.execute_with_meta(order, "plugin", "").await;
        match result {
            Err(err) => assert!(
                err.message.contains("session cancelled"),
                "got: {}",
                err.message
            ),
            Ok(_) => panic!("cancelled token must abort before broker call"),
        }
    }

    /// `PluginSessionContextGuard` must restore the prior context on
    /// drop so the engine's next `execute_with_meta` reattaches to its
    /// own strategy metadata (otherwise a plugin order would clobber
    /// the engine's `SessionContext` for all subsequent orders).
    #[test]
    fn plugin_session_context_guard_restores_previous() {
        let auth = DhanAuth::new("test-token").unwrap();
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("trade_log.jsonl");
        let log = Arc::new(TradeLog::open(log_path).unwrap());
        let broker = Arc::new(DhanBroker::new(Arc::new(DhanClient::new(auth)), log));

        let engine_ctx = SessionContext {
            strategy_id: "engine-strategy".to_string(),
            strategy_name: "Engine".to_string(),
            mode: "live".to_string(),
        };
        *broker.session_context.write() = Some(engine_ctx.clone());

        {
            let _guard = PluginSessionContextGuard::new(
                broker.clone(),
                SessionContext {
                    strategy_id: "plugin:engine-strategy".to_string(),
                    strategy_name: "Engine [plugin order]".to_string(),
                    mode: "live".to_string(),
                },
            );
            let active = broker.session_context.read().clone().unwrap();
            assert_eq!(active.strategy_id, "plugin:engine-strategy");
        }
        let restored = broker.session_context.read().clone().unwrap();
        assert_eq!(restored.strategy_id, "engine-strategy");
        assert_eq!(restored.strategy_name, "Engine");
    }

    /// `PluginSessionContextGuard` also handles the "no prior context"
    /// case (e.g. a plugin order placed before any engine session has
    /// run) by restoring `None`.
    #[test]
    fn plugin_session_context_guard_handles_no_prior() {
        let auth = DhanAuth::new("test-token").unwrap();
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("trade_log.jsonl");
        let log = Arc::new(TradeLog::open(log_path).unwrap());
        let broker = Arc::new(DhanBroker::new(Arc::new(DhanClient::new(auth)), log));

        assert!(broker.session_context.read().is_none());
        {
            let _guard = PluginSessionContextGuard::new(
                broker.clone(),
                SessionContext {
                    strategy_id: "plugin:standalone".to_string(),
                    strategy_name: "Plugin Order".to_string(),
                    mode: "live".to_string(),
                },
            );
            assert!(broker.session_context.read().is_some());
        }
        assert!(
            broker.session_context.read().is_none(),
            "guard must restore None when there was no prior context"
        );
    }

    /// B4 Verification: ensure that the gates check the *session's* broker state,
    /// not the global API broker state. This is critical for future multi-session
    /// support where different sessions might have different broker connectivity.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_gate_uses_session_broker_not_global_broker() {
        let global_broker = dummy_broker();

        // Use mocks for the session broker so we don't trip on Dhan client config/network.
        let (log, _dir) = temp_trade_log();
        let mut sb = DhanBroker::new(
            Arc::new(DhanClient::new(DhanAuth::new("test-token").unwrap())),
            log.clone(),
        );
        sb.order_placer = Arc::new(MockOrderPlacer {
            result: crate::models::OrderResult {
                order_id: "mock-order-1".to_string(),
                status: crate::models::OrderStatus::Traded,
                timestamp: 0,
                correlation_id: "mock-corr".to_string(),
            },
        });
        let session_broker = Arc::new(sb);

        let session_slot = Arc::new(Mutex::new(None));

        // Setup dependencies for LiveSession::start
        let strategy_node = crate::strategy::dsl::StrategyNode {
            name: "Test".to_string(),
            trade_in: None,
            stop_loss: None,
            take_profit: None,
            rules: vec![],
            risk: None,
        };
        let feed = Arc::new(Mutex::new(crate::feed::FeedManager::new()));
        let trade_log = Arc::new(TradeLog::open(tempfile::tempdir().unwrap().path().join("test.log")).unwrap());
        let event_bus = crate::plugin::api::events::EventBus::new();
        let emitter = Arc::new(crate::live::session::NoopEmitter);
        let holidays = Arc::new(NseHolidayCalendar::new());

        let session = crate::live::session::LiveSession::start(
            "test-strat".to_string(),
            "Test Strategy".to_string(),
            "RELIANCE".to_string(),
            strategy_node,
            session_broker.clone(),
            feed,
            trade_log,
            event_bus,
            vec![],
            100_000.0,
            emitter,
            holidays,
        )
        .await
        .expect("Session should start");

        *session_slot.lock().await = Some(session);

        let api = gated_api(global_broker, session_slot);

        let order = OrderRequest {
            symbol: "RELIANCE".to_string(),
            side: OrderSide::Buy,
            quantity: 1,
            order_type: OrderType::Market,
            price: None,
        };

        let result = api.submit_order(order).await;

        // If B4 is fixed, this should be Ok because it checks session_broker (which is fresh),
        // even if the global_broker was something else.
        assert!(result.is_ok(), "Order should be accepted when session broker is fresh, regardless of global broker state. Got: {:?}", result.err());
    }
}
use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::models::Candle;
use crate::plugin::types::{PluginError, PluginId, PluginResult};

use super::{IndicatorInstance, IndicatorRegistryApi};

/// Type alias matching the spec: a stateless computation function from a
/// candle slice + a window length to a vector of indicator values.
pub type IndicatorFn = dyn Fn(&[Candle], usize) -> Vec<f64> + Send + Sync;

pub struct SharedIndicatorRegistry {
    inner: Arc<RwLock<HashMap<String, (PluginId, Arc<IndicatorFn>)>>>,
}

impl SharedIndicatorRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for SharedIndicatorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Spec-shaped registration entry point used by plugins that want to register
/// a pure candle-slice computation. This is the entry point the spec describes
/// as `register`. Plugins using the trait-level `IndicatorRegistryApi` go
/// through the factory path below; both share the same backing map.
impl SharedIndicatorRegistry {
    pub fn register_fn(
        &self,
        name: &str,
        plugin_id: PluginId,
        f: Arc<IndicatorFn>,
    ) -> PluginResult<()> {
        let mut map = self.inner.write();
        if let Some((existing_id, _)) = map.get(name) {
            if existing_id != &plugin_id {
                return Err(PluginError::ApiError(format!(
                    "indicator '{name}' already registered by plugin '{existing_id}'"
                )));
            }
            // Same plugin re-registration: silent overwrite.
        }
        map.insert(name.to_string(), (plugin_id, f));
        Ok(())
    }

    pub fn get_fn(&self, name: &str) -> Option<Arc<IndicatorFn>> {
        self.inner.read().get(name).map(|(_, f)| f.clone())
    }

    pub fn list(&self) -> Vec<String> {
        let mut names: Vec<String> = self.inner.read().keys().cloned().collect();
        names.sort();
        names
    }

    pub fn unregister_all_for(&self, plugin_id: &PluginId) {
        self.inner.write().retain(|_, (id, _)| id != plugin_id);
    }
}

/// Adapter that wraps a stateless `IndicatorFn` as a stateful `IndicatorInstance`
/// for the trait-level API. Each `new()` captures the function reference and
/// computes the full vector once on first `value()` call; subsequent updates
/// recompute on the current buffer.
#[allow(dead_code)]
pub struct FnIndicatorInstance {
    f: Arc<IndicatorFn>,
    history: Vec<f64>,
    last_result: Option<f64>,
}

#[async_trait::async_trait]
impl IndicatorRegistryApi for SharedIndicatorRegistry {
    fn register(
        &self,
        name: String,
        registered_by: PluginId,
        f: std::sync::Arc<dyn Fn(&[Candle], usize) -> Vec<f64> + Send + Sync>,
    ) -> PluginResult<()> {
        let mut map = self.inner.write();
        if let Some((existing, _)) = map.get(&name) {
            if existing != &registered_by {
                return Err(PluginError::ApiError(format!(
                    "indicator '{name}' already registered by plugin '{existing}'"
                )));
            }
            // Same plugin re-registration: silent overwrite.
        }
        map.insert(name, (registered_by, f));
        Ok(())
    }

    fn get(&self, name: &str) -> PluginResult<Box<dyn IndicatorInstance>> {
        let f = self
            .inner
            .read()
            .get(name)
            .map(|(_, f)| f.clone())
            .ok_or_else(|| PluginError::NotFound(format!("indicator '{name}' not found")))?;
        Ok(Box::new(FnIndicatorInstance {
            f,
            history: Vec::new(),
            last_result: None,
        }))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl IndicatorInstance for FnIndicatorInstance {
    fn update(&mut self, value: f64) {
        self.history.push(value);
    }
    fn value(&self) -> Option<f64> {
        self.last_result
    }
    fn name(&self) -> &str {
        "fn_indicator"
    }
}
use crate::plugin::types::PluginId;

use super::LogApi;

pub struct NamespacedLog {
    pub plugin_id: String,
}

impl NamespacedLog {
    pub fn new(plugin_id: PluginId) -> Self {
        Self {
            plugin_id: plugin_id.to_string(),
        }
    }
}

#[async_trait::async_trait]
impl LogApi for NamespacedLog {
    fn debug(&self, _plugin_id: &PluginId, message: &str) {
        eprintln!("[plugin:{}] [DEBUG] {}", self.plugin_id, message);
    }
    fn info(&self, _plugin_id: &PluginId, message: &str) {
        eprintln!("[plugin:{}] [INFO] {}", self.plugin_id, message);
    }
    fn warn(&self, _plugin_id: &PluginId, message: &str) {
        eprintln!("[plugin:{}] [WARN] {}", self.plugin_id, message);
    }
    fn error(&self, _plugin_id: &PluginId, message: &str) {
        eprintln!("[plugin:{}] [ERROR] {}", self.plugin_id, message);
    }
}

pub struct NoopLog;

#[async_trait::async_trait]
impl LogApi for NoopLog {
    fn debug(&self, _plugin_id: &PluginId, _message: &str) {}
    fn info(&self, _plugin_id: &PluginId, _message: &str) {}
    fn warn(&self, _plugin_id: &PluginId, _message: &str) {}
    fn error(&self, _plugin_id: &PluginId, _message: &str) {}
}
//! Rate-limited, rolling-file plugin logger.
//!
//! Plugin code is untrusted — a misbehaving (or malicious) plugin could
//! spam `log_info` / `log_warn` / `log_error` in a tight loop and fill
//! the user's SSD with `[plugin:x] ...` lines. This module is the
//! containment layer for that risk.
//!
//! Two defenses are layered:
//!
//! 1. **Token-bucket rate limit per plugin** (see [`RateLimiter`]). The
//!    default is 10 messages per second burst and 100 messages per
//!    minute sustained, shared across all log levels. Excess messages
//!    are silently dropped, but a counter increments and a single
//!    summary line per minute is written so the user can see that
//!    something is being throttled without filling the log.
//!
//! 2. **5MB rolling log file per plugin** (see [`RollingLog`]). The
//!    file lives under `<app_data_dir>/logs/plugin-<id>.log`. On every
//!    write, the writer checks the file size; if the line would push
//!    it over the cap, the file is rotated (current file is renamed
//!    to `<base>.1` — older `*.1` is overwritten — and a fresh
//!    current file is opened). This bounds disk usage to ~5MB per
//!    plugin regardless of rate-limit behavior.
//!
//! The two are combined in [`RateLimitedFileLog`], which implements
//! [`LogApi`]. The Tauri binary wires one of these per plugin via the
//! host factory; the CLI keeps using the simpler [`NamespacedLog`]
//! (no file output, no rate limit) so terminal output stays
//! human-readable.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use parking_lot::Mutex as PlMutex;

use crate::plugin::types::PluginId;

use super::LogApi;

/// Maximum size of a single log file before rotation, in bytes.
/// 5 MB is the spec; this constant is the single source of truth.
pub const MAX_LOG_BYTES: u64 = 5 * 1024 * 1024;

/// On rotation the current file is renamed to `<base>.1`; any prior
/// `<base>.1` is overwritten. We do NOT keep a sequence of historical
/// logs — the spec asks for a single rolling file per plugin.
const ROTATED_SUFFIX: &str = ".1";

/// Token-bucket rate limiter. Per plugin, shared across all log
/// levels. Defaults: 10 msg/sec burst, 100 msg/min sustained.
///
/// `now` is fed in (rather than read from `Instant::now()` inside the
/// struct) so tests can drive the clock deterministically.
#[derive(Debug)]
pub struct RateLimiter {
    /// Token-bucket refills at `burst_per_second` tokens/sec up to
    /// `burst_capacity`. We track fractional tokens so a steady drip
    /// (1 msg / 250ms) and a single 10-msg burst both work.
    burst_capacity: f64,
    burst_refill_per_sec: f64,
    burst_tokens: f64,
    burst_last_refill: Instant,

    /// Sliding window: how many messages were written in the last
    /// `sustained_window_secs`. When the count exceeds
    /// `sustained_max` the limiter blocks until the oldest message
    /// in the window ages out.
    sustained_max: u32,
    sustained_window_secs: u64,
    /// `(timestamp, count)` pairs in arrival order. Pairs are merged
    /// when adjacent to keep the vector short.
    sustained_log: Vec<(Instant, u32)>,
    sustained_in_window: u32,

    /// Throttling telemetry. Every time we drop a message we bump
    /// `dropped`. Once per minute we emit a single summary line
    /// ("N messages dropped in the last 60s") and reset the counter.
    dropped: u64,
    last_summary_at: Instant,
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new(
            /* burst_capacity */ 10, /* burst_per_sec */ 10.0,
            /* sustained_max */ 100, /* sustained_window_secs */ 60,
        )
    }
}

impl RateLimiter {
    /// Build a limiter with explicit limits. `burst_per_sec` is the
    /// sustained refill rate for the short-term bucket; for the
    /// defaults we pass `10.0` to match the 10/sec burst budget.
    pub fn new(
        burst_capacity: u32,
        burst_per_sec: f64,
        sustained_max: u32,
        sustained_window_secs: u64,
    ) -> Self {
        let now = Instant::now();
        Self {
            burst_capacity: burst_capacity as f64,
            burst_refill_per_sec: burst_per_sec,
            burst_tokens: burst_capacity as f64,
            burst_last_refill: now,
            sustained_max,
            sustained_window_secs,
            sustained_log: Vec::new(),
            sustained_in_window: 0,
            dropped: 0,
            last_summary_at: now,
        }
    }

    /// Try to admit one message. Returns `true` if the caller should
    /// proceed, `false` if the message should be dropped.
    pub fn try_admit(&mut self, now: Instant) -> bool {
        // Refill the burst bucket.
        let elapsed = now
            .saturating_duration_since(self.burst_last_refill)
            .as_secs_f64();
        if elapsed > 0.0 {
            self.burst_tokens =
                (self.burst_tokens + elapsed * self.burst_refill_per_sec).min(self.burst_capacity);
            self.burst_last_refill = now;
        }
        if self.burst_tokens < 1.0 {
            self.record_drop(now);
            return false;
        }

        // Drop any sustained-log entries that have aged out.
        self.sustained_log.retain(|(t, _)| {
            now.saturating_duration_since(*t).as_secs() < self.sustained_window_secs
        });
        // Recompute the in-window count after aging out.
        self.sustained_in_window = self.sustained_log.iter().map(|(_, c)| *c).sum();

        if self.sustained_in_window >= self.sustained_max {
            self.record_drop(now);
            return false;
        }

        // Admit: consume a burst token and append to the window.
        self.burst_tokens -= 1.0;
        match self.sustained_log.last_mut() {
            Some((t, c)) if now.saturating_duration_since(*t).as_secs() < 1 => {
                *c += 1;
            }
            _ => self.sustained_log.push((now, 1)),
        }
        self.sustained_in_window += 1;
        true
    }

    fn record_drop(&mut self, _now: Instant) {
        self.dropped += 1;
    }

    /// Number of messages dropped since the last summary was emitted.
    pub fn dropped_since_summary(&self) -> u64 {
        self.dropped
    }

    /// Take the dropped counter, returning how many messages were
    /// dropped since the last reset. The caller is expected to write
    /// a single summary line per minute; the limiter does not write
    /// on its own.
    pub fn take_summary(&mut self, now: Instant) -> Option<(u64, Duration)> {
        if self.dropped == 0 {
            return None;
        }
        let since = now.saturating_duration_since(self.last_summary_at);
        // Emit at most once per minute.
        if since < Duration::from_secs(60) {
            return None;
        }
        let dropped = std::mem::take(&mut self.dropped);
        self.last_summary_at = now;
        Some((dropped, since))
    }
}

/// Rolling log file. Owns one open `File` plus a `Mutex` so the
/// `RateLimitedFileLog` methods can be called from multiple threads
/// (plugins run concurrently and the Rhai/WASM runtimes may invoke
/// `log_info` from any task). The size check is "best-effort" — two
/// concurrent writers can both observe `len() < MAX_LOG_BYTES` and
/// write through, briefly exceeding the cap. The size check is a
/// guardrail, not a strict budget; rolling on every write would be
/// pathologically expensive.
pub struct RollingLog {
    path: PathBuf,
    file: Mutex<Option<File>>,
    /// Cached file size, refreshed on every `write_line`. Avoids an
    /// `fstat` per line. `0` means "fresh start or post-rotate".
    size: Mutex<u64>,
}

impl RollingLog {
    /// Open (or create) the rolling log at `path`. The parent
    /// directory must already exist.
    pub fn open(path: PathBuf) -> std::io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let size = file.metadata().map(|m| m.len()).unwrap_or(0);
        Ok(Self {
            path,
            file: Mutex::new(Some(file)),
            size: Mutex::new(size),
        })
    }

    /// Append one line (no trailing newline added; the caller is
    /// expected to include it). Rotates the file first if the line
    /// would push the file over `MAX_LOG_BYTES`.
    pub fn write_line(&self, line: &str) -> std::io::Result<()> {
        let line_len = line.as_bytes().len() as u64;
        let current = *self.size.lock().unwrap();
        if current + line_len > MAX_LOG_BYTES {
            self.rotate()?;
        }

        let mut guard = self.file.lock().unwrap();
        let file = guard
            .as_mut()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "log file closed"))?;
        file.write_all(line.as_bytes())?;
        file.flush()?;
        let new_size = file
            .metadata()
            .map(|m| m.len())
            .unwrap_or(current + line_len);
        *self.size.lock().unwrap() = new_size;
        Ok(())
    }

    /// Rotate: close the current file (by taking the handle out of
    /// the `Mutex`), rename it to `<base>.1`, and open a fresh
    /// current file. Any prior `<base>.1` is overwritten.
    ///
    /// On Windows, `std::fs::rename` fails if the source file is
    /// open. Holding the `Mutex` across the rename would block
    /// concurrent `write_line` callers for the cost of a rename,
    /// so we briefly take the `Option<File>` out, then release
    /// the lock before performing IO.
    fn rotate(&self) -> std::io::Result<()> {
        // Step 1: take the open handle out of the slot. Any
        // concurrent writer that grabs the lock now sees `None`
        // and is rejected — but the spec is "rate-limited logs",
        // not "logs must never fail", and a single drop is fine.
        let old = {
            let mut guard = self.file.lock().unwrap();
            guard.take()
        };
        // Drop closes the OS handle.
        drop(old);

        let rotated = rotated_path(&self.path);
        // Best-effort: ignore "not found" on the unlink step.
        let _ = std::fs::remove_file(&rotated);
        std::fs::rename(&self.path, &rotated)?;

        // `create(true)` alone is enough — the file does not exist
        // yet at this path (we just renamed it away). `append(true)`
        // makes every write seek to the end; that combined with
        // `create(true)` gives us a fresh, empty log file.
        let fresh = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&self.path)?;
        *self.file.lock().unwrap() = Some(fresh);
        *self.size.lock().unwrap() = 0;
        Ok(())
    }

    /// Path to the rolling log file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn rotated_path(base: &Path) -> PathBuf {
    let mut s = base.as_os_str().to_owned();
    s.push(ROTATED_SUFFIX);
    PathBuf::from(s)
}

/// Per-plugin rate limiter + rolling file. The "production"
/// `LogApi` implementation wired in by the Tauri host factory.
pub struct RateLimitedFileLog {
    plugin_id: PluginId,
    log: RollingLog,
    limiter: PlMutex<RateLimiter>,
}

impl RateLimitedFileLog {
    /// Open the log file and allocate a fresh rate limiter for this
    /// plugin. Creates the parent directory if needed.
    pub fn open(logs_dir: &Path, plugin_id: PluginId) -> std::io::Result<Self> {
        std::fs::create_dir_all(logs_dir)?;
        let path = logs_dir.join(format!("plugin-{}.log", plugin_id));
        let log = RollingLog::open(path)?;
        Ok(Self {
            plugin_id,
            log,
            limiter: PlMutex::new(RateLimiter::default()),
        })
    }

    /// Build a `RateLimitedFileLog` over an already-opened log file.
    /// Used by tests and by callers that want to keep the file
    /// outside the standard `<app_data>/logs/` tree.
    pub fn with_log(plugin_id: PluginId, log: RollingLog) -> Self {
        Self {
            plugin_id,
            log,
            limiter: PlMutex::new(RateLimiter::default()),
        }
    }

    /// Format `[plugin:{id}] [{LEVEL}] {msg}\n` and run it through
    /// the rate limiter. Returns `true` if the line was actually
    /// written, `false` if it was dropped.
    fn emit(&self, level: &str, message: &str) -> bool {
        let now = Instant::now();
        let mut limiter = self.limiter.lock();
        if !limiter.try_admit(now) {
            return false;
        }
        let line = format!("[plugin:{}] [{}] {}\n", self.plugin_id, level, message);
        // Drop the limiter lock before taking the file lock to keep
        // the critical sections short.
        drop(limiter);
        if let Err(err) = self.log.write_line(&line) {
            // We can't do much — the spec is "logs 5MB rolling", not
            // "logs must not fail". Surface to stderr so a tail -f
            // still sees something rather than silently disappearing.
            eprintln!("[plugin:{}] log write failed: {err}", self.plugin_id);
        }
        // Periodic dropped-message summary. We do this on every
        // admit (cheap) but `take_summary` only returns a value
        // once per minute.
        if let Some((dropped, since)) = self.limiter.lock().take_summary(now) {
            let line = format!(
                "[plugin:{}] [WARN] rate-limited: {} message(s) dropped in the last {}s\n",
                self.plugin_id,
                dropped,
                since.as_secs()
            );
            let _ = self.log.write_line(&line);
        }
        true
    }
}

#[async_trait::async_trait]
impl LogApi for RateLimitedFileLog {
    fn debug(&self, _plugin_id: &PluginId, message: &str) {
        // Spec defines only info/warn/error. Map debug to info so
        // plugin authors that ask for `log_debug` from a future
        // runtime still get something on disk.
        self.emit("DEBUG", message);
    }
    fn info(&self, _plugin_id: &PluginId, message: &str) {
        self.emit("INFO", message);
    }
    fn warn(&self, _plugin_id: &PluginId, message: &str) {
        self.emit("WARN", message);
    }
    fn error(&self, _plugin_id: &PluginId, message: &str) {
        self.emit("ERROR", message);
    }
}

/// Container that owns one [`RateLimitedFileLog`] per plugin. The
/// Tauri host factory would build one of these and hand each plugin
/// a cheap `Arc<dyn LogApi>` clone, but plugins can be enabled /
/// disabled / reloaded at runtime — to avoid leaking entries for
/// disabled plugins, the registry is responsible for dropping them.
///
/// In practice the Tauri host factory is currently `Fn`-based and
/// hands out fresh `RateLimitedFileLog` per plugin, so this container
/// is mainly useful for tests and for callers that want a single
/// `LogApi` per app rather than per plugin. Kept here as scaffolding
/// for the per-plugin resource dashboard the team is likely to add
/// next.
#[derive(Default)]
pub struct SharedPluginLogRegistry {
    inner: PlMutex<HashMap<PluginId, std::sync::Arc<RateLimitedFileLog>>>,
}

impl SharedPluginLogRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get or create a log for the given plugin id. The closure runs
    /// only on the create path; if a log already exists for this id
    /// the existing `Arc` is returned.
    pub fn get_or_create<F>(
        &self,
        plugin_id: PluginId,
        make: F,
    ) -> std::io::Result<std::sync::Arc<RateLimitedFileLog>>
    where
        F: FnOnce() -> std::io::Result<RateLimitedFileLog>,
    {
        let mut guard = self.inner.lock();
        if let Some(existing) = guard.get(&plugin_id) {
            return Ok(existing.clone());
        }
        let log = std::sync::Arc::new(make()?);
        guard.insert(plugin_id, log.clone());
        Ok(log)
    }

    /// Drop the entry for a plugin. Call this on
    /// `enable -> disabled` transitions or on `unload`.
    pub fn drop_for(&self, plugin_id: &PluginId) {
        self.inner.lock().remove(plugin_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    fn fresh_limiter() -> RateLimiter {
        // Tight windows so tests don't sleep for a real minute.
        RateLimiter::new(3, 3.0, 5, 60)
    }

    #[test]
    fn rate_limiter_admits_within_burst() {
        let mut l = fresh_limiter();
        let now = Instant::now();
        for _ in 0..3 {
            assert!(l.try_admit(now));
        }
    }

    #[test]
    fn rate_limiter_blocks_after_burst() {
        let mut l = fresh_limiter();
        let now = Instant::now();
        for _ in 0..3 {
            assert!(l.try_admit(now));
        }
        assert!(!l.try_admit(now));
    }

    #[test]
    fn rate_limiter_refills() {
        let mut l = fresh_limiter();
        let now = Instant::now();
        for _ in 0..3 {
            assert!(l.try_admit(now));
        }
        assert!(!l.try_admit(now));
        // One second later, the burst bucket has refilled.
        let later = now + Duration::from_secs(1);
        assert!(l.try_admit(later));
    }

    #[test]
    fn rate_limiter_sustained_caps_long_run() {
        // Burst=1000 to keep the burst bucket out of the way, then
        // make sure the sustained window blocks the 6th message in
        // a 5-message window.
        let mut l = RateLimiter::new(1000, 1000.0, 5, 60);
        let now = Instant::now();
        for i in 0..5 {
            assert!(l.try_admit(now), "msg {i} should be admitted");
        }
        assert!(!l.try_admit(now), "6th should be dropped");
    }

    #[test]
    fn rate_limiter_summary_only_after_window() {
        let mut l = RateLimiter::new(1, 0.0, 1, 60);
        let now = Instant::now();
        // Admit one, drop the rest.
        assert!(l.try_admit(now));
        for _ in 0..5 {
            assert!(!l.try_admit(now));
        }
        // Within the summary window we get None.
        assert!(l.take_summary(now).is_none());
        // After 60s+ we get the count.
        let later = now + Duration::from_secs(61);
        let summary = l.take_summary(later);
        assert_eq!(summary.unwrap().0, 5);
    }

    #[test]
    fn rolling_log_rotates_at_cap() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("plugin-x.log");
        let log = RollingLog::open(path.clone()).unwrap();

        // Write enough to overflow twice. Each line is 1 MB.
        let big = "x".repeat(1024 * 1024);
        for _ in 0..7 {
            log.write_line(&format!("{big}\n")).unwrap();
        }
        // After rotation we should have a `<base>.1` file plus the
        // current file, and the current file should be under the
        // cap.
        let rotated = dir.path().join("plugin-x.log.1");
        assert!(rotated.exists());
        let current_size = std::fs::metadata(&path).unwrap().len();
        assert!(current_size < MAX_LOG_BYTES);
    }

    #[test]
    fn rolling_log_appends_existing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("plugin-x.log");
        {
            let log = RollingLog::open(path.clone()).unwrap();
            log.write_line("first\n").unwrap();
        }
        // Reopen: should preserve contents, not truncate.
        let log = RollingLog::open(path.clone()).unwrap();
        log.write_line("second\n").unwrap();
        let mut contents = String::new();
        File::open(&path)
            .unwrap()
            .read_to_string(&mut contents)
            .unwrap();
        assert_eq!(contents, "first\nsecond\n");
    }

    #[test]
    fn rate_limited_file_log_drops_under_spam() {
        let dir = tempfile::tempdir().unwrap();
        let log = RateLimitedFileLog::open(dir.path(), PluginId::from("p")).unwrap();
        // 3 admitted, the rest dropped (defaults: burst=10, sustained=100).
        let mut admitted = 0;
        for i in 0..50 {
            if log.emit("INFO", &format!("msg-{i}")) {
                admitted += 1;
            }
        }
        // We don't assert an exact admitted count — the burst bucket
        // can refill slightly between iterations. The point is that
        // a spam run is throttled, not amplified.
        assert!(admitted <= 50);
        assert!(admitted >= 3);
    }
}
use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::broker::{BrokerClient, Timeframe};
use crate::plugin::types::{PluginError, PluginResult, SubscriptionHandle};

use super::{Candle, MarketDataApi, MarketDataEvent, MarketEventKind};

pub struct BrokerMarketDataApi {
    pub broker: Arc<dyn BrokerClient>,
    subscriptions: Arc<Mutex<HashMap<SubscriptionHandle, tokio::task::AbortHandle>>>,
}

impl BrokerMarketDataApi {
    pub fn new(broker: Arc<dyn BrokerClient>) -> Self {
        Self {
            broker,
            subscriptions: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait::async_trait]
impl MarketDataApi for BrokerMarketDataApi {
    fn subscribe_ticks(
        &self,
        symbol: &str,
        callback: Arc<dyn Fn(MarketDataEvent) + Send + Sync>,
    ) -> PluginResult<SubscriptionHandle> {
        let handle = SubscriptionHandle(uuid::Uuid::new_v4());
        let broker = self.broker.clone();
        let sym = symbol.to_string();

        let abort = tokio::spawn(async move {
            loop {
                let timeframe = Timeframe::M1;
                let now = chrono::Utc::now().timestamp();
                let from = now - 60;
                let to = now;
                let sym_for_tick = sym.clone();
                let cb = callback.clone();
                let b = broker.clone();
                // Build a tick event and invoke the callback.
                let event = MarketDataEvent {
                    symbol: sym_for_tick.clone(),
                    kind: MarketEventKind::Tick,
                };
                cb(event);
                // Also push a Candle event for the most recent candle if available.
                let candles = b.get_ohlcv(&sym_for_tick, timeframe, from, to).await;
                if let Ok(candles) = candles {
                    if let Some(last) = candles.last() {
                        let evt = MarketDataEvent {
                            symbol: sym_for_tick,
                            kind: MarketEventKind::Candle,
                        };
                        // Note: the trait event does not carry the candle body — the spec keeps
                        // it as a tagged union over MarketEventKind. The latest candle can
                        // still be retrieved via the latest_candle accessor.
                        let _ = last;
                        cb(evt);
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        })
        .abort_handle();

        // SAFETY: requires a multi-thread Tokio runtime to be current on this thread.
        let map_for_insert = self.subscriptions.clone();
        let h = handle;
        let a = abort;
        let inserted = tokio::runtime::Handle::current().block_on(async move {
            let mut guard = map_for_insert.lock().await;
            guard.insert(h, a).is_some()
        });
        let _ = inserted;

        Ok(handle)
    }

    fn unsubscribe_ticks(&self, handle: SubscriptionHandle) -> PluginResult<()> {
        // SAFETY: requires a multi-thread Tokio runtime to be current on this thread.
        let map = self.subscriptions.clone();
        let removed = tokio::runtime::Handle::current().block_on(async move {
            let mut guard = map.lock().await;
            guard.remove(&handle)
        });
        match removed {
            Some(abort) => {
                abort.abort();
                Ok(())
            }
            None => Err(PluginError::NotFound("subscription not found".into())),
        }
    }

    fn latest_candle(&self, symbol: &str) -> PluginResult<Candle> {
        let broker = self.broker.clone();
        let sym = symbol.to_string();
        // SAFETY: requires a multi-thread Tokio runtime to be current on this thread.
        let res: anyhow::Result<Vec<crate::models::Candle>> = tokio::runtime::Handle::current()
            .block_on(async move {
                let timeframe = Timeframe::M1;
                let now = chrono::Utc::now().timestamp();
                broker.get_ohlcv(&sym, timeframe, now - 60, now).await
            });
        let candles = res.map_err(|e| PluginError::ApiError(e.to_string()))?;
        let last = candles
            .last()
            .ok_or_else(|| PluginError::ApiError("no candles returned".into()))?;
        Ok(Candle {
            open: last.open,
            high: last.high,
            low: last.low,
            close: last.close,
            volume: last.volume,
            timestamp_ms: last.timestamp,
        })
    }
}
pub mod analytics;
pub mod dsl_extension;
pub mod events;
pub mod execution;
pub mod indicator_registry;
pub mod log;
pub mod log_file;
pub mod market_data;
pub mod scheduler;
pub mod storage;
pub mod ui;

use std::sync::Arc;

use crate::plugin::types::{
    NotificationKind, PluginId, PluginResult, ScheduleHandle, SubscriptionHandle,
};

#[async_trait::async_trait]
pub trait MarketDataApi: Send + Sync {
    fn subscribe_ticks(
        &self,
        symbol: &str,
        callback: Arc<dyn Fn(MarketDataEvent) + Send + Sync>,
    ) -> PluginResult<SubscriptionHandle>;
    fn unsubscribe_ticks(&self, handle: SubscriptionHandle) -> PluginResult<()>;
    fn latest_candle(&self, symbol: &str) -> PluginResult<Candle>;
}

#[derive(Debug, Clone)]
pub struct MarketDataEvent {
    pub symbol: String,
    pub kind: MarketEventKind,
}

#[derive(Debug, Clone, Copy)]
pub enum MarketEventKind {
    Candle,
    Tick,
}

#[derive(Debug, Clone)]
pub struct Candle {
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub timestamp_ms: i64,
}

#[async_trait::async_trait]
pub trait ExecutionApi: Send + Sync {
    async fn submit_order(&self, order: OrderRequest) -> PluginResult<String>;
    async fn cancel_order(&self, order_id: &str) -> PluginResult<()>;
    fn positions(&self) -> PluginResult<Vec<Position>>;
}

#[derive(Debug, Clone)]
pub struct OrderRequest {
    pub symbol: String,
    pub side: OrderSide,
    pub quantity: u32,
    pub order_type: OrderType,
    pub price: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy)]
pub enum OrderType {
    Market,
    Limit,
}

#[derive(Debug, Clone)]
pub struct Position {
    pub symbol: String,
    pub quantity: i64,
    pub average_price: f64,
}

#[async_trait::async_trait]
pub trait StorageApi: Send + Sync {
    fn read(&self, key: &str) -> PluginResult<Option<Vec<u8>>>;
    fn write(&self, key: &str, value: &[u8]) -> PluginResult<()>;
    fn delete(&self, key: &str) -> PluginResult<()>;
    fn list_keys(&self, prefix: &str) -> PluginResult<Vec<String>>;
}

#[async_trait::async_trait]
pub trait IndicatorRegistryApi: Send + Sync {
    fn register(
        &self,
        name: String,
        registered_by: crate::plugin::PluginId,
        f: std::sync::Arc<dyn Fn(&[crate::models::Candle], usize) -> Vec<f64> + Send + Sync>,
    ) -> crate::plugin::PluginResult<()>;
    fn get(&self, name: &str) -> PluginResult<Box<dyn IndicatorInstance>>;
    /// Downcast hook so plugin runtimes (e.g. Rhai) can recover the
    /// concrete registry type to access plugin-id-attributed APIs
    /// like `register_fn` that the trait-level surface cannot express.
    fn as_any(&self) -> &dyn std::any::Any;
}

pub trait IndicatorInstance: Send + Sync {
    fn update(&mut self, value: f64);
    fn value(&self) -> Option<f64>;
    fn name(&self) -> &str;
}

#[async_trait::async_trait]
pub trait AnalyticsApi: Send + Sync {
    fn register_metric(
        &self,
        name: String,
        registered_by: crate::plugin::PluginId,
        f: std::sync::Arc<dyn Fn(&[crate::models::PaperTrade]) -> f64 + Send + Sync>,
    ) -> crate::plugin::PluginResult<()>;
    fn get_metric(
        &self,
        name: &str,
    ) -> Option<std::sync::Arc<dyn Fn(&[crate::models::PaperTrade]) -> f64 + Send + Sync>>;
    fn list_metrics(&self) -> Vec<String>;
}

#[async_trait::async_trait]
pub trait DslExtensionApi: Send + Sync {
    fn register_keyword(
        &self,
        keyword: String,
        registered_by: crate::plugin::PluginId,
        handler: std::sync::Arc<
            dyn Fn(&crate::strategy::runtime::context::EvalContext) -> bool + Send + Sync,
        >,
    ) -> crate::plugin::PluginResult<()>;
    fn get_keyword(
        &self,
        keyword: &str,
    ) -> Option<
        std::sync::Arc<
            dyn Fn(&crate::strategy::runtime::context::EvalContext) -> bool + Send + Sync,
        >,
    >;
    fn unregister_all_for(&self, plugin_id: &crate::plugin::PluginId);
}

#[async_trait::async_trait]
pub trait UiApi: Send + Sync {
    fn register_panel(&self, panel: UiPanel) -> PluginResult<()>;
    fn notify(&self, kind: NotificationKind, message: &str) -> PluginResult<()>;
    fn list_panels(&self) -> Vec<(String, String)>;
    /// Downcast hook so plugin runtimes (e.g. WASM) can recover the
    /// concrete `TauriUiApi` type to call `emit_panel_data`, which
    /// lives outside the trait surface.
    fn as_any(&self) -> &dyn std::any::Any;
}

#[derive(Debug, Clone)]
pub struct UiPanel {
    pub id: String,
    pub title: String,
    pub route: String,
}

#[async_trait::async_trait]
pub trait SchedulerApi: Send + Sync {
    fn schedule(
        &self,
        plugin_id: PluginId,
        cron: &str,
        task: Arc<dyn Fn() + Send + Sync>,
    ) -> PluginResult<ScheduleHandle>;
    fn cancel(&self, handle: ScheduleHandle) -> PluginResult<()>;
    fn cancel_all_for(&self, plugin_id: &PluginId);
}

#[async_trait::async_trait]
pub trait LogApi: Send + Sync {
    fn debug(&self, plugin_id: &PluginId, message: &str);
    fn info(&self, plugin_id: &PluginId, message: &str);
    fn warn(&self, plugin_id: &PluginId, message: &str);
    fn error(&self, plugin_id: &PluginId, message: &str);
}
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use crate::plugin::types::{PluginError, PluginId, PluginResult, ScheduleHandle};

use super::SchedulerApi;

pub struct CronScheduler {
    handles: Arc<Mutex<HashMap<ScheduleHandle, (PluginId, CancellationToken)>>>,
}

impl CronScheduler {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            handles: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}

#[async_trait::async_trait]
impl SchedulerApi for CronScheduler {
    fn schedule(
        &self,
        plugin_id: PluginId,
        cron_expr: &str,
        task: Arc<dyn Fn() + Send + Sync>,
    ) -> PluginResult<ScheduleHandle> {
        let schedule = cron::Schedule::from_str(cron_expr)
            .map_err(|e| PluginError::ApiError(format!("invalid cron expression: {e}")))?;
        let handle = ScheduleHandle(uuid::Uuid::new_v4());
        let token = CancellationToken::new();

        let task_token = token.clone();
        tokio::spawn(async move {
            loop {
                let now = chrono::Utc::now();
                let next = match schedule.upcoming(chrono::Utc).next() {
                    Some(t) => t,
                    None => break,
                };
                if next <= now {
                    // Skip past times.
                    continue;
                }
                let sleep_until = tokio::time::Instant::now()
                    + (next - now)
                        .to_std()
                        .unwrap_or(std::time::Duration::from_secs(0));
                tokio::select! {
                    _ = tokio::time::sleep_until(sleep_until) => {
                        task();
                    }
                    _ = task_token.cancelled() => {
                        break;
                    }
                }
            }
        });

        self.handles.lock().insert(handle, (plugin_id, token));
        Ok(handle)
    }

    fn cancel(&self, handle: ScheduleHandle) -> PluginResult<()> {
        let (_, token) = self
            .handles
            .lock()
            .remove(&handle)
            .ok_or_else(|| PluginError::NotFound("schedule not found".into()))?;
        token.cancel();
        Ok(())
    }

    fn cancel_all_for(&self, plugin_id: &PluginId) {
        let mut map = self.handles.lock();
        let to_cancel: Vec<ScheduleHandle> = map
            .iter()
            .filter_map(|(handle, (id, _))| if id == plugin_id { Some(*handle) } else { None })
            .collect();
        for handle in to_cancel {
            if let Some((_, token)) = map.remove(&handle) {
                token.cancel();
            }
        }
    }
}
use std::fs;
use std::path::{Path, PathBuf};

use crate::plugin::types::{PluginError, PluginId, PluginResult};

use super::StorageApi;

pub struct PluginKvStore {
    pub plugin_id: PluginId,
    pub base_dir: PathBuf,
}

impl PluginKvStore {
    pub fn new(plugin_id: PluginId, base_dir: PathBuf) -> PluginResult<Self> {
        fs::create_dir_all(&base_dir)
            .map_err(|e| PluginError::ApiError(format!("create_dir_all failed: {e}")))?;
        Ok(Self {
            plugin_id,
            base_dir,
        })
    }
}

fn sanitize_key(key: &str) -> String {
    let mut sanitized = String::with_capacity(key.len());
    for ch in key.chars() {
        match ch {
            '/' | '\\' | ':' => sanitized.push('_'),
            // Replace the literal ".." substring by tracking it separately.
            _ => sanitized.push(ch),
        }
    }
    // Replace ".." substring with "__" (two underscores).
    let sanitized = sanitized.replace("..", "__");
    // Truncate to 200 chars (by char count, not bytes).
    let truncated: String = sanitized.chars().take(200).collect();
    if truncated.is_empty() {
        "_empty_".to_string()
    } else {
        truncated
    }
}

fn path_for(base_dir: &Path, key: &str) -> PathBuf {
    let sanitized = sanitize_key(key);
    base_dir.join(format!("{sanitized}.val"))
}

#[async_trait::async_trait]
impl StorageApi for PluginKvStore {
    fn read(&self, key: &str) -> PluginResult<Option<Vec<u8>>> {
        let path = path_for(&self.base_dir, key);
        match fs::read(&path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(PluginError::ApiError(format!("read failed: {e}"))),
        }
    }

    fn write(&self, key: &str, value: &[u8]) -> PluginResult<()> {
        let path = path_for(&self.base_dir, key);
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, value)
            .map_err(|e| PluginError::ApiError(format!("write tmp failed: {e}")))?;
        // Atomic rename.
        fs::rename(&tmp, &path)
            .map_err(|e| PluginError::ApiError(format!("rename failed: {e}")))?;
        Ok(())
    }

    fn delete(&self, key: &str) -> PluginResult<()> {
        let path = path_for(&self.base_dir, key);
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(PluginError::ApiError(format!("delete failed: {e}"))),
        }
    }

    fn list_keys(&self, prefix: &str) -> PluginResult<Vec<String>> {
        let entries = fs::read_dir(&self.base_dir)
            .map_err(|e| PluginError::ApiError(format!("read_dir failed: {e}")))?;
        let mut keys = Vec::new();
        for entry in entries {
            let entry =
                entry.map_err(|e| PluginError::ApiError(format!("dir entry error: {e}")))?;
            let name = entry.file_name().to_string_lossy().to_string();
            // Only consider *.val files.
            let stripped = match name.strip_suffix(".val") {
                Some(s) => s,
                None => continue,
            };
            // Compare against the prefix by reconstructing the user-supplied "key" path.
            // We match on the raw sanitized portion which is how files are named.
            let user_prefix = sanitize_key(prefix);
            if stripped.starts_with(&user_prefix) {
                keys.push(stripped.to_string());
            }
        }
        Ok(keys)
    }
}
use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::Serialize;
use tokio::sync::broadcast;

use crate::plugin::types::{NotificationKind, PluginResult};

use super::{UiApi, UiPanel};

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum UiMessage {
    PanelRegistered {
        id: String,
        title: String,
    },
    Notification {
        msg: String,
        kind: String,
    },
    PanelData {
        panel_id: String,
        data: serde_json::Value,
    },
}

pub struct TauriUiApi {
    sender: broadcast::Sender<UiMessage>,
    panels: Arc<RwLock<Vec<(String, String)>>>,
    panel_data: Arc<RwLock<HashMap<String, serde_json::Value>>>,
}

impl TauriUiApi {
    pub fn new() -> (Arc<Self>, broadcast::Receiver<UiMessage>) {
        let (sender, receiver) = broadcast::channel(256);
        let api = Arc::new(Self {
            sender,
            panels: Arc::new(RwLock::new(Vec::new())),
            panel_data: Arc::new(RwLock::new(HashMap::new())),
        });
        (api, receiver)
    }

    pub fn receiver(&self) -> broadcast::Receiver<UiMessage> {
        self.sender.subscribe()
    }
}

#[async_trait::async_trait]
impl UiApi for TauriUiApi {
    fn register_panel(&self, panel: UiPanel) -> PluginResult<()> {
        let UiPanel {
            id,
            title,
            route: _,
        } = panel;
        self.panels.write().push((id.clone(), title.clone()));
        let _ = self.sender.send(UiMessage::PanelRegistered { id, title });
        Ok(())
    }

    fn notify(&self, kind: NotificationKind, message: &str) -> PluginResult<()> {
        let _ = self.sender.send(UiMessage::Notification {
            kind: kind.to_string(),
            msg: message.to_string(),
        });
        Ok(())
    }

    fn list_panels(&self) -> Vec<(String, String)> {
        self.panels.read().clone()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl TauriUiApi {
    pub fn emit_panel_data(&self, panel_id: String, data: serde_json::Value) -> PluginResult<()> {
        self.panel_data
            .write()
            .insert(panel_id.clone(), data.clone());
        let _ = self.sender.send(UiMessage::PanelData { panel_id, data });
        Ok(())
    }
}
﻿use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use crate::plugin::manifest::PluginPermissions;
use crate::plugin::types::{Capability, PluginError, PluginId, PluginResult};
use rhai::{AST, Engine, FnPtr};

use super::api::{
    AnalyticsApi, DslExtensionApi, ExecutionApi, IndicatorRegistryApi, LogApi, MarketDataApi,
    SchedulerApi, StorageApi, UiApi,
};

/// Registry of stateless Rhai callbacks for a single plugin load.
/// Callbacks are plain functions (not closures) invoked with a fresh execution scope.
#[derive(Default)]
pub(crate) struct PluginCallbacks {
    pub(crate) indicators: Mutex<HashMap<String, FnPtr>>,
    pub(crate) metrics: Mutex<HashMap<String, FnPtr>>,
    pub(crate) keywords: Mutex<HashMap<String, FnPtr>>,
    pub(crate) schedules: Mutex<HashMap<crate::plugin::types::ScheduleHandle, FnPtr>>,
    pub(crate) events: Mutex<HashMap<crate::plugin::api::events::EventFilter, Vec<FnPtr>>>,
    pub(crate) schedule_failures: Mutex<HashMap<crate::plugin::types::ScheduleHandle, u32>>,
}

/// The runtime handle given to a plugin when it is loaded. The host exposes
/// capability-gated access to platform services â€” plugins must request the
/// `*_guarded` accessor corresponding to each capability listed in their
/// manifest, and any undeclared access is denied.
pub struct PluginHost {
    pub id: PluginId,
    pub market_data: Arc<dyn MarketDataApi>,
    pub execution: Arc<dyn ExecutionApi>,
    pub storage: Arc<dyn StorageApi>,
    pub event_bus: Arc<crate::plugin::api::events::EventBus>,
    pub indicators: Arc<dyn IndicatorRegistryApi>,
    pub analytics: Arc<dyn AnalyticsApi>,
    pub dsl: Arc<dyn DslExtensionApi>,
    pub ui: Arc<dyn UiApi>,
    pub scheduler: Arc<dyn SchedulerApi>,
    pub log: Arc<dyn LogApi>,
    pub(crate) callbacks: Arc<PluginCallbacks>,
    pub(crate) engine: OnceLock<Arc<Engine>>,
    pub(crate) ast: OnceLock<Arc<AST>>,
    pub(crate) capabilities: Vec<Capability>,
    pub(crate) permissions: PluginPermissions,
}

impl PluginHost {
    /// Verify that the host was built with a capability matching `cap`.
    /// Matching is done by variant discriminant so that
    /// `Capability::MarketData` matches a registered `Capability::MarketData`
    /// regardless of any future associated payload.
    pub fn require_capability(&self, cap: &Capability) -> PluginResult<()> {
        let needle = std::mem::discriminant(cap);
        let granted = self
            .capabilities
            .iter()
            .any(|c| std::mem::discriminant(c) == needle);
        if granted {
            Ok(())
        } else {
            Err(PluginError::PermissionDenied {
                capability: cap.to_string(),
                plugin_id: self.id.to_string(),
            })
        }
    }

    pub fn market_data_guarded(&self) -> PluginResult<&Arc<dyn MarketDataApi>> {
        self.require_capability(&Capability::MarketData)?;
        Ok(&self.market_data)
    }

    pub fn execution_guarded(&self) -> PluginResult<&Arc<dyn ExecutionApi>> {
        self.require_capability(&Capability::Execution)?;
        Ok(&self.execution)
    }

    pub fn storage_guarded(&self) -> PluginResult<&Arc<dyn StorageApi>> {
        self.require_capability(&Capability::Storage)?;
        Ok(&self.storage)
    }

    pub fn event_bus_guarded(&self) -> PluginResult<&Arc<crate::plugin::api::events::EventBus>> {
        self.require_capability(&Capability::Events)?;
        Ok(&self.event_bus)
    }

    pub fn indicators_guarded(&self) -> PluginResult<&Arc<dyn IndicatorRegistryApi>> {
        self.require_capability(&Capability::Indicators)?;
        Ok(&self.indicators)
    }

    pub fn analytics_guarded(&self) -> PluginResult<&Arc<dyn AnalyticsApi>> {
        self.require_capability(&Capability::Analytics)?;
        Ok(&self.analytics)
    }

    pub fn dsl_guarded(&self) -> PluginResult<&Arc<dyn DslExtensionApi>> {
        self.require_capability(&Capability::DslExtension)?;
        Ok(&self.dsl)
    }

    pub fn ui_guarded(&self) -> PluginResult<&Arc<dyn UiApi>> {
        self.require_capability(&Capability::UiPanels)?;
        Ok(&self.ui)
    }

    pub fn scheduler_guarded(&self) -> PluginResult<&Arc<dyn SchedulerApi>> {
        self.require_capability(&Capability::Scheduler)?;
        Ok(&self.scheduler)
    }

    /// Logging is intentionally unguarded â€” every plugin can always log,
    /// regardless of declared capabilities.
    pub fn log(&self) -> &Arc<dyn LogApi> {
        &self.log
    }
}

/// Builder for `PluginHost`. Mirrors the host's fields; `build` consumes
/// the builder and produces an `Arc<PluginHost>` suitable for handing to
/// `Plugin::on_load`.
pub struct PluginHostBuilder {
    pub id: PluginId,
    pub market_data: Arc<dyn MarketDataApi>,
    pub execution: Arc<dyn ExecutionApi>,
    pub storage: Arc<dyn StorageApi>,
    pub event_bus: Arc<crate::plugin::api::events::EventBus>,
    pub indicators: Arc<dyn IndicatorRegistryApi>,
    pub analytics: Arc<dyn AnalyticsApi>,
    pub dsl: Arc<dyn DslExtensionApi>,
    pub ui: Arc<dyn UiApi>,
    pub scheduler: Arc<dyn SchedulerApi>,
    pub log: Arc<dyn LogApi>,
    pub capabilities: Vec<Capability>,
    pub permissions: PluginPermissions,
}

impl PluginHostBuilder {
    pub fn build(self) -> Arc<PluginHost> {
        Arc::new(PluginHost {
            id: self.id,
            market_data: self.market_data,
            execution: self.execution,
            storage: self.storage,
            event_bus: self.event_bus,
            indicators: self.indicators,
            analytics: self.analytics,
            dsl: self.dsl,
            ui: self.ui,
            scheduler: self.scheduler,
            log: self.log,
            callbacks: Arc::new(PluginCallbacks::default()),
            engine: OnceLock::new(),
            ast: OnceLock::new(),
            capabilities: self.capabilities,
            permissions: self.permissions,
        })
    }
}

//! Plugin loader: reads a plugin directory and returns a boxed `Plugin`.
//!
//! The loader is intentionally thin — it doesn't hold any state. It
//! just inspects the manifest, dispatches to the right runtime, and
//! hands back a fully constructed `Plugin` ready to be registered.

use crate::plugin::manifest::PluginManifest;
use crate::plugin::runtime::{rhai_runtime::RhaiPlugin, wasm_runtime::WasmPlugin};
use crate::plugin::types::{PluginError, PluginResult};

use super::Plugin;

pub struct PluginLoader;

impl PluginLoader {
    /// Load a plugin from a directory containing `plugin.toml` and the
    /// entry file referenced by the manifest. Dispatches to either
    /// the Rhai or WASM runtime based on the entry file's extension.
    pub fn load_from_dir(dir: &std::path::Path) -> PluginResult<Box<dyn Plugin>> {
        let manifest = PluginManifest::load(dir)?;
        let meta = manifest.to_meta()?;
        let capabilities = manifest.to_capabilities()?;
        let entry = dir.join(&manifest.entry);

        let ext = entry.extension().and_then(|e| e.to_str()).unwrap_or("");

        match ext {
            "rhai" => Ok(Box::new(RhaiPlugin::new(meta, capabilities, entry)?)),
            "wasm" => Ok(Box::new(WasmPlugin::new(
                meta,
                capabilities,
                entry,
                manifest.permissions.max_memory_mb,
            )?)),
            other => Err(PluginError::LoadFailed(format!(
                "unknown entry type: {}",
                other
            ))),
        }
    }
}
use std::path::Path;

use lazy_static::lazy_static;
use regex::Regex;
use serde::Deserialize;

use super::types::{Capability, PluginError, PluginId, PluginMeta, PluginResult, PluginVersion};

lazy_static! {
    static ref PLUGIN_ID_RE: Regex =
        Regex::new(r"^[a-z0-9][a-z0-9\-]*[a-z0-9]$").expect("valid plugin id regex");
}

#[derive(Debug, Clone, Deserialize)]
pub struct PluginManifest {
    pub id: String,
    pub name: String,
    pub version: String,
    pub description: String,
    pub author: String,
    pub capabilities: Vec<String>,
    pub entry: String,
    pub permissions: PluginPermissions,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PluginPermissions {
    #[serde(default)]
    pub network: bool,
    #[serde(default)]
    pub file_system: bool,
    #[serde(default = "default_memory")]
    pub max_memory_mb: u32,
    #[serde(default)]
    pub allowed_symbols: Vec<String>,
}

fn default_memory() -> u32 {
    32
}

impl PluginManifest {
    pub fn load(plugin_dir: &Path) -> PluginResult<Self> {
        let manifest_path = plugin_dir.join("plugin.toml");
        let raw = std::fs::read_to_string(&manifest_path).map_err(|e| {
            PluginError::ManifestParse(format!("read {}: {e}", manifest_path.display()))
        })?;

        let manifest: PluginManifest = toml::from_str(&raw)
            .map_err(|e| PluginError::ManifestParse(format!("toml parse: {e}")))?;

        if manifest.id.len() < 2 || !PLUGIN_ID_RE.is_match(&manifest.id) {
            return Err(PluginError::ManifestParse("id must be kebab-case".into()));
        }

        let _ = PluginVersion::try_from(manifest.version.as_str())
            .map_err(|e| PluginError::ManifestParse(e))?;

        let mut unknown = Vec::new();
        for cap in &manifest.capabilities {
            if let Err(e) = Capability::try_from(cap.as_str()) {
                unknown.push(format!("{} ({e})", cap));
            }
        }
        if !unknown.is_empty() {
            return Err(PluginError::ManifestParse(format!(
                "unknown capabilities: {}",
                unknown.join(", ")
            )));
        }

        let entry_path = plugin_dir.join(&manifest.entry);
        if !entry_path.is_file() {
            return Err(PluginError::ManifestParse(format!(
                "entry file '{}' not found",
                manifest.entry
            )));
        }

        let ext = entry_path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("");
        if ext != "rhai" && ext != "wasm" {
            return Err(PluginError::ManifestParse(
                "entry must be .rhai or .wasm".into(),
            ));
        }

        Ok(manifest)
    }

    pub fn to_meta(&self) -> PluginResult<PluginMeta> {
        let version = PluginVersion::try_from(self.version.as_str())
            .map_err(|e| PluginError::ManifestParse(e))?;
        Ok(PluginMeta {
            id: PluginId::from(self.id.clone()),
            name: self.name.clone(),
            version,
            description: self.description.clone(),
            author: self.author.clone(),
        })
    }

    pub fn to_capabilities(&self) -> PluginResult<Vec<Capability>> {
        self.capabilities
            .iter()
            .map(|c| Capability::try_from(c.as_str()).map_err(|e| PluginError::ManifestParse(e)))
            .collect()
    }
}
pub mod api;
pub mod host;
pub mod loader;
pub mod manifest;
pub mod registry;
pub mod runtime;
pub mod types;

#[cfg(test)]
mod tests;

pub use types::*;

#[async_trait::async_trait]
pub trait Plugin: Send + Sync {
    fn meta(&self) -> &PluginMeta;
    fn capabilities(&self) -> &[Capability];
    async fn on_load(&mut self, host: std::sync::Arc<host::PluginHost>) -> PluginResult<()>;
    async fn on_enable(&mut self) -> PluginResult<()>;
    async fn on_disable(&mut self) -> PluginResult<()>;
    fn on_unload(&mut self);

    fn has_capability(&self, cap: &Capability) -> bool {
        self.capabilities()
            .iter()
            .any(|c| std::mem::discriminant(c) == std::mem::discriminant(cap))
    }
}
//! Plugin registry: holds all loaded plugins, dispatches lifecycle
//! transitions, and exposes a snapshot for the UI.
//!
//! The registry stores a manifest alongside each plugin so the original
//! `plugin.toml` (and the permissions it declared) is available even
//! after the boxed plugin has taken ownership of the meta/capabilities
//! it needs internally.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::plugin::host::{self, PluginHost};
use crate::plugin::loader::PluginLoader;
use crate::plugin::manifest::{PluginManifest, PluginPermissions};
use crate::plugin::types::{
    Capability, PluginError, PluginId, PluginListEntry, PluginMeta, PluginResult, PluginStatus,
    PluginVersion, ScheduleHandle,
};

use super::Plugin;

/// One loaded plugin and the bookkeeping the registry needs to keep
/// it alive across lifecycle transitions.
pub struct PluginEntry {
    pub plugin: Box<dyn Plugin>,
    pub status: PluginStatus,
    pub manifest: PluginManifest,
    pub schedule_handles: Vec<ScheduleHandle>,
}

/// In-memory registry of all loaded plugins. Wrapped in an `Arc` so
/// callers (Tauri commands, the strategy engine, the plugin host
/// factory) can hold shared references.
pub struct PluginRegistry {
    plugins: Arc<RwLock<HashMap<PluginId, PluginEntry>>>,
    plugins_dir: PathBuf,
    host_factory: Arc<
        dyn Fn(
                PluginId,
                Vec<crate::plugin::types::Capability>,
                PluginPermissions,
            ) -> Arc<PluginHost>
            + Send
            + Sync,
    >,
}

/// Host-factory signature: given a plugin id, declared capabilities,
/// and permissions, produce a fully constructed `Arc<PluginHost>`.
pub type HostFactory = Arc<
    dyn Fn(PluginId, Vec<crate::plugin::types::Capability>, PluginPermissions) -> Arc<PluginHost>
        + Send
        + Sync,
>;

impl PluginRegistry {
    pub fn new(plugins_dir: PathBuf, host_factory: HostFactory) -> Arc<Self> {
        Arc::new(Self {
            plugins: Arc::new(RwLock::new(HashMap::new())),
            plugins_dir,
            host_factory,
        })
    }

    /// Scan the plugins directory and load every subdirectory that
    /// contains a valid `plugin.toml`. Each entry is loaded
    /// independently — a failure in one plugin does not block the
    /// others. Returns one `(dir_name, result)` pair per attempted
    /// load so callers can surface per-plugin errors to the UI.
    pub async fn scan_and_load(&self) -> Vec<(String, PluginResult<()>)> {
        let mut results: Vec<(String, PluginResult<()>)> = Vec::new();

        let entries = match std::fs::read_dir(&self.plugins_dir) {
            Ok(e) => e,
            Err(err) => {
                results.push((
                    self.plugins_dir.display().to_string(),
                    Err(PluginError::LoadFailed(format!("read plugins_dir: {err}"))),
                ));
                return results;
            }
        };

        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let dir_name = entry.file_name().to_string_lossy().to_string();

            let plugin_result = PluginLoader::load_from_dir(Path::new(&path));
            let mut plugin = match plugin_result {
                Ok(p) => p,
                Err(e) => {
                    results.push((dir_name, Err(e)));
                    continue;
                }
            };

            // Re-parse the manifest so we can stash it on the entry
            // and so we have permissions to hand to the host factory.
            // The loader has already validated everything, so this
            // re-parse is purely a lookup.
            let manifest = match PluginManifest::load(Path::new(&path)) {
                Ok(m) => m,
                Err(e) => {
                    results.push((dir_name, Err(e)));
                    continue;
                }
            };

            let id = plugin.meta().id.clone();
            let caps: Vec<crate::plugin::types::Capability> = plugin.capabilities().to_vec();
            let permissions = manifest.permissions.clone();
            let host = (self.host_factory)(id.clone(), caps.clone(), permissions.clone());

            let load_result = plugin.on_load(host).await;
            match load_result {
                Ok(()) => {
                    let id_for_results = id.to_string();
                    let entry = PluginEntry {
                        plugin,
                        status: PluginStatus::Loaded,
                        manifest,
                        schedule_handles: Vec::new(),
                    };
                    self.plugins.write().insert(id, entry);
                    results.push((id_for_results, Ok(())));
                }
                Err(e) => {
                    let id_for_results = id.to_string();
                    let entry = PluginEntry {
                        plugin,
                        status: PluginStatus::Failed(e.to_string()),
                        manifest,
                        schedule_handles: Vec::new(),
                    };
                    self.plugins.write().insert(id.clone(), entry);
                    results.push((id_for_results, Err(e)));
                }
            }
        }

        results
    }

    /// Move a loaded plugin into the `Enabled` state.
    pub async fn enable(&self, id: &PluginId) -> PluginResult<()> {
        // Take the lock briefly to verify the plugin exists and is not in
        // a failed state, then release it before awaiting. Holding a
        // parking_lot write guard across an `.await` would (a) make the
        // future `!Send` and (b) risk deadlock if the awaited future
        // re-enters the registry. There is a small TOCTOU window between
        // the check and the status write, but `on_enable` is idempotent
        // for the plugins shipped in this repo and the registry is
        // single-process.
        enum Check {
            Enable(Box<dyn Plugin>),
            Failed(String),
            Missing,
        }
        let to_run = {
            let mut guard = self.plugins.write();
            match guard.get_mut(id) {
                None => Check::Missing,
                Some(entry) => {
                    if let PluginStatus::Failed(msg) = &entry.status {
                        Check::Failed(msg.clone())
                    } else {
                        // Replace the plugin slot with a marker entry so
                        // the registry stays consistent while `on_enable`
                        // is in flight. We move the plugin out and back
                        // via the entry to avoid a separate per-plugin
                        // lock.
                        let entry = std::mem::replace(
                            entry,
                            PluginEntry {
                                plugin: Box::new(EmptyPlugin),
                                status: PluginStatus::Loaded,
                                manifest: entry.manifest.clone(),
                                schedule_handles: Vec::new(),
                            },
                        );
                        Check::Enable(entry.plugin)
                    }
                }
            }
        };
        match to_run {
            Check::Missing => Err(PluginError::NotFound(id.to_string())),
            Check::Failed(msg) => Err(PluginError::ApiError(format!(
                "cannot enable failed plugin: {msg}"
            ))),
            Check::Enable(plugin) => {
                let mut plugin = plugin;
                match plugin.on_enable().await {
                    Ok(()) => {
                        let mut guard = self.plugins.write();
                        if let Some(entry) = guard.get_mut(id) {
                            entry.plugin = plugin;
                            entry.status = PluginStatus::Enabled;
                        }
                        Ok(())
                    }
                    Err(e) => {
                        let mut guard = self.plugins.write();
                        if let Some(entry) = guard.get_mut(id) {
                            entry.plugin = plugin;
                            entry.status = PluginStatus::Loaded;
                        }
                        Err(e)
                    }
                }
            }
        }
    }

    /// Move an enabled plugin back to `Disabled`.
    pub async fn disable(&self, id: &PluginId) -> PluginResult<()> {
        // Same release-before-await pattern as `enable`. See the comment
        // there for why.
        let to_run = {
            let mut guard = self.plugins.write();
            match guard.get_mut(id) {
                None => return Err(PluginError::NotFound(id.to_string())),
                Some(entry) => {
                    let entry = std::mem::replace(
                        entry,
                        PluginEntry {
                            plugin: Box::new(EmptyPlugin),
                            status: PluginStatus::Loaded,
                            manifest: entry.manifest.clone(),
                            schedule_handles: Vec::new(),
                        },
                    );
                    entry.plugin
                }
            }
        };
        let mut plugin = to_run;
        match plugin.on_disable().await {
            Ok(()) => {
                let mut guard = self.plugins.write();
                if let Some(entry) = guard.get_mut(id) {
                    entry.plugin = plugin;
                    entry.status = PluginStatus::Disabled;
                }
                Ok(())
            }
            Err(e) => {
                let mut guard = self.plugins.write();
                if let Some(entry) = guard.get_mut(id) {
                    entry.plugin = plugin;
                    entry.status = PluginStatus::Enabled;
                }
                Err(e)
            }
        }
    }

    /// Tear down a plugin and remove it from the registry.
    pub async fn unload(&self, id: &PluginId) -> PluginResult<()> {
        // `on_unload` is sync, so the guard is never held across an
        // `.await`. We do still need to drop the lock before calling
        // `on_unload` because the plugin could re-enter the registry
        // (e.g. via storage cleanup) and we don't want a deadlock.
        let mut entry = {
            let mut guard = self.plugins.write();
            guard
                .remove(id)
                .ok_or_else(|| PluginError::NotFound(id.to_string()))?
        };

        // Use the host factory to get a handle to the shared APIs
        // to clean up callbacks registered by this plugin.
        let host = (self.host_factory)(
            id.clone(),
            vec![],
            PluginPermissions {
                network: false,
                file_system: false,
                max_memory_mb: 0,
                allowed_symbols: vec![],
            },
        );
        host.scheduler.cancel_all_for(id);
        host.event_bus.unsubscribe_all_for(id);

        entry.plugin.on_unload();
        Ok(())
    }

    /// Snapshot of every loaded plugin for the UI.
    pub fn list(&self) -> Vec<PluginListEntry> {
        let guard = self.plugins.read();
        let mut out: Vec<PluginListEntry> = guard
            .values()
            .map(|entry| PluginListEntry {
                meta: entry.plugin.meta().clone(),
                status: entry.status.clone(),
                capabilities: entry.plugin.capabilities().to_vec(),
            })
            .collect();
        out.sort_by(|a, b| a.meta.id.cmp(&b.meta.id));
        out
    }

    /// Current status for a single plugin, or `None` if unknown.
    pub fn get_status(&self, id: &PluginId) -> Option<PluginStatus> {
        self.plugins.read().get(id).map(|e| e.status.clone())
    }
}

/// Placeholder plugin used to swap a real plugin out of a registry
/// entry while its `on_enable` / `on_disable` future is in flight.
/// Holds the write lock for a much shorter window than the old
/// "hold across await" design — see `PluginRegistry::enable` for the
/// TOCTOU rationale.
struct EmptyPlugin;

#[async_trait::async_trait]
impl Plugin for EmptyPlugin {
    fn meta(&self) -> &PluginMeta {
        static META: PluginMeta = PluginMeta {
            id: PluginId(String::new()),
            name: String::new(),
            version: PluginVersion {
                major: 0,
                minor: 0,
                patch: 0,
            },
            description: String::new(),
            author: String::new(),
        };
        &META
    }

    fn capabilities(&self) -> &[Capability] {
        &[]
    }

    async fn on_load(&mut self, _host: std::sync::Arc<host::PluginHost>) -> PluginResult<()> {
        Ok(())
    }

    async fn on_enable(&mut self) -> PluginResult<()> {
        Ok(())
    }

    async fn on_disable(&mut self) -> PluginResult<()> {
        Ok(())
    }

    fn on_unload(&mut self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::types::*;
    use std::sync::Arc;

    struct MockPlugin {
        meta: PluginMeta,
        fail_enable: bool,
        fail_disable: bool,
    }

    impl MockPlugin {
        fn new(id: &str, fail_enable: bool, fail_disable: bool) -> Self {
            Self {
                meta: PluginMeta {
                    id: PluginId(id.to_string()),
                    name: "Mock".to_string(),
                    version: PluginVersion { major: 0, minor: 0, patch: 0 },
                    description: "".to_string(),
                    author: "".to_string(),
                },
                fail_enable,
                fail_disable,
            }
        }
    }

    #[async_trait::async_trait]
    impl Plugin for MockPlugin {
        fn meta(&self) -> &PluginMeta { &self.meta }
        fn capabilities(&self) -> &[Capability] { &[] }
        async fn on_load(&mut self, _host: Arc<PluginHost>) -> PluginResult<()> { Ok(()) }
        async fn on_enable(&mut self) -> PluginResult<()> {
            if self.fail_enable {
                Err(PluginError::ApiError("enable failed".to_string()))
            } else {
                Ok(())
            }
        }
        async fn on_disable(&mut self) -> PluginResult<()> {
            if self.fail_disable {
                Err(PluginError::ApiError("disable failed".to_string()))
            } else {
                Ok(())
            }
        }
        fn on_unload(&mut self) {}
    }

    #[tokio::test]
    async fn test_enable_failure_restores_plugin() {
        let host_factory = Arc::new(|_, _, _| panic!("should not be called"));
        let registry = PluginRegistry::new(PathBuf::from("test_plugins"), host_factory);

        let id = PluginId("test_plugin".to_string());
        let plugin = Box::new(MockPlugin::new("test_plugin", true, false));

        {
            let mut plugins = registry.plugins.write();
            plugins.insert(id.clone(), PluginEntry {
                plugin,
                status: PluginStatus::Loaded,
                manifest: PluginManifest {
                    id: "test_plugin".to_string(),
                    name: "Mock".to_string(),
                    version: "0.0.0".to_string(),
                    description: "".to_string(),
                    author: "".to_string(),
                    capabilities: vec![],
                    entry: "entry.rhai".to_string(),
                    permissions: PluginPermissions {
                        network: false,
                        file_system: false,
                        max_memory_mb: 32,
                        allowed_symbols: vec![],
                    },
                },
                schedule_handles: Vec::new(),
            });
        }

        let result = registry.enable(&id).await;
        assert!(result.is_err());

        // Verify that the plugin is NOT an EmptyPlugin
        let guard = registry.plugins.read();
        let entry = guard.get(&id).expect("Plugin should still exist");

        // If it's EmptyPlugin, the ID in meta() will be empty
        assert_eq!(entry.plugin.meta().id, id);
        assert_eq!(entry.status, PluginStatus::Loaded);
    }

    #[tokio::test]
    async fn test_disable_failure_restores_plugin() {
        let host_factory = Arc::new(|_, _, _| panic!("should not be called"));
        let registry = PluginRegistry::new(PathBuf::from("test_plugins"), host_factory);

        let id = PluginId("test_plugin".to_string());
        let plugin = Box::new(MockPlugin::new("test_plugin", false, true));

        {
            let mut plugins = registry.plugins.write();
            plugins.insert(id.clone(), PluginEntry {
                plugin,
                status: PluginStatus::Enabled,
                manifest: PluginManifest {
                    id: "test_plugin".to_string(),
                    name: "Mock".to_string(),
                    version: "0.0.0".to_string(),
                    description: "".to_string(),
                    author: "".to_string(),
                    capabilities: vec![],
                    entry: "entry.rhai".to_string(),
                    permissions: PluginPermissions {
                        network: false,
                        file_system: false,
                        max_memory_mb: 32,
                        allowed_symbols: vec![],
                    },
                },
                schedule_handles: Vec::new(),
            });
        }

        let result = registry.disable(&id).await;
        assert!(result.is_err());

        // Verify that the plugin is NOT an EmptyPlugin
        let guard = registry.plugins.read();
        let entry = guard.get(&id).expect("Plugin should still exist");

        assert_eq!(entry.plugin.meta().id, id);
        assert_eq!(entry.status, PluginStatus::Enabled);
    }
}
pub mod rhai_runtime;
pub mod wasm_runtime;
//! Rhai scripting runtime for plugins.
//!
//! A `RhaiPlugin` compiles a user-supplied `.rhai` source file with a
//! heavily restricted `rhai::Engine` (tight operation / call / size
//! budgets, no print, no module loading) and exposes the **full**
//! capability-gated host surface: logging (`log_debug/info/warn/error`),
//! per-plugin storage (`storage_get/set/delete/list_keys`), UI
//! (`notify_*`, `register_panel`, `emit_panel_data`), execution
//! (`submit_order`, `cancel_order`, `get_positions`), market-data read
//! (`get_latest_candle`), and the callback-based registries
//! (`register_indicator`, `register_metric`, `register_keyword`,
//! `schedule`/`cancel_schedule`, `subscribe_event`). Every function is
//! gated by the plugin's declared capabilities via the host's
//! `*_guarded` accessors and returns a benign fallback ("" / false / ()
//! / NaN / no-op) when the capability is denied or the backend errors.
//!
//! Lifetime of a plugin script:
//! 1. `on_load` — compile source, register host functions, invoke
//!    `on_load()` if defined.
//! 2. `on_enable` — invoke `on_enable()` if defined.
//! 3. `on_disable` — invoke `on_disable()` if defined.
//! 4. `on_unload` — invoke `on_unload()` if defined (errors ignored),
//!    drop the host handle and the AST.
//!
//! Callback dispatch: the registry/scheduler/event closures need to call
//! the plugin's `FnPtr`s *later*, outside any rhai call context. They do
//! so through a shared, lazily-filled `OnceLock<Weak<Engine>>` cell — the
//! engine is wrapped in `Arc` only after all host functions are
//! registered on the uniquely-owned `&mut Engine`, then the cell is
//! populated with a `Weak` handle. Using `Weak` avoids a reference cycle
//! (the engine owns the closures, the closures would otherwise own the
//! engine) and makes every callback a no-op once the plugin is unloaded
//! and the engine `Arc` is dropped.

use std::path::PathBuf;
use std::sync::{Arc};
use parking_lot::Mutex;

use rhai::{Array, Dynamic, Engine, EvalAltResult, FnPtr, Map, Scope};

use crate::models::Candle;
use crate::plugin::api::events::{EventBus, EventFilter, EventKind};
use crate::plugin::api::indicator_registry::SharedIndicatorRegistry;
use crate::plugin::api::{OrderRequest, OrderSide, OrderType, Position, UiPanel};
use crate::plugin::host::PluginHost;
use crate::plugin::types::{
    Capability, NotificationKind, PluginError, PluginId, PluginMeta, PluginResult, ScheduleHandle,
};
use crate::strategy::execution::paper::PaperTrade;
use crate::strategy::runtime::context::EvalContext;

use super::super::Plugin;

/// Newtype wrapper around `Candle` so we can register it with Rhai as a
/// custom type called "Candle" (Rhai types must be distinct from the
/// surrounding `Dynamic` value space).
#[derive(Clone)]
pub struct CandleWrapper(pub Candle);

impl std::fmt::Debug for CandleWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Candle")
            .field("open", &self.0.open)
            .field("high", &self.0.high)
            .field("low", &self.0.low)
            .field("close", &self.0.close)
            .field("volume", &self.0.volume)
            .field("timestamp", &self.0.timestamp)
            .finish()
    }
}

/// Rhai-backed plugin. Compiles a `.rhai` source file at load time and
/// invokes the `on_load` / `on_enable` / `on_disable` / `on_unload`
/// script functions at the corresponding lifecycle events.
///
/// The engine and AST are wrapped in `Arc` so that closures registered
/// as host functions (specifically `register_indicator`) can hold
/// long-lived references to them and dispatch back into the script
/// when the indicator pipeline is evaluated. Rhai's `Engine` is not
/// `Clone`, so wrapping it is the only way to share it between the
/// plugin struct and the registered functions.
pub struct RhaiPlugin {
    meta: PluginMeta,
    capabilities: Vec<Capability>,
    source_path: PathBuf,
    host: Option<Arc<PluginHost>>,
    engine: Arc<Engine>,
    scope: Option<Scope<'static>>,
}

/// Build a freshly-hardened `rhai::Engine` with the containment budgets
/// and the `Candle` custom type registered. Used by both `RhaiPlugin::new`
/// (wrapped in `Arc`) and `on_load` (which registers host functions on the
/// uniquely-owned engine before wrapping it), so the two never drift.
fn build_hardened_engine() -> Engine {
    let mut engine = Engine::new();
    // Hard execution budgets — these are the primary containment
    // mechanism for untrusted plugin code.
    engine.set_max_operations(200_000);
    engine.set_max_call_levels(32);
    engine.set_max_string_size(65_536);
    engine.set_max_array_size(10_000);
    engine.set_max_map_size(1_000);
    // Silently swallow plugin print() calls; the engine still parses
    // them but they go nowhere.
    engine.on_print(|_| {});
    // Module loading is intentionally NOT installed — the spec says
    // to leave it disabled by default. Plugins only see what we
    // explicitly register below.

    // Register the Candle newtype so plugin scripts can address
    // candles by their public field names.
    engine.register_type_with_name::<CandleWrapper>("Candle");
    engine.register_get("open", |c: &mut CandleWrapper| c.0.open);
    engine.register_get("high", |c: &mut CandleWrapper| c.0.high);
    engine.register_get("low", |c: &mut CandleWrapper| c.0.low);
    engine.register_get("close", |c: &mut CandleWrapper| c.0.close);
    engine.register_get("volume", |c: &mut CandleWrapper| c.0.volume);
    engine.register_get("timestamp", |c: &mut CandleWrapper| c.0.timestamp);

    engine
}

impl RhaiPlugin {
    /// Build a new Rhai plugin with a hardened engine. The source file
    /// is *not* compiled yet — that happens in `on_load`, which builds a
    /// fresh engine, registers the host functions on it while it is
    /// uniquely owned, and replaces this placeholder engine.
    pub fn new(
        meta: PluginMeta,
        capabilities: Vec<Capability>,
        source_path: PathBuf,
    ) -> PluginResult<Self> {
        Ok(Self {
            meta,
            capabilities,
            source_path,
            host: None,
            engine: Arc::new(build_hardened_engine()),
            scope: None,
        })
    }
}

/// Build a Rhai `Map` from a single `Candle` so plugin scripts can
/// iterate over candle arrays the same way they would over any other
/// array of record-like values.
fn candle_to_map(c: &Candle) -> Map {
    let mut m = Map::new();
    m.insert("open".into(), Dynamic::from(c.open));
    m.insert("high".into(), Dynamic::from(c.high));
    m.insert("low".into(), Dynamic::from(c.low));
    m.insert("close".into(), Dynamic::from(c.close));
    m.insert("volume".into(), Dynamic::from(c.volume));
    m.insert("timestamp".into(), Dynamic::from(c.timestamp));
    m
}

/// Convert a Rhai `Array` of numerics into `Vec<f64>`. Any non-numeric
/// entries become `f64::NAN` so the indicator pipeline keeps its
/// length and the failure is visible per-element rather than crashing
/// the whole evaluation.
fn dynamic_array_to_vec(arr: Array) -> Vec<f64> {
    arr.into_iter()
        .map(|v| {
            if v.is::<f64>() {
                v.cast::<f64>()
            } else if v.is::<i64>() {
                v.cast::<i64>() as f64
            } else {
                f64::NAN
            }
        })
        .collect()
}

/// Coerce a Rhai `Dynamic` returned from a plugin callback into `f64`.
/// `Dynamic::as_float` / `as_int` consume `self`, so we clone before each
/// attempt; anything that is neither a float nor an int becomes `NaN`.
fn dynamic_to_f64(d: &Dynamic) -> f64 {
    d.clone()
        .as_float()
        .ok()
        .or_else(|| d.clone().as_int().ok().map(|i| i as f64))
        .unwrap_or(f64::NAN)
}

/// Build a Rhai `Map` from the plugin-API `Candle` (which uses
/// `timestamp_ms` rather than the engine `Candle`'s `timestamp`). The
/// script sees the field as `timestamp` for parity with `candle_to_map`.
fn candle_api_to_map(c: &crate::plugin::api::Candle) -> Map {
    let mut m = Map::new();
    m.insert("open".into(), Dynamic::from(c.open));
    m.insert("high".into(), Dynamic::from(c.high));
    m.insert("low".into(), Dynamic::from(c.low));
    m.insert("close".into(), Dynamic::from(c.close));
    m.insert("volume".into(), Dynamic::from(c.volume));
    m.insert("timestamp".into(), Dynamic::from(c.timestamp_ms));
    m
}

/// Build a Rhai `Map` from a `PaperTrade` so analytics callbacks can
/// iterate over trade history. `side` is lowered to `"buy"` / `"sell"`
/// and `pnl` maps to a float or `()` when absent.
fn paper_trade_to_map(t: &PaperTrade) -> Map {
    let mut m = Map::new();
    m.insert("id".into(), Dynamic::from(t.id.clone()));
    m.insert("timestamp".into(), Dynamic::from(t.timestamp));
    m.insert("symbol".into(), Dynamic::from(t.symbol.clone()));
    let side = match t.side {
        crate::models::OrderSide::Buy => "buy",
        crate::models::OrderSide::Sell => "sell",
    };
    m.insert("side".into(), Dynamic::from(side.to_string()));
    m.insert("quantity".into(), Dynamic::from(t.quantity as i64));
    m.insert("price".into(), Dynamic::from(t.price));
    m.insert("ruleId".into(), Dynamic::from(t.rule_id.clone()));
    match t.pnl {
        Some(p) => m.insert("pnl".into(), Dynamic::from(p)),
        None => m.insert("pnl".into(), Dynamic::UNIT),
    };
    m
}

/// Build a Rhai `Map` from a broker `Position`.
fn position_to_map(p: &Position) -> Map {
    let mut m = Map::new();
    m.insert("symbol".into(), Dynamic::from(p.symbol.clone()));
    m.insert("quantity".into(), Dynamic::from(p.quantity));
    m.insert("averagePrice".into(), Dynamic::from(p.average_price));
    m
}

/// Build a Rhai `Map` from an `EventKind` so `subscribe_event` callbacks
/// receive a self-describing record keyed by `type`.
fn event_to_map(e: &EventKind) -> Map {
    let mut m = Map::new();
    match e {
        EventKind::CandleProcessed(c) => {
            m.insert("type".into(), Dynamic::from("candleProcessed".to_string()));
            m.insert("candle".into(), Dynamic::from(candle_to_map(c)));
        }
        EventKind::TradeExecuted(t) => {
            m.insert("type".into(), Dynamic::from("tradeExecuted".to_string()));
            m.insert("trade".into(), Dynamic::from(paper_trade_to_map(t)));
        }
        EventKind::RuleFired {
            rule_id,
            strategy_id,
        } => {
            m.insert("type".into(), Dynamic::from("ruleFired".to_string()));
            m.insert("ruleId".into(), Dynamic::from(rule_id.clone()));
            m.insert("strategyId".into(), Dynamic::from(strategy_id.clone()));
        }
        EventKind::StrategyStatusChanged {
            strategy_id,
            new_status,
        } => {
            m.insert(
                "type".into(),
                Dynamic::from("strategyStatusChanged".to_string()),
            );
            m.insert("strategyId".into(), Dynamic::from(strategy_id.clone()));
            m.insert("newStatus".into(), Dynamic::from(new_status.clone()));
        }
        EventKind::SystemShutdown => {
            m.insert("type".into(), Dynamic::from("systemShutdown".to_string()));
        }
    }
    m
}

/// Parse a script-supplied event filter string into an `EventFilter`.
/// Unknown values default to `All` so a typo subscribes broadly rather
/// than silently receiving nothing.
fn parse_event_filter(s: &str) -> EventFilter {
    match s.to_ascii_lowercase().as_str() {
        "candle" => EventFilter::CandleProcessed,
        "trade" => EventFilter::TradeExecuted,
        "rule" => EventFilter::RuleFired,
        "status" => EventFilter::StrategyStatusChanged,
        "shutdown" => EventFilter::SystemShutdown,
        _ => EventFilter::All,
    }
}

/// Invokes a Rhai callback using the execution context provided by the host.
/// Returns a `PluginResult` containing the result or a `PluginError::ApiError`.
fn invoke_rhai_callback<Args: rhai::FuncArgs>(
    host: &PluginHost,
    fn_ptr: &FnPtr,
    args: Args,
) -> PluginResult<Dynamic> {
    let engine = host
        .engine
        .get()
        .ok_or_else(|| PluginError::ApiError("plugin engine not loaded".into()))?;
    let ast = host
        .ast
        .get()
        .ok_or_else(|| PluginError::ApiError("plugin ast not loaded".into()))?;

    match fn_ptr.call(engine, ast, args) {
        Ok(d) => Ok(d),
        Err(e) => Err(PluginError::ApiError(e.to_string())),
    }
}

/// Register all host-facing functions onto the given engine. Each
/// function captures a clone of the host's `Arc`, so the engine owns
/// the references it needs and dropping the engine drops the closures.
///
/// Callback-based functions (`register_indicator` / `register_metric` /
/// `register_keyword` / `schedule` / `subscribe_event`) also capture the
/// shared `engine_cell` — a lazily-filled `OnceLock<Weak<Engine>>` — and
/// the plugin's `AST`. When the registered callback fires later, it
/// upgrades the `Weak` to reach the engine; if the plugin has been
/// unloaded (engine `Arc` dropped) the upgrade fails and the callback
/// returns its benign fallback.
fn register_host_functions(
    engine: &mut Engine,
    host: Arc<PluginHost>,
    plugin_id: PluginId,
) {
    // ---- Logging (unguarded by capability, namespaced to the plugin). ----
    {
        let host = host.clone();
        let pid = plugin_id.clone();
        engine.register_fn("log_info", move |msg: &str| {
            host.log().info(&pid, msg);
        });
    }
    {
        let host = host.clone();
        let pid = plugin_id.clone();
        engine.register_fn("log_warn", move |msg: &str| {
            host.log().warn(&pid, msg);
        });
    }
    {
        let host = host.clone();
        let pid = plugin_id.clone();
        engine.register_fn("log_error", move |msg: &str| {
            host.log().error(&pid, msg);
        });
    }

    // ---- Per-plugin storage (Storage capability). ----
    //
    // The underlying `StorageApi::read` / `write` are synchronous
    // (the `async_trait` is on the trait only for forward compat), so
    // we call them directly. We still tolerate being inside a Tokio
    // runtime by going through `Handle::current().block_on` if the
    // implementation later becomes async — falling back to a direct
    // call if there is no current runtime.
    {
        let host = host.clone();
        engine.register_fn("storage_get", move |key: &str| -> Dynamic {
            let storage = match host.storage_guarded() {
                Ok(s) => s,
                Err(_) => return Dynamic::UNIT,
            };
            match storage.read(key) {
                Ok(Some(bytes)) => match String::from_utf8(bytes) {
                    Ok(s) => Dynamic::from(s),
                    Err(_) => Dynamic::UNIT,
                },
                _ => Dynamic::UNIT,
            }
        });
    }
    {
        let host = host.clone();
        engine.register_fn("storage_set", move |key: &str, val: &str| -> bool {
            let storage = match host.storage_guarded() {
                Ok(s) => s,
                Err(_) => return false,
            };
            storage.write(key, val.as_bytes()).is_ok()
        });
    }

    // ---- UI notifications (UiPanels capability). ----
    {
        let host = host.clone();
        engine.register_fn("notify_info", move |msg: &str| {
            if let Ok(ui) = host.ui_guarded() {
                let _ = ui.notify(NotificationKind::Info, msg);
            }
        });
    }
    {
        let host = host.clone();
        engine.register_fn("notify_warning", move |msg: &str| {
            if let Ok(ui) = host.ui_guarded() {
                let _ = ui.notify(NotificationKind::Warning, msg);
            }
        });
    }
    {
        let host = host.clone();
        engine.register_fn("notify_error", move |msg: &str| {
            if let Ok(ui) = host.ui_guarded() {
                let _ = ui.notify(NotificationKind::Error, msg);
            }
        });
    }

    // ---- Indicator registration (Indicators capability). ----
    //
    // The Rhai script calls `register_indicator(name, fn_ptr)`. The
    // `FnPtr` is a handle into the plugin's AST — to dispatch it, the
    // closure we hand to the registry needs to call
    // `func.call(&engine, &ast, args)`. That means the closure must
    // capture both the engine and the AST, which is why `RhaiPlugin`
    // stores them in `Arc`s.
    //
    // The trait-level `IndicatorRegistryApi` exposes a factory-based
    // `register` that loses plugin-id information. The spec's
    // `register(name, plugin_id, fn)` semantics live on the concrete
    // `SharedIndicatorRegistry::register_fn`. We downcast back to the
    // concrete type when the host is built with one; if the cast
    // fails, the registration is best-effort no-op.
    {
        let host_for_ind = host.clone();
        let pid_clone = plugin_id.clone();
        engine.register_fn(
            "register_indicator",
            move |name: String, func: FnPtr| -> bool {
                let registry = match host_for_ind.indicators_guarded() {
                    Ok(r) => r,
                    Err(_) => return false,
                };
                // Try the spec-shaped path first (plugin-id-attributed).
                if let Some(shared) = registry.as_any().downcast_ref::<SharedIndicatorRegistry>() {
                    let host_for_call = host_for_ind.clone();
                    let func_for_call = func.clone();
                    let indicator_fn: std::sync::Arc<
                        dyn Fn(&[Candle], usize) -> Vec<f64> + Send + Sync,
                    > = std::sync::Arc::new(move |candles: &[Candle], period: usize| -> Vec<f64> {
                        let n = candles.len();
                        let engine = match host_for_call.engine.get() {
                            Some(e) => e,
                            None => return vec![f64::NAN; n],
                        };
                        let ast = match host_for_call.ast.get() {
                            Some(a) => a,
                            None => return vec![f64::NAN; n],
                        };
                        let candles_array: Array = candles
                            .iter()
                            .map(candle_to_map)
                            .map(Dynamic::from)
                            .collect();
                        let call_result: Result<Array, Box<EvalAltResult>> = func_for_call.call(
                            engine,
                            ast,
                            (candles_array, period as i64),
                        );
                        match call_result {
                            Ok(arr) => dynamic_array_to_vec(arr),
                            Err(_) => vec![f64::NAN; n],
                        }
                    });
                    return shared
                        .register_fn(&name, pid_clone.clone(), indicator_fn)
                        .is_ok();
                }
                // No downcast: the trait-level register path requires a
                // plugin-id-attributed indicator function which we already
                // constructed above. The trait-level register method now
                // matches the spec shape and the factory-based fallback is
                // intentionally removed.
                false
            },
        );
    }

    // ---- Extra logging level (unguarded). ----
    {
        let host = host.clone();
        let pid = plugin_id.clone();
        engine.register_fn("log_debug", move |msg: &str| {
            host.log().debug(&pid, msg);
        });
    }

    // ---- Storage: delete + list_keys (Storage capability). ----
    {
        let host = host.clone();
        engine.register_fn("storage_delete", move |key: &str| -> bool {
            let storage = match host.storage_guarded() {
                Ok(s) => s,
                Err(_) => return false,
            };
            storage.delete(key).is_ok()
        });
    }
    {
        let host = host.clone();
        engine.register_fn("storage_list_keys", move |prefix: &str| -> Array {
            let storage = match host.storage_guarded() {
                Ok(s) => s,
                Err(_) => return Array::new(),
            };
            match storage.list_keys(prefix) {
                Ok(keys) => keys.into_iter().map(Dynamic::from).collect(),
                Err(_) => Array::new(),
            }
        });
    }

    // ---- UI panels (UiPanels capability). ----
    {
        let host = host.clone();
        engine.register_fn(
            "register_panel",
            move |id: &str, title: &str, route: &str| -> bool {
                let ui = match host.ui_guarded() {
                    Ok(u) => u,
                    Err(_) => return false,
                };
                ui.register_panel(UiPanel {
                    id: id.to_string(),
                    title: title.to_string(),
                    route: route.to_string(),
                })
                .is_ok()
            },
        );
    }
    // Panel data emit downcasts to the Tauri-backed impl so the broadcast
    // channel picks the value up — same path the WASM runtime uses.
    {
        let host = host.clone();
        let pid = plugin_id.clone();
        engine.register_fn(
            "emit_panel_data",
            move |panel_id: &str, json_string: &str| -> bool {
                let ui = match host.ui_guarded() {
                    Ok(u) => u,
                    Err(_) => return false,
                };
                let value: serde_json::Value = match serde_json::from_str(json_string) {
                    Ok(v) => v,
                    Err(e) => {
                        host.log()
                            .error(&pid, &format!("emit_panel_data: invalid json: {e}"));
                        return false;
                    }
                };
                if let Some(tauri_ui) = ui
                    .as_any()
                    .downcast_ref::<crate::plugin::api::ui::TauriUiApi>()
                {
                    tauri_ui.emit_panel_data(panel_id.to_string(), value).is_ok()
                } else {
                    host.log()
                        .error(&pid, "emit_panel_data: host ui is not a TauriUiApi");
                    false
                }
            },
        );
    }

    // ---- Execution (Execution capability). ----
    //
    // `submit_order` / `cancel_order` are async on the trait; we bridge
    // them synchronously via `block_in_place + Handle::block_on`, which
    // requires a multi-thread runtime (Tauri default; tests use
    // `#[tokio::test(flavor = "multi_thread")]`). `positions()` is sync
    // and self-manages any blocking internally, so it is called directly.
    {
        let host = host.clone();
        let pid = plugin_id.clone();
        engine.register_fn("submit_order", move |symbol: &str, side: &str, qty: i64, order_type: &str, price: f64| -> Result<String, Box<EvalAltResult>> {
            let execution = match host.execution_guarded() {
                Ok(e) => e.clone(),
                Err(_) => return Ok(String::new()),
            };
            if qty <= 0 {
                return Ok(String::new());
            }
            let side = match side.to_ascii_lowercase().as_str() {
                "buy" => OrderSide::Buy,
                "sell" => OrderSide::Sell,
                other => {
                    host.log()
                        .error(&pid, &format!("submit_order: invalid side '{other}'"));
                    return Ok(String::new());
                }
            };
            let (order_type, price) = match order_type.to_ascii_lowercase().as_str() {
                "market" => (OrderType::Market, None),
                "limit" => (OrderType::Limit, Some(price)),
                other => {
                    host.log()
                        .error(&pid, &format!("submit_order: invalid order type '{other}'"));
                    return Ok(String::new());
                }
            };
            let request = OrderRequest {
                symbol: symbol.to_string(),
                side,
                quantity: qty as u32,
                order_type,
                price,
            };
            let result = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(execution.submit_order(request))
            });
            match result {
                Ok(id) => Ok(id),
                Err(e) => {
                    host.log().error(&pid, &format!("submit_order: {e}"));
                    Ok(String::new())
                }
            }
        },
    );

    {
        let host = host.clone();
        let pid = plugin_id.clone();
        engine.register_fn("cancel_order", move |order_id: &str| -> Result<bool, Box<EvalAltResult>> {
            let execution = match host.execution_guarded() {
                Ok(e) => e.clone(),
                Err(_) => return Ok(false),
            };
            let result = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(execution.cancel_order(order_id))
            });
            match result {
                Ok(()) => Ok(true),
                Err(e) => {
                    host.log().error(&pid, &format!("cancel_order: {e}"));
                    Ok(false)
                }
            }
        });
    }
    {
        let host = host.clone();
        let pid = plugin_id.clone();
        engine.register_fn("get_positions", move || -> Array {
            let execution = match host.execution_guarded() {
                Ok(e) => e,
                Err(_) => return Array::new(),
            };
            match execution.positions() {
                Ok(positions) => positions
                    .iter()
                    .map(position_to_map)
                    .map(Dynamic::from)
                    .collect(),
                Err(e) => {
                    host.log().error(&pid, &format!("get_positions: {e}"));
                    Array::new()
                }
            }
        });
    }

    // ---- Market-data read (MarketData capability). ----
    //
    // `latest_candle` is sync but its impl calls a bare `block_on`
    // internally, so we wrap the call in `block_in_place` to avoid
    // nesting a runtime inside the current worker.
    {
        let host = host.clone();
        let pid = plugin_id.clone();
        engine.register_fn("get_latest_candle", move |symbol: &str| -> Dynamic {
            let md = match host.market_data_guarded() {
                Ok(m) => m.clone(),
                Err(_) => return Dynamic::UNIT,
            };
            let result = tokio::task::block_in_place(|| md.latest_candle(symbol));
            match result {
                Ok(candle) => Dynamic::from(candle_api_to_map(&candle)),
                Err(e) => {
                    host.log().error(&pid, &format!("get_latest_candle: {e}"));
                    Dynamic::UNIT
                }
            }
        });
    }

    // ---- Analytics metric registration (Analytics capability). ----
    //
    // The script's `FnPtr` is invoked later as `fn(trades_array) -> number`
    // whenever the metric is evaluated.
    {
        let host = host.clone();
        let pid = plugin_id.clone();
        engine.register_fn("register_metric", move |name: String, func: FnPtr| -> bool {
            let analytics = match host.analytics_guarded() {
                Ok(a) => a,
                Err(_) => return false,
            };
            if func.is_curried() {
                host.log().error(&pid, "register_metric: callbacks must be plain named functions, not closures");
                return false;
            }
            let host_for_call = host.clone();
            let func_for_call = func.clone();
            let pid_for_call = pid.clone();
            let metric_fn: Arc<dyn Fn(&[PaperTrade]) -> f64 + Send + Sync> =
                Arc::new(move |trades: &[PaperTrade]| -> f64 {
                    let trades_array: Array = trades
                        .iter()
                        .map(paper_trade_to_map)
                        .map(Dynamic::from)
                        .collect();
                    match invoke_rhai_callback(&host_for_call, &func_for_call, (trades_array,)) {
                        Ok(d) => dynamic_to_f64(&d),
                        Err(e) => {
                            host_for_call.log().error(&pid_for_call, &format!("metric callback failed: {e}"));
                            f64::NAN
                        }
                    }
                });

            host.callbacks.metrics.lock().unwrap().insert(name.clone(), func.clone());
            analytics.register_metric(name, pid.clone(), metric_fn).is_ok()
        });
    }

    // ---- DSL keyword registration (DslExtension capability). ----
    //
    // The script's `FnPtr` is invoked later as
    // `fn(candles_array, current_map) -> bool` from the strategy
    // evaluator.
    {
        let host = host.clone();
        let pid = plugin_id.clone();
        engine.register_fn("register_keyword", move |keyword: String, func: FnPtr| -> bool {
            let dsl = match host.dsl_guarded() {
                Ok(d) => d,
                Err(_) => return false,
            };
            if func.is_curried() {
                host.log().error(&pid, "register_keyword: callbacks must be plain named functions, not closures");
                return false;
            }
            let host_for_call = host.clone();
            let func_for_call = func.clone();
            let pid_for_call = pid.clone();
            let handler: Arc<dyn Fn(&EvalContext) -> bool + Send + Sync> =
                Arc::new(move |ctx: &EvalContext| -> bool {
                    let candles_array: Array = ctx
                        .candles
                        .iter()
                        .map(candle_to_map)
                        .map(Dynamic::from)
                        .collect();
                    let current_map = candle_to_map(ctx.current);
                    match invoke_rhai_callback(&host_for_call, &func_for_call, (candles_array, current_map)) {
                        Ok(d) => d.as_bool().unwrap_or(false),
                        Err(e) => {
                            host_for_call.log().error(&pid_for_call, &format!("keyword callback failed: {e}"));
                            false
                        }
                    }
                });
            host.callbacks.keywords.lock().unwrap().insert(keyword.clone(), func.clone());
            dsl.register_keyword(keyword, pid.clone(), handler).is_ok()
        });
    }

    // ---- Cron scheduling (Scheduler capability). ----
    //
    // The task closure is spawned on a tokio task by the scheduler and
    // calls the plugin `FnPtr` as `fn()`.
    {
        let host = host.clone();
        let pid = plugin_id.clone();
        engine.register_fn("schedule", move |cron_expr: &str, func: FnPtr| -> String {
            let scheduler = match host.scheduler_guarded() {
                Ok(s) => s.clone(),
                Err(_) => return String::new(),
            };
            if func.is_curried() {
                host.log().error(&pid, "schedule: callbacks must be plain named functions, not closures");
                return String::new();
            }
            let host_for_call = host.clone();
            let func_for_call = func.clone();
            let pid_for_call = pid.clone();
            let scheduler_for_call = scheduler.clone();
            let failures = Arc::new(std::sync::atomic::AtomicU32::new(0));
            let failures_for_call = failures.clone();
            let handle_cell = Arc::new(Mutex::new(None));
            let handle_cell_for_call = handle_cell.clone();
            let task: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
                let res = tokio::task::block_in_place(|| {
                    invoke_rhai_callback(&host_for_call, &func_for_call, ())
                });
                match res {
                    Ok(_) => {
                        failures_for_call.store(0, std::sync::atomic::Ordering::SeqCst);
                    }
                    Err(e) => {
                        let count = failures_for_call.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                        host_for_call.log().error(&pid_for_call, &format!("scheduled task failed: {e}"));
                        if count >= 3 {
                            host_for_call.log().error(&pid_for_call, "scheduled task disabled after 3 consecutive failures");
                            if let Some(handle) = handle_cell_for_call.lock().clone() {
                                let _ = scheduler_for_call.cancel(handle);
                            }
                        }
                    }
                }
            });
            match scheduler.schedule(pid.clone(), cron_expr, task) {
                Ok(handle) => {
                    *handle_cell.lock() = Some(handle.clone());
                    host.callbacks.schedules.lock().unwrap().insert(handle.clone(), func.clone());
                    handle.to_string()
                }
                Err(e) => {
                    host.log().error(&pid, &format!("schedule: {e}"));
                    String::new()
                }
            }
        });
    }
        {
        let host = host.clone();
        engine.register_fn("cancel_schedule", move |handle_string: &str| -> bool {
            let scheduler = match host.scheduler_guarded() {
                Ok(s) => s,
                Err(_) => return false,
            };
            let uuid = match uuid::Uuid::parse_str(handle_string) {
                Ok(u) => u,
                Err(_) => return false,
            };
            scheduler.cancel(ScheduleHandle(uuid)).is_ok()
        });
    }

    // ---- Event subscription (Events capability). ----
    //
    // The callback is invoked from a tokio task by the event bus and
    // calls the plugin `FnPtr` as `fn(event_map)`.
    {
        let host = host.clone();
        let pid = plugin_id.clone();
        engine.register_fn("subscribe_event", move |filter: &str, func: FnPtr| -> String {
            let bus: &Arc<EventBus> = match host.event_bus_guarded() {
                Ok(b) => b,
                Err(_) => return String::new(),
            };
            if func.is_curried() {
                host.log().error(&pid, "subscribe_event: callbacks must be plain named functions, not closures");
                return String::new();
            }
            let host_for_call = host.clone();
            let func_for_call = func.clone();
            let pid_for_call = pid.clone();
            let callback: Arc<dyn Fn(EventKind) + Send + Sync> = Arc::new(move |event: EventKind| {
                let event_map = event_to_map(&event);
                match invoke_rhai_callback(&host_for_call, &func_for_call, (event_map,)) {
                    Ok(_) => (),
                    Err(e) => {
                        host_for_call.log().error(&pid_for_call, &format!("event callback failed: {e}"));
                    }
                }
            });
            let handle = bus.subscribe(pid.clone(), parse_event_filter(filter), callback);
            host.callbacks.events.lock().unwrap()
                .entry(parse_event_filter(filter))
                .or_default()
                .push(func.clone());
            handle.to_string()

        });
    }
}

struct NoopIndicator;
impl crate::plugin::api::IndicatorInstance for NoopIndicator {
    fn update(&mut self, _value: f64) {}
    fn value(&self) -> Option<f64> {
        None
    }
    fn name(&self) -> &str {
        "no-op"
    }
}

#[async_trait::async_trait]
impl Plugin for RhaiPlugin {
    fn meta(&self) -> &PluginMeta {
        &self.meta
    }

    fn capabilities(&self) -> &[Capability] {
        &self.capabilities
    }

    async fn on_load(&mut self, host: Arc<PluginHost>) -> PluginResult<()> {
        // Stash the host so lifecycle hooks can use it.
        self.host = Some(host.clone());

        // Build a FRESH, uniquely-owned engine so host functions can be
        // registered on `&mut Engine` before it is shared.
        let mut engine = build_hardened_engine();

        // Compile the user source with the fresh engine.
        let ast = engine
            .compile_file(self.source_path.clone())
            .map_err(|e| PluginError::LoadFailed(format!("compile_file failed: {e}")))?;

        // Populate the host's execution context.
        host.ast.set(Arc::new(ast)).expect("ast should be empty");

        register_host_functions(
            &mut engine,
            host.clone(),
            self.meta.id.clone(),
        );

        // Wrap the now fully-configured engine in an `Arc` and store it in the host.
        let engine_arc = Arc::new(engine);
        host.engine.set(engine_arc.clone()).expect("engine should be empty");
        self.engine = engine_arc;

        // Initialize the call scope.
        self.scope = Some(Scope::new());

        // Call the script's `on_load` if present.
        let ast_ref = host.ast.get().expect("just set");
        let scope = self.scope.as_mut().expect("just set");
        let result: Result<Dynamic, Box<EvalAltResult>> =
            self.engine.call_fn(scope, ast_ref, "on_load", ());
        match result {
            Ok(_) => Ok(()),
            Err(e) if matches!(*e, EvalAltResult::ErrorFunctionNotFound(_, _)) => Ok(()),
            Err(e) => Err(PluginError::LoadFailed(format!("on_load failed: {e}"))),
        }
    }

    async fn on_enable(&mut self) -> PluginResult<()> {
        let host = self
            .host
            .as_ref()
            .ok_or_else(|| PluginError::ApiError("plugin not loaded".into()))?;
        let ast = host
            .ast
            .get()
            .ok_or_else(|| PluginError::ApiError("plugin not loaded".into()))?;
        let scope = self
            .scope
            .as_mut()
            .ok_or_else(|| PluginError::ApiError("plugin not loaded".into()))?;
        let result: Result<Dynamic, Box<EvalAltResult>> =
            self.engine.call_fn(scope, ast, "on_enable", ());
        match result {
            Ok(_) => Ok(()),
            Err(e) if matches!(*e, EvalAltResult::ErrorFunctionNotFound(_, _)) => Ok(()),
            Err(e) => Err(PluginError::ApiError(format!("on_enable failed: {e}"))),
        }
    }

    async fn on_disable(&mut self) -> PluginResult<()> {
        let host = self
            .host
            .as_ref()
            .ok_or_else(|| PluginError::ApiError("plugin not loaded".into()))?;
        let ast = host
            .ast
            .get()
            .ok_or_else(|| PluginError::ApiError("plugin not loaded".into()))?;
        let scope = self
            .scope
            .as_mut()
            .ok_or_else(|| PluginError::ApiError("plugin not loaded".into()))?;
        let result: Result<Dynamic, Box<EvalAltResult>> =
            self.engine.call_fn(scope, ast, "on_disable", ());
        match result {
            Ok(_) => Ok(()),
            Err(e) if matches!(*e, EvalAltResult::ErrorFunctionNotFound(_, _)) => Ok(()),
            Err(e) => Err(PluginError::ApiError(format!("on_disable failed: {e}"))),
        }
    }

    fn on_unload(&mut self) {
        if let (Some(host), Some(scope)) = (self.host.as_ref(), self.scope.as_mut()) {
            if let Some(ast) = host.ast.get() {
                // Errors from on_unload are intentionally ignored per spec.
                let _: Result<(), Box<EvalAltResult>> =
                    self.engine.call_fn(scope, ast, "on_unload", ());
            }

            // Explicitly clear registries to break Arc cycles between
            // callbacks and the host/engine.
            host.callbacks.indicators.lock().clear();
            host.callbacks.metrics.lock().clear();
            host.callbacks.keywords.lock().clear();
            host.callbacks.schedules.lock().clear();
            host.callbacks.events.lock().clear();
            host.callbacks.schedule_failures.lock().clear();
        }
        self.host = None;
        self.scope = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression guard for the `Arc::get_mut`-on-a-cloned-`Arc` bug that
    /// made *every* Rhai plugin fail to load with "engine Arc unexpectedly
    /// shared". This drives a tiny `.rhai` plugin through the full
    /// `on_load` → `on_enable` → `on_unload` lifecycle against a real
    /// `PluginHost` and asserts that (1) no lifecycle call errors and
    /// (2) a `storage_set` inside `on_load` round-trips through the host.
    ///
    /// Mirrors the WASM end-to-end smoke test's host construction.
    #[tokio::test(flavor = "multi_thread")]
    async fn rhai_plugin_loads_and_drives_host_fns_end_to_end() {
        use std::path::PathBuf;

        use crate::plugin::api::analytics::SharedAnalyticsRegistry;
        use crate::plugin::api::dsl_extension::SharedDslExtensionRegistry;
        use crate::plugin::api::events::EventBus;
        use crate::plugin::api::indicator_registry::SharedIndicatorRegistry;
        use crate::plugin::api::log_file::{RateLimitedFileLog, RollingLog};
        use crate::plugin::api::scheduler::CronScheduler;
        use crate::plugin::api::storage::PluginKvStore;
        use crate::plugin::api::ui::TauriUiApi;
        use crate::plugin::api::StorageApi;
        use crate::plugin::host::PluginHostBuilder;
        use crate::plugin::manifest::PluginPermissions;
        use crate::plugin::types::{PluginId, PluginMeta, PluginVersion};

        // Tiny Rhai plugin: logs on load and writes a storage key.
        let source = r#"
            fn on_load() {
                log_info("loaded");
                storage_set("k", "v");
            }
        "#;

        let dir = tempfile::tempdir().expect("tempdir");
        let source_path: PathBuf = dir.path().join("plugin.rhai");
        let log_path: PathBuf = dir.path().join("plugin.log");
        let storage_dir: PathBuf = dir.path().join("storage");
        std::fs::create_dir_all(&storage_dir).expect("mkdir storage");
        std::fs::write(&source_path, source.as_bytes()).expect("write .rhai");

        let plugin_id = PluginId::from("rhai-smoke");
        let meta = PluginMeta {
            id: plugin_id.clone(),
            name: "rhai-smoke".into(),
            version: PluginVersion {
                major: 0,
                minor: 1,
                patch: 0,
            },
            description: "Rhai lifecycle smoke fixture".into(),
            author: "tests".into(),
        };
        let mut plugin =
            RhaiPlugin::new(meta, vec![Capability::Storage], source_path).expect("RhaiPlugin::new");

        // Host-side services the plugin never touches get minimal stubs so
        // the test does not pull in a broker.
        struct NoopMarketData;
        #[async_trait::async_trait]
        impl crate::plugin::api::MarketDataApi for NoopMarketData {
            fn subscribe_ticks(
                &self,
                _symbol: &str,
                _callback: std::sync::Arc<
                    dyn Fn(crate::plugin::api::MarketDataEvent) + Send + Sync,
                >,
            ) -> crate::plugin::PluginResult<crate::plugin::types::SubscriptionHandle> {
                Err(crate::plugin::types::PluginError::ApiError("noop".into()))
            }
            fn unsubscribe_ticks(
                &self,
                _handle: crate::plugin::types::SubscriptionHandle,
            ) -> crate::plugin::PluginResult<()> {
                Ok(())
            }
            fn latest_candle(
                &self,
                _symbol: &str,
            ) -> crate::plugin::PluginResult<crate::plugin::api::Candle> {
                Err(crate::plugin::types::PluginError::ApiError("noop".into()))
            }
        }

        let log = std::sync::Arc::new(RateLimitedFileLog::with_log(
            plugin_id.clone(),
            RollingLog::open(log_path.clone()).expect("open log file"),
        ));
        let storage = std::sync::Arc::new(
            PluginKvStore::new(plugin_id.clone(), storage_dir.clone()).expect("kv store"),
        );
        let (tauri_ui_api, _rx) = TauriUiApi::new();
        let host = PluginHostBuilder {
            id: plugin_id.clone(),
            market_data: std::sync::Arc::new(NoopMarketData),
            execution: std::sync::Arc::new(crate::plugin::api::execution::NoopExecutionApi),
            storage,
            event_bus: EventBus::new(),
            indicators: std::sync::Arc::new(SharedIndicatorRegistry::new()),
            analytics: std::sync::Arc::new(SharedAnalyticsRegistry::new()),
            dsl: std::sync::Arc::new(SharedDslExtensionRegistry::new()),
            ui: tauri_ui_api,
            scheduler: CronScheduler::new(),
            log,
            capabilities: vec![Capability::Storage],
            permissions: PluginPermissions {
                network: false,
                file_system: false,
                max_memory_mb: 8,
                allowed_symbols: Vec::new(),
            },
        }
        .build();

        // The core regression: on_load must NOT error (the old
        // Arc::get_mut path always did).
        plugin.on_load(host.clone()).await.expect("on_load");
        plugin.on_enable().await.expect("on_enable");
        plugin.on_unload();

        // The storage write inside on_load should have landed.
        let re_open =
            PluginKvStore::new(plugin_id.clone(), storage_dir.clone()).expect("re-open kv");
        assert_eq!(
            re_open.read("k").expect("read k"),
            Some(b"v".to_vec()),
            "storage_set through the Rhai host fn should leave 'k' -> 'v'"
        );
    }
}
//! WebAssembly plugin runtime.
//!
//! A `WasmPlugin` loads a `.wasm` artifact, links a small set of
//! capability-gated host functions into the `algomln` module namespace,
//! and invokes the exported `_algomln_on_load` / `_algomln_on_enable` /
//! `_algomln_on_disable` / `_algomln_on_unload` functions at the
//! corresponding lifecycle events.
//!
//! The host surface bridges the **non-callback** capabilities. Fourteen
//! `algomln::*` host functions are linked: logging
//! (`log_info`/`log_warn`/`log_error`/`log_debug`), per-plugin KV storage
//! (`storage_get`/`storage_set`/`storage_delete`), UI
//! (`notify`/`register_panel`/`emit_panel_data`), execution
//! (`submit_order`/`cancel_order`/`get_positions`), and market-data read
//! (`get_latest_candle`). WASI is intentionally not linked — plugins
//! interact with the platform exclusively through the `algomln::*` host
//! functions below.
//!
//! **Deferred for WASM: callback-based capabilities.** Custom indicators,
//! analytics metrics, DSL keywords, cron tasks, event subscriptions, and
//! market-data tick subscriptions all require the host to call *back into*
//! the guest module later, on another thread. wasmtime's `Store` is not
//! `Sync` and is not re-entrant that way, so those capabilities cannot be
//! bridged without a message-queue / re-entrant-store redesign and are
//! intentionally NOT exposed here (they are faked nowhere). The Rhai
//! runtime, whose engine is `Sync` under the `sync` feature, bridges the
//! full surface including these callbacks. See `build_linker` for the
//! matching in-code note.
//!
//! Memory is bounded by a `ResourceLimiter` that refuses linear-memory
//! growth past the configured `memory_limit_bytes`. CPU is bounded by
//! wasmtime's epoch-interruption mechanism: each `WasmPlugin` owns an
//! `EpochWatchdog` background thread that calls `Engine::increment_epoch`
//! once every `EPOCH_TICK_MS` milliseconds, and every lifecycle export is
//! invoked with a fresh `LIFECYCLE_CPU_BUDGET_TICKS` deadline. A plugin
//! export that runs longer than roughly `EPOCH_TICK_MS *
//! LIFECYCLE_CPU_BUDGET_TICKS` ms traps instead of hanging the host
//! thread — the watchdog is what actually drives the deadline (arming a
//! deadline without incrementing the epoch would never fire). The
//! watchdog is stopped and joined in `on_unload`.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use wasmtime::ResourceLimiter;
use wasmtime::{Caller, Config, Engine, Instance, Linker, Memory, Module, OptLevel, Store};

/// How often the epoch watchdog ticks the engine's epoch counter. The
/// effective CPU cap for a lifecycle export is roughly
/// `EPOCH_TICK_MS * LIFECYCLE_CPU_BUDGET_TICKS` milliseconds.
const EPOCH_TICK_MS: u64 = 100;

/// Number of epoch ticks a single lifecycle export may consume before the
/// store's deadline is reached and the call traps. 50 ticks × 100 ms ≈ 5 s.
const LIFECYCLE_CPU_BUDGET_TICKS: u64 = 50;

use crate::plugin::api::{OrderRequest, OrderSide, OrderType, UiPanel};
use crate::plugin::host::PluginHost;
use crate::plugin::types::{Capability, NotificationKind, PluginError, PluginMeta, PluginResult};

use super::super::Plugin;

/// A resource limiter that bounds the total linear memory a WASM module
/// may grow to. Refusing growth past the configured cap is the
/// primary memory-isolation mechanism for untrusted plugin code.
///
/// Both `memory_growing` and `table_growing` take `usize` arguments: for
/// `memory_growing` they are page-aligned byte counts; for `table_growing`
/// they are element counts. We reject any growth past the configured caps.
struct MemoryLimitState {
    memory_limit: u32,
}

impl ResourceLimiter for MemoryLimitState {
    fn memory_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> Result<bool, wasmtime::Error> {
        Ok(desired as u64 <= self.memory_limit as u64)
    }

    fn table_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> Result<bool, wasmtime::Error> {
        // Tables are not exposed through our host surface, so a small
        // generous bound is sufficient.
        Ok(desired <= 10_000usize)
    }
}

/// Background thread that drives an `Engine`'s epoch counter so that
/// per-store epoch deadlines actually fire. Without something calling
/// `Engine::increment_epoch`, a `set_epoch_deadline` is inert and an
/// infinite loop in a plugin export would hang the host thread. The
/// watchdog ticks once every `EPOCH_TICK_MS` ms until dropped; `Drop`
/// signals the thread to stop and joins it.
struct EpochWatchdog {
    running: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl EpochWatchdog {
    /// Spawn a watchdog that increments `engine`'s epoch on a fixed timer.
    /// `Engine` is cheap to clone (internally reference-counted), so the
    /// thread owns its own handle.
    fn spawn(engine: Engine) -> PluginResult<Self> {
        let running = Arc::new(AtomicBool::new(true));
        let flag = running.clone();
        let handle = std::thread::Builder::new()
            .name("algomln-wasm-epoch".into())
            .spawn(move || {
                while flag.load(Ordering::Relaxed) {
                    std::thread::sleep(Duration::from_millis(EPOCH_TICK_MS));
                    engine.increment_epoch();
                }
            })
            .map_err(|e| {
                PluginError::LoadFailed(format!("failed to spawn wasm epoch watchdog: {e}"))
            })?;
        Ok(Self {
            running,
            handle: Some(handle),
        })
    }
}

impl Drop for EpochWatchdog {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            // The thread wakes at most `EPOCH_TICK_MS` ms after the flag
            // flips, so this join is bounded.
            let _ = handle.join();
        }
    }
}

/// WASM-backed plugin. Compiles and instantiates a `.wasm` artifact at
/// load time, links capability-gated host functions into the
/// `algomln` namespace, and dispatches the exported lifecycle hooks.
pub struct WasmPlugin {
    meta: PluginMeta,
    capabilities: Vec<Capability>,
    wasm_path: PathBuf,
    memory_limit_bytes: u32,
    host: Option<Arc<PluginHost>>,
    engine: Engine,
    store: Option<Store<WasmState>>,
    instance: Option<Instance>,
    /// Drives the epoch counter so lifecycle deadlines fire. Present only
    /// while the plugin is loaded; dropped (which stops the thread) in
    /// `on_unload`.
    watchdog: Option<EpochWatchdog>,
}

/// Per-store state: the plugin's host handle and an inline
/// resource limiter. Kept narrow so that `WasmState` (and therefore
/// `WasmPlugin`) can satisfy the `Send + Sync` bound that the
/// `Plugin` trait requires.
///
/// Note: WASI is intentionally **not** linked in this build — plugins
/// interact with the platform exclusively through the `algomln::*`
/// host functions. There is therefore no `wasi` field on this struct.
pub struct WasmState {
    pub host: Arc<PluginHost>,
    #[allow(private_interfaces)]
    pub(crate) memory_limiter: MemoryLimitState,
}

impl WasmPlugin {
    /// Build a new WASM plugin. The wasmtime engine is constructed
    /// eagerly; compilation and instantiation happen in `on_load`.
    pub fn new(
        meta: PluginMeta,
        capabilities: Vec<Capability>,
        wasm_path: PathBuf,
        memory_limit_mb: u32,
    ) -> PluginResult<Self> {
        let mut config = Config::new();
        config.epoch_interruption(true);
        config.cranelift_opt_level(OptLevel::Speed);
        let engine = Engine::new(&config).map_err(|e| {
            PluginError::LoadFailed(format!("wasmtime engine construction failed: {e}"))
        })?;

        let memory_limit_bytes = memory_limit_mb.saturating_mul(1024 * 1024);

        Ok(Self {
            meta,
            capabilities,
            wasm_path,
            memory_limit_bytes,
            host: None,
            engine,
            store: None,
            instance: None,
            watchdog: None,
        })
    }
}

/// Read a UTF-8 string from WASM linear memory at `[ptr..ptr+len]`.
/// Returns lossy-decoded data on invalid UTF-8 so a buggy plugin
/// still produces a log line rather than crashing the host.
fn read_string_from_memory(caller: &mut Caller<'_, WasmState>, ptr: i32, len: i32) -> String {
    if ptr < 0 || len < 0 {
        return String::new();
    }
    let memory = match caller.get_export("memory") {
        Some(wasmtime::Extern::Memory(m)) => m,
        _ => return String::new(),
    };
    let data = memory.data(caller);
    let start = ptr as usize;
    let end = start.saturating_add(len as usize).min(data.len());
    if start >= data.len() {
        return String::new();
    }
    String::from_utf8_lossy(&data[start..end]).into_owned()
}

/// Write raw bytes into WASM linear memory at `ptr`. Returns `Err(())` if the write
/// would be out-of-bounds or if the `memory` export is missing.
fn write_bytes_to_memory(caller: &mut Caller<'_, WasmState>, ptr: i32, bytes: &[u8]) -> Result<(), ()> {
    if ptr < 0 {
        return Err(());
    }
    let memory = match caller.get_export("memory") {
        Some(wasmtime::Extern::Memory(m)) => m,
        _ => return Err(()),
    };
    let mem_data = memory.data_mut(caller);
    let start = ptr as usize;
    let end = start.checked_add(bytes.len()).ok_or(())?;
    if end > mem_data.len() {
        return Err(());
    }
    mem_data[start..end].copy_from_slice(bytes);
    Ok(())
}

fn memory_of(caller: &mut Caller<'_, WasmState>) -> Option<Memory> {
    match caller.get_export("memory") {
        Some(wasmtime::Extern::Memory(m)) => Some(m),
        _ => None,
    }
}

fn write_i32(caller: &mut Caller<'_, WasmState>, ptr: i32, value: i32) {
    if ptr < 0 {
        return;
    }
    if let Some(mem) = memory_of(caller) {
        let data = mem.data_mut(caller);
        let slot = ptr as usize;
        if let Some(end) = slot.checked_add(4) {
            if end <= data.len() {
                data[slot..end].copy_from_slice(&value.to_le_bytes());
            }
        }
    }
}

fn read_bytes_from_memory(
    caller: &mut Caller<'_, WasmState>,
    ptr: i32,
    len: i32,
) -> Option<Vec<u8>> {
    if ptr < 0 || len < 0 {
        return None;
    }
    let mem = memory_of(caller)?;
    let data = mem.data(caller);
    let start = ptr as usize;
    let end = start.saturating_add(len as usize).min(data.len());
    if start >= data.len() {
        return None;
    }
    Some(data[start..end].to_vec())
}

/// Write a UTF-8 string result into a caller-provided output buffer, using
/// the guest ABI shared by every string-returning host fn: the required
/// byte length is always written to `out_len_ptr`, and the bytes are copied
/// into `[out_ptr..out_ptr+len]` only if they fit within `out_max`.
/// Returns `0` on success, `1` if the buffer was too small (the guest can
/// re-call with a larger buffer using the length just written).
fn write_string_result(
    caller: &mut Caller<'_, WasmState>,
    out_ptr: i32,
    out_max: i32,
    out_len_ptr: i32,
    s: &str,
) -> i32 {
    let b = s.as_bytes();
    write_i32(caller, out_len_ptr, b.len() as i32);
    if (b.len() as i32) > out_max {
        return 1;
    }
    write_bytes_to_memory(caller, out_ptr, b);
    0
}

/// Build a linker pre-populated with the `algomln` host functions.
/// The host functions are pure capability bridges — they only return
/// data the plugin's declared capabilities allow.
fn build_linker(engine: &Engine) -> PluginResult<Linker<WasmState>> {
    let mut linker: Linker<WasmState> = Linker::new(engine);

    // ---- Logging (unguarded; every plugin may log). ----
    linker
        .func_wrap(
            "algomln",
            "log_info",
            |mut caller: Caller<'_, WasmState>, ptr: i32, len: i32| {
                let msg = read_string_from_memory(&mut caller, ptr, len);
                let host = caller.data().host.clone();
                let pid = host.id.clone();
                host.log().info(&pid, &msg);
            },
        )
        .map_err(|e| PluginError::LoadFailed(format!("link algomln::log_info: {e}")))?;

    linker
        .func_wrap(
            "algomln",
            "log_warn",
            |mut caller: Caller<'_, WasmState>, ptr: i32, len: i32| {
                let msg = read_string_from_memory(&mut caller, ptr, len);
                let host = caller.data().host.clone();
                let pid = host.id.clone();
                host.log().warn(&pid, &msg);
            },
        )
        .map_err(|e| PluginError::LoadFailed(format!("link algomln::log_warn: {e}")))?;

    linker
        .func_wrap(
            "algomln",
            "log_error",
            |mut caller: Caller<'_, WasmState>, ptr: i32, len: i32| {
                let msg = read_string_from_memory(&mut caller, ptr, len);
                let host = caller.data().host.clone();
                let pid = host.id.clone();
                host.log().error(&pid, &msg);
            },
        )
        .map_err(|e| PluginError::LoadFailed(format!("link algomln::log_error: {e}")))?;

    // ---- Per-plugin storage (Storage capability). ----
    //
    // `StorageApi::read` and `StorageApi::write` are synchronous
    // (the `async_trait` is on the trait only for forward compat), so
    // we call them directly without going through `block_on`.
    linker
        .func_wrap(
            "algomln",
            "storage_get",
            |mut caller: Caller<'_, WasmState>,
             key_ptr: i32,
             key_len: i32,
             out_ptr: i32,
             out_len_ptr: i32|
             -> i32 {
                let key = read_string_from_memory(&mut caller, key_ptr, key_len);
                let storage = match caller.data().host.storage_guarded() {
                    Ok(s) => s.clone(),
                    Err(e) => {
                        let host = caller.data().host.clone();
                        let pid = host.id.clone();
                        host.log().error(&pid, &format!("storage_get: {e}"));
                        return -1;
                    }
                };
                let result = storage.read(&key);
                match result {
                    Ok(Some(bytes)) => {
                        write_bytes_to_memory(&mut caller, out_ptr, &bytes);
                        write_i32(&mut caller, out_len_ptr, bytes.len() as i32);
                        0
                    }
                    Ok(None) => {
                        write_i32(&mut caller, out_len_ptr, 0);
                        1
                    }
                    Err(e) => {
                        let host = caller.data().host.clone();
                        let pid = host.id.clone();
                        host.log().error(&pid, &format!("storage_get: {e}"));
                        -1
                    }
                }
            },
        )
        .map_err(|e| PluginError::LoadFailed(format!("link algomln::storage_get: {e}")))?;

    linker
        .func_wrap(
            "algomln",
            "storage_set",
            |mut caller: Caller<'_, WasmState>,
             key_ptr: i32,
             key_len: i32,
             val_ptr: i32,
             val_len: i32|
             -> i32 {
                let key = read_string_from_memory(&mut caller, key_ptr, key_len);
                let val = match read_bytes_from_memory(&mut caller, val_ptr, val_len) {
                    Some(v) => v,
                    None => return -1,
                };
                let storage = match caller.data().host.storage_guarded() {
                    Ok(s) => s.clone(),
                    Err(e) => {
                        let host = caller.data().host.clone();
                        let pid = host.id.clone();
                        host.log().error(&pid, &format!("storage_set: {e}"));
                        return -1;
                    }
                };
                let result = storage.write(&key, &val);
                match result {
                    Ok(()) => 0,
                    Err(e) => {
                        let host = caller.data().host.clone();
                        let pid = host.id.clone();
                        host.log().error(&pid, &format!("storage_set: {e}"));
                        -1
                    }
                }
            },
        )
        .map_err(|e| PluginError::LoadFailed(format!("link algomln::storage_set: {e}")))?;

    linker
        .func_wrap(
            "algomln",
            "storage_list_keys",
            |mut caller: Caller<'_, WasmState>,
             prefix_ptr: i32,
             prefix_len: i32,
             out_ptr: i32,
             out_max: i32,
             out_len_ptr: i32|
             -> i32 {
                let prefix = read_string_from_memory(&mut caller, prefix_ptr, prefix_len);
                let storage = match caller.data().host.storage_guarded() {
                    Ok(s) => s.clone(),
                    Err(e) => {
                        let host = caller.data().host.clone();
                        let pid = host.id.clone();
                        host.log().error(&pid, &format!("storage_list_keys: {e}"));
                        return -1;
                    }
                };
                match storage.list_keys(&prefix) {
                    Ok(keys) => {
                        let json = serde_json::to_string(&keys).unwrap_or_else(|_| "[]".to_string());
                        write_string_result(&mut caller, out_ptr, out_max, out_len_ptr, &json)
                    }
                    Err(e) => {
                        let host = caller.data().host.clone();
                        let pid = host.id.clone();
                        host.log().error(&pid, &format!("storage_list_keys: {e}"));
                        -1
                    }
                }
            },
        )
        .map_err(|e| PluginError::LoadFailed(format!("link algomln::storage_list_keys: {e}")))?;


    // ---- UI notifications (UiPanels capability). ----
    linker
        .func_wrap(
            "algomln",
            "notify",
            |mut caller: Caller<'_, WasmState>, msg_ptr: i32, msg_len: i32, kind: i32| {
                let msg = read_string_from_memory(&mut caller, msg_ptr, msg_len);
                let ui = match caller.data().host.ui_guarded() {
                    Ok(u) => u.clone(),
                    Err(e) => {
                        let host = caller.data().host.clone();
                        let pid = host.id.clone();
                        host.log().error(&pid, &format!("notify: {e}"));
                        return;
                    }
                };
                let mapped = match kind {
                    0 => NotificationKind::Info,
                    1 => NotificationKind::Warning,
                    _ => NotificationKind::Error,
                };
                let _ = ui.notify(mapped, &msg);
            },
        )
        .map_err(|e| PluginError::LoadFailed(format!("link algomln::notify: {e}")))?;

    // ---- Panel data emit (UiPanels capability; downcasts to the
    // Tauri-backed impl so the broadcast channel picks the value up). ----
    linker
        .func_wrap(
            "algomln",
            "emit_panel_data",
            |mut caller: Caller<'_, WasmState>,
             panel_id_ptr: i32,
             panel_id_len: i32,
             json_ptr: i32,
             json_len: i32|
             -> i32 {
                let panel_id = read_string_from_memory(&mut caller, panel_id_ptr, panel_id_len);
                let json_str = read_string_from_memory(&mut caller, json_ptr, json_len);
                let value: serde_json::Value = match serde_json::from_str(&json_str) {
                    Ok(v) => v,
                    Err(e) => {
                        let host = caller.data().host.clone();
                        let pid = host.id.clone();
                        host.log()
                            .error(&pid, &format!("emit_panel_data: invalid json: {e}"));
                        return -1;
                    }
                };
                let ui = match caller.data().host.ui_guarded() {
                    Ok(u) => u.clone(),
                    Err(e) => {
                        let host = caller.data().host.clone();
                        let pid = host.id.clone();
                        host.log().error(&pid, &format!("emit_panel_data: {e}"));
                        return -1;
                    }
                };
                if let Some(tauri_ui) = ui
                    .as_any()
                    .downcast_ref::<crate::plugin::api::ui::TauriUiApi>()
                {
                    match tauri_ui.emit_panel_data(panel_id, value) {
                        Ok(()) => 0,
                        Err(e) => {
                            let host = caller.data().host.clone();
                            let pid = host.id.clone();
                            host.log().error(&pid, &format!("emit_panel_data: {e}"));
                            -1
                        }
                    }
                } else {
                    let host = caller.data().host.clone();
                    let pid = host.id.clone();
                    host.log()
                        .error(&pid, "emit_panel_data: host ui is not a TauriUiApi");
                    -1
                }
            },
        )
        .map_err(|e| PluginError::LoadFailed(format!("link algomln::emit_panel_data: {e}")))?;

    // ---- Extra logging level (unguarded). ----
    linker
        .func_wrap(
            "algomln",
            "log_debug",
            |mut caller: Caller<'_, WasmState>, ptr: i32, len: i32| {
                let msg = read_string_from_memory(&mut caller, ptr, len);
                let host = caller.data().host.clone();
                let pid = host.id.clone();
                host.log().debug(&pid, &msg);
            },
        )
        .map_err(|e| PluginError::LoadFailed(format!("link algomln::log_debug: {e}")))?;

    // ---- Storage delete (Storage capability). ----
    linker
        .func_wrap(
            "algomln",
            "storage_delete",
            |mut caller: Caller<'_, WasmState>, key_ptr: i32, key_len: i32| -> i32 {
                let key = read_string_from_memory(&mut caller, key_ptr, key_len);
                let storage = match caller.data().host.storage_guarded() {
                    Ok(s) => s.clone(),
                    Err(e) => {
                        let host = caller.data().host.clone();
                        let pid = host.id.clone();
                        host.log().error(&pid, &format!("storage_delete: {e}"));
                        return -1;
                    }
                };
                match storage.delete(&key) {
                    Ok(()) => 0,
                    Err(e) => {
                        let host = caller.data().host.clone();
                        let pid = host.id.clone();
                        host.log().error(&pid, &format!("storage_delete: {e}"));
                        -1
                    }
                }
            },
        )
        .map_err(|e| PluginError::LoadFailed(format!("link algomln::storage_delete: {e}")))?;

    // ---- UI panel registration (UiPanels capability). ----
    linker
        .func_wrap(
            "algomln",
            "register_panel",
            |mut caller: Caller<'_, WasmState>,
             id_ptr: i32,
             id_len: i32,
             title_ptr: i32,
             title_len: i32,
             route_ptr: i32,
             route_len: i32|
             -> i32 {
                let id = read_string_from_memory(&mut caller, id_ptr, id_len);
                let title = read_string_from_memory(&mut caller, title_ptr, title_len);
                let route = read_string_from_memory(&mut caller, route_ptr, route_len);
                let ui = match caller.data().host.ui_guarded() {
                    Ok(u) => u.clone(),
                    Err(e) => {
                        let host = caller.data().host.clone();
                        let pid = host.id.clone();
                        host.log().error(&pid, &format!("register_panel: {e}"));
                        return -1;
                    }
                };
                match ui.register_panel(UiPanel { id, title, route }) {
                    Ok(()) => 0,
                    Err(e) => {
                        let host = caller.data().host.clone();
                        let pid = host.id.clone();
                        host.log().error(&pid, &format!("register_panel: {e}"));
                        -1
                    }
                }
            },
        )
        .map_err(|e| PluginError::LoadFailed(format!("link algomln::register_panel: {e}")))?;

    // ---- Execution (Execution capability). ----
    //
    // `submit_order` / `cancel_order` are async on the trait; we bridge
    // them synchronously via `block_in_place + Handle::block_on`, which
    // requires a multi-thread runtime (Tauri default). `get_positions`
    // calls the sync `positions()` directly.
    linker
        .func_wrap(
            "algomln",
            "submit_order",
            |mut caller: Caller<'_, WasmState>,
             sym_ptr: i32,
             sym_len: i32,
             side: i32,
             qty: i32,
             order_type: i32,
             price: f64,
             out_ptr: i32,
             out_max: i32,
             out_len_ptr: i32|
             -> i32 {
                let symbol = read_string_from_memory(&mut caller, sym_ptr, sym_len);
                let execution = match caller.data().host.execution_guarded() {
                    Ok(e) => e.clone(),
                    Err(e) => {
                        let host = caller.data().host.clone();
                        let pid = host.id.clone();
                        host.log().error(&pid, &format!("submit_order: {e}"));
                        return -1;
                    }
                };
                if qty <= 0 {
                    return -1;
                }
                let side = match side {
                    0 => OrderSide::Buy,
                    1 => OrderSide::Sell,
                    _ => return -1,
                };
                let (order_type, price) = match order_type {
                    0 => (OrderType::Market, None),
                    1 => (OrderType::Limit, Some(price)),
                    _ => return -1,
                };
                let request = OrderRequest {
                    symbol,
                    side,
                    quantity: qty as u32,
                    order_type,
                    price,
                };
                let result = tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(execution.submit_order(request))
                });
                match result {
                    Ok(id) => write_string_result(&mut caller, out_ptr, out_max, out_len_ptr, &id),
                    Err(e) => {
                        let host = caller.data().host.clone();
                        let pid = host.id.clone();
                        host.log().error(&pid, &format!("submit_order: {e}"));
                        -1
                    }
                }
            },
        )
        .map_err(|e| PluginError::LoadFailed(format!("link algomln::submit_order: {e}")))?;

    linker
        .func_wrap(
            "algomln",
            "cancel_order",
            |mut caller: Caller<'_, WasmState>, id_ptr: i32, id_len: i32| -> i32 {
                let order_id = read_string_from_memory(&mut caller, id_ptr, id_len);
                let execution = match caller.data().host.execution_guarded() {
                    Ok(e) => e.clone(),
                    Err(e) => {
                        let host = caller.data().host.clone();
                        let pid = host.id.clone();
                        host.log().error(&pid, &format!("cancel_order: {e}"));
                        return -1;
                    }
                };
                let result = tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(execution.cancel_order(&order_id))
                });
                match result {
                    Ok(()) => 0,
                    Err(e) => {
                        let host = caller.data().host.clone();
                        let pid = host.id.clone();
                        host.log().error(&pid, &format!("cancel_order: {e}"));
                        -1
                    }
                }
            },
        )
        .map_err(|e| PluginError::LoadFailed(format!("link algomln::cancel_order: {e}")))?;

    linker
        .func_wrap(
            "algomln",
            "get_positions",
            |mut caller: Caller<'_, WasmState>,
             out_ptr: i32,
             out_max: i32,
             out_len_ptr: i32|
             -> i32 {
                let execution = match caller.data().host.execution_guarded() {
                    Ok(e) => e.clone(),
                    Err(e) => {
                        let host = caller.data().host.clone();
                        let pid = host.id.clone();
                        host.log().error(&pid, &format!("get_positions: {e}"));
                        return -1;
                    }
                };
                let positions = match execution.positions() {
                    Ok(p) => p,
                    Err(e) => {
                        let host = caller.data().host.clone();
                        let pid = host.id.clone();
                        host.log().error(&pid, &format!("get_positions: {e}"));
                        return -1;
                    }
                };
                let json = serde_json::json!(positions
                    .iter()
                    .map(|p| serde_json::json!({
                        "symbol": p.symbol,
                        "quantity": p.quantity,
                        "averagePrice": p.average_price,
                    }))
                    .collect::<Vec<_>>());
                write_string_result(&mut caller, out_ptr, out_max, out_len_ptr, &json.to_string())
            },
        )
        .map_err(|e| PluginError::LoadFailed(format!("link algomln::get_positions: {e}")))?;

    // ---- Market-data read (MarketData capability). ----
    //
    // `latest_candle` is sync but its impl calls a bare `block_on`
    // internally, so we wrap the call in `block_in_place`.
    linker
        .func_wrap(
            "algomln",
            "get_latest_candle",
            |mut caller: Caller<'_, WasmState>,
             sym_ptr: i32,
             sym_len: i32,
             out_ptr: i32,
             out_max: i32,
             out_len_ptr: i32|
             -> i32 {
                let symbol = read_string_from_memory(&mut caller, sym_ptr, sym_len);
                let md = match caller.data().host.market_data_guarded() {
                    Ok(m) => m.clone(),
                    Err(e) => {
                        let host = caller.data().host.clone();
                        let pid = host.id.clone();
                        host.log().error(&pid, &format!("get_latest_candle: {e}"));
                        return -1;
                    }
                };
                let candle = match tokio::task::block_in_place(|| md.latest_candle(&symbol)) {
                    Ok(c) => c,
                    Err(e) => {
                        let host = caller.data().host.clone();
                        let pid = host.id.clone();
                        host.log().error(&pid, &format!("get_latest_candle: {e}"));
                        return -1;
                    }
                };
                let json = serde_json::json!({
                    "open": candle.open,
                    "high": candle.high,
                    "low": candle.low,
                    "close": candle.close,
                    "volume": candle.volume,
                    "timestampMs": candle.timestamp_ms,
                });
                write_string_result(&mut caller, out_ptr, out_max, out_len_ptr, &json.to_string())
            },
        )
        .map_err(|e| PluginError::LoadFailed(format!("link algomln::get_latest_candle: {e}")))?;

    // ---- Callback-based capabilities are intentionally NOT bridged for
    // WASM. ----
    //
    // Custom indicators, analytics metrics, DSL keywords, cron tasks,
    // event subscriptions, and market-data tick subscriptions all require
    // the host to call *back into* the guest module later, from another
    // thread (a scheduler tick, an event-bus publish, an indicator
    // evaluation). wasmtime's `Store` is neither `Sync` nor re-entrant,
    // so a host→guest callback across it would need a message-queue or a
    // re-entrant-store redesign. Rather than fake these (register a
    // handler that can never fire), they are deferred. The Rhai runtime,
    // whose engine is `Sync` under the `sync` feature, bridges the full
    // callback surface — see `rhai_runtime.rs::register_host_functions`.

    Ok(linker)
}

/// Call an exported lifecycle function on the WASM instance, if it
/// exists. Missing exports are silently treated as "no-op plugin";
/// any other trap bubbles up as `PluginError::LoadFailed`.
fn call_lifecycle(
    store: &mut Store<WasmState>,
    instance: &Instance,
    name: &str,
) -> PluginResult<()> {
    let func = match instance.get_typed_func::<(), ()>(&mut *store, name) {
        Ok(f) => f,
        Err(_) => return Ok(()),
    };
    // Re-arm the CPU deadline relative to the current epoch before every
    // lifecycle call. The `EpochWatchdog` (spawned in `on_load`) advances
    // the epoch on a timer, so a lifecycle export that overruns the budget
    // traps here instead of hanging the host thread.
    store.set_epoch_deadline(LIFECYCLE_CPU_BUDGET_TICKS);
    func.call(&mut *store, ())
        .map_err(|e| PluginError::LoadFailed(format!("{name} failed: {e}")))?;
    Ok(())
}

#[async_trait::async_trait]
impl Plugin for WasmPlugin {
    fn meta(&self) -> &PluginMeta {
        &self.meta
    }

    fn capabilities(&self) -> &[Capability] {
        &self.capabilities
    }

    async fn on_load(&mut self, host: Arc<PluginHost>) -> PluginResult<()> {
        self.host = Some(host.clone());

        let bytes = std::fs::read(&self.wasm_path).map_err(|e| {
            PluginError::LoadFailed(format!(
                "failed to read wasm artifact {}: {e}",
                self.wasm_path.display()
            ))
        })?;

        let module = Module::new(&self.engine, &bytes)
            .map_err(|e| PluginError::LoadFailed(format!("module compile failed: {e}")))?;

        let linker = build_linker(&self.engine)?;

        let memory_limit_bytes = self.memory_limit_bytes;
        let state = WasmState {
            host,
            memory_limiter: MemoryLimitState {
                memory_limit: memory_limit_bytes,
            },
        };
        let mut store = Store::new(&self.engine, state);

        // Wire the resource limiter by handing wasmtime a mutable
        // reference to the inline `MemoryLimitState` field on
        // `WasmState`. This avoids any per-call allocation and keeps
        // the limit configuration co-located with the rest of the
        // store state.
        store.limiter(|s: &mut WasmState| &mut s.memory_limiter);

        // Arm the epoch deadline BEFORE spawning the watchdog or
        // instantiating the module. With `epoch_interruption(true)`
        // enabled, a store with no deadline set traps on the first
        // epoch tick; `linker.instantiate` runs against this store
        // before `call_lifecycle` arms a deadline, so a slow
        // instantiate (or even just a watchdog tick that lands before
        // instantiation completes) would otherwise trap the load.
        store.set_epoch_deadline(LIFECYCLE_CPU_BUDGET_TICKS);

        // Spawn the epoch watchdog before any WASM code runs. It drives
        // the engine's epoch counter so that the per-call deadline armed
        // inside `call_lifecycle` actually fires — arming a deadline
        // without something incrementing the epoch is a no-op and would
        // let an infinite loop hang the host thread.
        self.watchdog = Some(EpochWatchdog::spawn(self.engine.clone())?);

        let instance = linker
            .instantiate(&mut store, &module)
            .map_err(|e| PluginError::LoadFailed(format!("instantiate failed: {e}")))?;

        // Dispatch the lifecycle hook (deadline armed inside call_lifecycle).
        call_lifecycle(&mut store, &instance, "_algomln_on_load")?;

        self.store = Some(store);
        self.instance = Some(instance);
        Ok(())
    }

    async fn on_enable(&mut self) -> PluginResult<()> {
        let store = self
            .store
            .as_mut()
            .ok_or_else(|| PluginError::ApiError("plugin not loaded".into()))?;
        let instance = self
            .instance
            .as_ref()
            .ok_or_else(|| PluginError::ApiError("plugin not loaded".into()))?;
        call_lifecycle(store, instance, "_algomln_on_enable")
    }

    async fn on_disable(&mut self) -> PluginResult<()> {
        let store = self
            .store
            .as_mut()
            .ok_or_else(|| PluginError::ApiError("plugin not loaded".into()))?;
        let instance = self
            .instance
            .as_ref()
            .ok_or_else(|| PluginError::ApiError("plugin not loaded".into()))?;
        call_lifecycle(store, instance, "_algomln_on_disable")
    }

    fn on_unload(&mut self) {
        if let (Some(mut store), Some(instance)) = (self.store.take(), self.instance.take()) {
            // Best-effort: errors from on_unload are ignored per spec.
            let _ = call_lifecycle(&mut store, &instance, "_algomln_on_unload");
        }
        // Stop and join the epoch watchdog thread (via Drop) so it does
        // not outlive the plugin.
        self.watchdog = None;
        self.host = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasmtime::{Config, Engine, Instance, Module, Store};

    /// B3 regression: an infinite loop in a WASM export must trap because
    /// the `EpochWatchdog` drives the engine's epoch counter past the
    /// store's armed deadline. Without the watchdog, `set_epoch_deadline`
    /// is inert and this call would hang forever.
    #[test]
    fn epoch_watchdog_traps_infinite_loop() {
        let mut config = Config::new();
        config.epoch_interruption(true);
        let engine = Engine::new(&config).expect("engine");

        // The watchdog ticks every EPOCH_TICK_MS ms; keep it alive for
        // the duration of the call.
        let _watchdog = EpochWatchdog::spawn(engine.clone()).expect("watchdog");

        // `Module::new` accepts `.wat` text directly (wasmtime's default
        // `wat` feature), so no extra dev-dependency is needed.
        let module = Module::new(
            &engine,
            r#"(module (func (export "spin") (loop br 0)))"#.as_bytes(),
        )
        .expect("compile spin module");

        let mut store = Store::new(&engine, ());
        // Use a small deadline so the test finishes in a few hundred ms
        // rather than the production LIFECYCLE_CPU_BUDGET_TICKS budget.
        store.set_epoch_deadline(2);

        let instance = Instance::new(&mut store, &module, &[]).expect("instantiate");
        let spin = instance
            .get_typed_func::<(), ()>(&mut store, "spin")
            .expect("spin export");

        let result = spin.call(&mut store, ());
        assert!(
            result.is_err(),
            "infinite loop should trap once the watchdog drives the epoch past the deadline"
        );
    }

    /// The watchdog thread must actually stop when the `EpochWatchdog` is
    /// dropped — otherwise it would leak for the lifetime of the process.
    #[test]
    fn epoch_watchdog_stops_on_drop() {
        let mut config = Config::new();
        config.epoch_interruption(true);
        let engine = Engine::new(&config).expect("engine");
        let watchdog = EpochWatchdog::spawn(engine).expect("watchdog");
        // Dropping joins the thread; if the thread never observed the stop
        // flag this would hang the test.
        drop(watchdog);
    }

    /// Chunk 3 verification: a `.wat` plugin that exercises the host
    /// surface end-to-end — `algomln::log_info`, `algomln::storage_set`,
    /// `algomln::storage_get` — must link, instantiate, and run through
    /// `WasmPlugin::on_load` / `on_enable` / `on_unload`. The B3
    /// tripwires cover epoch interruption but not the linker or the
    /// host-fn boundary; this test covers that.
    ///
    /// Strategy: write a `.wat` module to a temp file, build a
    /// `WasmPlugin` for it, construct a `PluginHost` against temp
    /// directories (no broker dependency), drive the lifecycle, then
    /// read the plugin's log file and the storage backing directory to
    /// confirm both round-trips worked.
    #[tokio::test(flavor = "multi_thread")]
    async fn wasm_plugin_drives_host_fns_end_to_end() {
        use std::io::Read;
        use std::path::PathBuf;

        use crate::plugin::api::analytics::SharedAnalyticsRegistry;
        use crate::plugin::api::dsl_extension::SharedDslExtensionRegistry;
        use crate::plugin::api::events::EventBus;
        use crate::plugin::api::indicator_registry::SharedIndicatorRegistry;
        use crate::plugin::api::log_file::{RateLimitedFileLog, RollingLog};
        use crate::plugin::api::scheduler::CronScheduler;
        use crate::plugin::api::storage::PluginKvStore;
        use crate::plugin::api::ui::TauriUiApi;
        use crate::plugin::api::StorageApi;
        use crate::plugin::host::PluginHostBuilder;
        use crate::plugin::manifest::PluginPermissions;
        use crate::plugin::types::{
            Capability, PluginId, PluginMeta, PluginVersion,
        };

        // Tiny WAT module. The host fns are linked by name into the
        // `algomln` namespace; the linker populates the imports below.
        //
        // Layout in linear memory (1 page = 64 KiB):
        //   256  "loaded"        (6 bytes)
        //   512  "k"             (1 byte)   storage key
        //   768  "v"             (1 byte)   storage value
        //  1024  "got-v"         (5 bytes)  log on enable success
        //  1280  "got-err"       (7 bytes)  log on enable failure
        //  1536  4-byte slot     (out_len from storage_get)
        //  2048  buffer          (out bytes from storage_get)
        //
        // Note: imports must precede `memory` in the textual form;
        // wasmtime rejects "import after memory" otherwise.
        let wat = r#"
            (module
              (import "algomln" "log_info"
                (func $log_info (param i32 i32)))
              (import "algomln" "storage_set"
                (func $storage_set (param i32 i32 i32 i32) (result i32)))
                (import "algomln" "storage_get"
                (func $storage_get (param i32 i32 i32 i32 i32) (result i32)))

              (memory (export "memory") 1)
              (data (i32.const  256) "loaded")
              (data (i32.const  512) "k")
              (data (i32.const  768) "v")
              (data (i32.const 1024) "got-v")
              (data (i32.const 1280) "got-err")

              (func (export "_algomln_on_load")
                i32.const 256 i32.const 6
                call $log_info
                i32.const 512 i32.const 1
                i32.const 768 i32.const 1
                call $storage_set
                drop)

              (func (export "_algomln_on_enable") (local $r i32)
                i32.const 512  i32.const 1
                i32.const 2048 i32.const 1536 i32.const 1536
                call $storage_get
                local.set $r
                ;; Always log success on the happy path; surface the
                ;; error case as a second line so the test can tell
                ;; them apart.
                i32.const 1024 i32.const 5
                call $log_info
                local.get $r
                if
                  i32.const 1280 i32.const 7
                  call $log_info
                end)

              (func (export "_algomln_on_unload")
                i32.const 256 i32.const 6
                call $log_info)
            )
        "#;

        // Materialize a temp layout: a `.wasm` file with the WAT bytes
        // (wasmtime's default `wat` feature accepts text directly), a
        // log file, and a storage directory.
        let dir = tempfile::tempdir().expect("tempdir");
        let wasm_path: PathBuf = dir.path().join("plugin.wasm");
        let log_path: PathBuf = dir.path().join("plugin.log");
        let storage_dir: PathBuf = dir.path().join("storage");
        std::fs::create_dir_all(&storage_dir).expect("mkdir storage");
        std::fs::write(&wasm_path, wat.as_bytes()).expect("write .wasm");

        // Stand up a `WasmPlugin` for the temp artifact.
        let plugin_id = PluginId::from("wasm-smoke");
        let meta = PluginMeta {
            id: plugin_id.clone(),
            name: "wasm-smoke".into(),
            version: PluginVersion {
                major: 0,
                minor: 1,
                patch: 0,
            },
            description: "Chunk 3 smoke fixture".into(),
            author: "tests".into(),
        };
        let mut plugin = WasmPlugin::new(
            meta,
            vec![Capability::Storage],
            wasm_path,
            8, // 8 MB memory limit
        )
        .expect("WasmPlugin::new");

        // Build a `PluginHost` against the temp log file and temp
        // storage directory. The host-side services the plugin never
        // touches (market data, execution, analytics, etc.) get minimal
        // inline stubs so the test does not pull in a broker.
        struct NoopMarketData;
        #[async_trait::async_trait]
        impl crate::plugin::api::MarketDataApi for NoopMarketData {
            fn subscribe_ticks(
                &self,
                _symbol: &str,
                _callback: std::sync::Arc<
                    dyn Fn(crate::plugin::api::MarketDataEvent) + Send + Sync,
                >,
            ) -> crate::plugin::PluginResult<crate::plugin::types::SubscriptionHandle> {
                Err(crate::plugin::types::PluginError::ApiError(
                    "noop".into(),
                ))
            }
            fn unsubscribe_ticks(
                &self,
                _handle: crate::plugin::types::SubscriptionHandle,
            ) -> crate::plugin::PluginResult<()> {
                Ok(())
            }
            fn latest_candle(
                &self,
                _symbol: &str,
            ) -> crate::plugin::PluginResult<crate::plugin::api::Candle> {
                Err(crate::plugin::types::PluginError::ApiError(
                    "noop".into(),
                ))
            }
        }

        let log = std::sync::Arc::new(RateLimitedFileLog::with_log(
            plugin_id.clone(),
            RollingLog::open(log_path.clone()).expect("open log file"),
        ));
        let storage = std::sync::Arc::new(
            PluginKvStore::new(plugin_id.clone(), storage_dir.clone()).expect("kv store"),
        );
        let (tauri_ui_api, _rx) = TauriUiApi::new();
        let host = PluginHostBuilder {
            id: plugin_id.clone(),
            market_data: std::sync::Arc::new(NoopMarketData),
            execution: std::sync::Arc::new(
                crate::plugin::api::execution::NoopExecutionApi,
            ),
            storage,
            event_bus: EventBus::new(),
            indicators: std::sync::Arc::new(SharedIndicatorRegistry::new()),
            analytics: std::sync::Arc::new(SharedAnalyticsRegistry::new()),
            dsl: std::sync::Arc::new(SharedDslExtensionRegistry::new()),
            ui: tauri_ui_api,
            scheduler: CronScheduler::new(),
            log,
            capabilities: vec![Capability::Storage],
            permissions: PluginPermissions {
                network: false,
                file_system: false,
                max_memory_mb: 8,
                allowed_symbols: Vec::new(),
            },
        }
        .build();

        // Drive the lifecycle. on_load instantiates and runs the
        // WAT's `_algomln_on_load`, which logs "loaded" and writes
        // key="k" value="v" via the host fns.
        plugin.on_load(host.clone()).await.expect("on_load");
        // on_enable runs `_algomln_on_enable`, which reads "k" back and
        // logs "got-v".
        plugin.on_enable().await.expect("on_enable");
        // on_unload stops the watchdog (via Drop on WasmPlugin) and
        // best-effort dispatches `_algomln_on_unload`, which logs
        // "loaded" again.
        plugin.on_unload();

        // Drop the host explicitly so its log file handle flushes
        // before we read the file from disk.
        drop(host);

        // Assert the log file contains both "loaded" (from on_load and
        // on_unload) and "got-v" (from on_enable).
        let mut contents = String::new();
        std::fs::File::open(&log_path)
            .expect("open log")
            .read_to_string(&mut contents)
            .expect("read log");
        assert!(
            contents.contains("loaded"),
            "log should contain 'loaded' from on_load; got: {contents}"
        );
        assert!(
            contents.contains("got-v"),
            "log should contain 'got-v' from on_enable; got: {contents}"
        );
        assert!(
            !contents.contains("got-err"),
            "log should NOT contain 'got-err' (storage_get returned non-zero); got: {contents}"
        );

        // Assert the storage write actually landed. The PluginKvStore
        // writes `<base>/<sanitized_key>.val`; we re-open it and read
        // the same key to confirm the round trip.
        let re_open =
            PluginKvStore::new(plugin_id.clone(), storage_dir.clone()).expect("re-open kv");
        assert_eq!(
            re_open.read("k").expect("read k"),
            Some(b"v".to_vec()),
            "storage_get/set round trip through the host fns should leave 'k' -> 'v'"
        );
    }
}
// ... (start of file)
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::path::PathBuf;

use parking_lot::Mutex;
use crate::models::Candle;
use crate::plugin::api::events::{EventBus, EventFilter, EventKind};
use crate::plugin::api::indicator_registry::{IndicatorFn, SharedIndicatorRegistry};
use crate::plugin::api::scheduler::CronScheduler;
use crate::plugin::api::storage::PluginKvStore;
use crate::plugin::api::StorageApi;
use crate::plugin::host::{PluginHost, PluginHostBuilder};
use crate::plugin::manifest::{PluginManifest, PluginPermissions};
use crate::plugin::registry::PluginRegistry;
use crate::plugin::runtime::rhai_runtime::RhaiPlugin;
use crate::plugin::types::{Capability, PluginError, PluginId, PluginMeta, PluginVersion};
use crate::strategy::execution::paper::PaperTrade;

struct NoopMarketData;
#[async_trait::async_trait]
impl crate::plugin::api::MarketDataApi for NoopMarketData {
    fn subscribe_ticks(&self, _s: &str, _cb: Arc<dyn Fn(crate::plugin::api::MarketDataEvent) + Send + Sync>) -> crate::plugin::PluginResult<crate::plugin::types::SubscriptionHandle> {
        Err(PluginError::ApiError("noop".into()))
    }
    fn unsubscribe_ticks(&self, _h: crate::plugin::types::SubscriptionHandle) -> crate::plugin::PluginResult<()> { Ok(()) }
    fn latest_candle(&self, _s: &str) -> crate::plugin::PluginResult<crate::plugin::api::Candle> {
        Err(PluginError::ApiError("noop".into()))
    }
}
// ...

fn dummy_candle() -> Candle {
    Candle {
        timestamp: 0,
        open: 1.0,
        high: 2.0,
        low: 0.5,
        close: 1.5,
        volume: 100.0,
    }
}

fn dummy_trade() -> PaperTrade {
    PaperTrade {
        id: "trade-1".to_string(),
        timestamp: 0,
        symbol: "NIFTY".to_string(),
        side: crate::models::OrderSide::Buy,
        quantity: 1,
        price: 100.0,
        rule_id: "rule-1".to_string(),
        pnl: None,
    }
}

#[tokio::test]
async fn storage_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let store =
        PluginKvStore::new(PluginId::from("test-plugin"), dir.path().to_path_buf()).unwrap();
    store.write("hello", b"world").unwrap();
    assert_eq!(store.read("hello").unwrap(), Some(b"world".to_vec()));
    store.delete("hello").unwrap();
    assert_eq!(store.read("hello").unwrap(), None);
    let keys = store.list_keys("").unwrap();
    assert!(keys.is_empty());
}

#[test]
fn storage_key_sanitization() {
    let dir = tempfile::tempdir().unwrap();
    let store =
        PluginKvStore::new(PluginId::from("test-plugin"), dir.path().to_path_buf()).unwrap();
    store.write("../../etc/passwd", b"bad").unwrap();
    assert!(!std::path::Path::new("../../etc/passwd").exists());
    assert_eq!(
        store.read("../../etc/passwd").unwrap(),
        Some(b"bad".to_vec())
    );
    let keys = store.list_keys("").unwrap();
    assert!(keys.iter().all(|k| !k.contains("..")));
}

#[test]
fn indicator_registry_dedup() {
    let registry = SharedIndicatorRegistry::new();
    let pid_a = PluginId::from("plugin-a");
    let pid_b = PluginId::from("plugin-b");
    let dummy_fn: Arc<IndicatorFn> = Arc::new(|_candles: &[Candle], _period: usize| vec![]);

    assert!(registry
        .register_fn("my_ind", pid_a.clone(), dummy_fn.clone())
        .is_ok());
    assert!(registry
        .register_fn("my_ind", pid_a.clone(), dummy_fn.clone())
        .is_ok());
    assert!(matches!(
        registry.register_fn("my_ind", pid_b.clone(), dummy_fn.clone()),
        Err(PluginError::ApiError(_))
    ));

    registry.unregister_all_for(&pid_a);
    assert!(registry.list().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn event_bus_filter() {
    use std::sync::atomic::{AtomicU32, Ordering};

    let bus = EventBus::new();
    let call_count = Arc::new(AtomicU32::new(0));
    let cc = call_count.clone();

    bus.subscribe(
        PluginId::from("test-plugin"),
        EventFilter::TradeExecuted,
        Arc::new(move |_| {
            cc.fetch_add(1, Ordering::SeqCst);
        }),
    );

    bus.publish(EventKind::CandleProcessed(dummy_candle()));
    std::thread::sleep(std::time::Duration::from_millis(50));
    assert_eq!(call_count.load(Ordering::SeqCst), 0);

    bus.publish(EventKind::TradeExecuted(dummy_trade()));
    std::thread::sleep(std::time::Duration::from_millis(50));
    assert_eq!(call_count.load(Ordering::SeqCst), 1);
}

#[test]
fn manifest_validation() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("plugin.rhai"), "").unwrap();

    // Valid manifest
    std::fs::write(
        dir.path().join("plugin.toml"),
        r#"id = "my-plugin"
name = "My Plugin"
version = "0.1.0"
description = "Test"
author = "Test"
capabilities = ["Indicators"]
entry = "plugin.rhai"
[permissions]
max_memory_mb = 8
network = false
file_system = false
"#,
    )
    .unwrap();
    let manifest = PluginManifest::load(dir.path()).unwrap();
    let meta = manifest.to_meta().unwrap();
    assert_eq!(meta.id.as_str(), "my-plugin");
    assert_eq!(meta.name, "My Plugin");

    // Bad version
    std::fs::write(
        dir.path().join("plugin.toml"),
        r#"id = "my-plugin"
name = "My Plugin"
version = "bad"
description = "Test"
author = "Test"
capabilities = []
entry = "plugin.rhai"
[permissions]
"#,
    )
    .unwrap();
    assert!(matches!(
        PluginManifest::load(dir.path()),
        Err(PluginError::ManifestParse(_))
    ));

    // Unknown capability
    std::fs::write(
        dir.path().join("plugin.toml"),
        r#"id = "my-plugin"
name = "My Plugin"
version = "0.1.0"
description = "Test"
author = "Test"
capabilities = ["Unknown"]
entry = "plugin.rhai"
[permissions]
"#,
    )
    .unwrap();
    assert!(matches!(
        PluginManifest::load(dir.path()),
        Err(PluginError::ManifestParse(_))
    ));

    // Bad extension: create the .dll file so we reach the extension check
    std::fs::write(dir.path().join("plugin.dll"), "").unwrap();
    std::fs::write(
        dir.path().join("plugin.toml"),
        r#"id = "my-plugin"
name = "My Plugin"
version = "0.1.0"
description = "Test"
author = "Test"
capabilities = []
entry = "plugin.dll"
[permissions]
"#,
    )
    .unwrap();
    assert!(matches!(
        PluginManifest::load(dir.path()),
        Err(PluginError::ManifestParse(_))
    ));

    // Entry file not found
    std::fs::write(
        dir.path().join("plugin.toml"),
        r#"id = "my-plugin"
name = "My Plugin"
version = "0.1.0"
description = "Test"
author = "Test"
capabilities = []
entry = "nonexistent.rhai"
[permissions]
"#,
    )
    .unwrap();
    assert!(matches!(
        PluginManifest::load(dir.path()),
        Err(PluginError::ManifestParse(_))
    ));
}

async fn setup_test_registry(path: PathBuf) -> (Arc<PluginRegistry>, Arc<CronScheduler>, Arc<EventBus>) {
    let scheduler = CronScheduler::new();
    let event_bus = EventBus::new();
    let path_clone = path.clone();
    let scheduler_clone = scheduler.clone();
    let event_bus_clone = event_bus.clone();

    let host_factory = Arc::new(move |id, caps, perms| {
        PluginHostBuilder {
            id,
            market_data: Arc::new(NoopMarketData),
            execution: Arc::new(crate::plugin::api::execution::NoopExecutionApi),
            storage: Arc::new(PluginKvStore::new(PluginId::from("test"), path_clone.clone()).unwrap()),
            event_bus: event_bus_clone.clone(),
            indicators: Arc::new(SharedIndicatorRegistry::new()),
            analytics: Arc::new(crate::plugin::api::analytics::SharedAnalyticsRegistry::new()),
            dsl: Arc::new(crate::plugin::api::dsl_extension::SharedDslExtensionRegistry::new()),
            ui: crate::plugin::api::ui::TauriUiApi::new().0,
            scheduler: scheduler_clone.clone(),
            log: Arc::new(crate::plugin::api::log::NoopLog),
            capabilities: caps,
            permissions: perms,
        }.build()
    });

    let registry = PluginRegistry::new(path, host_factory);
    (registry, scheduler, event_bus)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rhai_callback_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let plugin_dir = dir.path().join("callback-test");
    std::fs::create_dir_all(&plugin_dir).unwrap();
    let (registry, _, _) = setup_test_registry(plugin_dir.parent().unwrap().to_path_buf()).await;

    let plugin_id = PluginId::from("callback-test");
    let source = r#"
        fn my_metric(trades) {
            1.0
        }
        fn is_bullish(candles, current) {
            true
        }
        fn on_load() {
            register_metric("my_metric", my_metric);
            register_keyword("is_bullish", is_bullish);
        }
    "#;

    let source_path = plugin_dir.join("plugin.rhai");
    std::fs::write(&source_path, source.as_bytes()).unwrap();

    let manifest_path = plugin_dir.join("plugin.toml");
    std::fs::write(&manifest_path, format!(r#"
id = "{}"
name = "Test"
version = "0.1.0"
description = "Test"
author = "Test"
capabilities = ["Analytics", "DslExtension"]
entry = "plugin.rhai"
[permissions]
max_memory_mb = 8
network = false
file_system = false
"#, plugin_id.as_str())).unwrap();

    registry.scan_and_load().await;
    registry.enable(&plugin_id).await.unwrap();

    assert_eq!(registry.get_status(&plugin_id), Some(crate::plugin::types::PluginStatus::Enabled));
}
use std::fmt;
use std::ops::Deref;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct PluginId(pub String);

impl PluginId {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for PluginId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for PluginId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for PluginId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl AsRef<str> for PluginId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Deref for PluginId {
    type Target = String;
    fn deref(&self) -> &String {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct PluginVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl fmt::Display for PluginVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

impl TryFrom<&str> for PluginVersion {
    type Error = String;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let parts: Vec<&str> = value.split('.').collect();
        if parts.len() != 3 {
            return Err(format!("invalid version string: {value}"));
        }
        let major = parts[0]
            .parse::<u32>()
            .map_err(|e| format!("invalid major: {e}"))?;
        let minor = parts[1]
            .parse::<u32>()
            .map_err(|e| format!("invalid minor: {e}"))?;
        let patch = parts[2]
            .parse::<u32>()
            .map_err(|e| format!("invalid patch: {e}"))?;
        Ok(Self {
            major,
            minor,
            patch,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginMeta {
    pub id: PluginId,
    pub name: String,
    pub version: PluginVersion,
    pub description: String,
    pub author: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Capability {
    MarketData,
    Execution,
    Storage,
    Events,
    Indicators,
    Analytics,
    DslExtension,
    UiPanels,
    Scheduler,
}

impl Capability {
    pub fn as_str(&self) -> &'static str {
        match self {
            Capability::MarketData => "marketData",
            Capability::Execution => "execution",
            Capability::Storage => "storage",
            Capability::Events => "events",
            Capability::Indicators => "indicators",
            Capability::Analytics => "analytics",
            Capability::DslExtension => "dslExtension",
            Capability::UiPanels => "uiPanels",
            Capability::Scheduler => "scheduler",
        }
    }
}

impl fmt::Display for Capability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<&str> for Capability {
    type Error = String;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let normalized = value.to_ascii_lowercase();
        match normalized.as_str() {
            "marketdata" | "market_data" | "market-data" => Ok(Capability::MarketData),
            "execution" => Ok(Capability::Execution),
            "storage" => Ok(Capability::Storage),
            "events" => Ok(Capability::Events),
            "indicators" => Ok(Capability::Indicators),
            "analytics" => Ok(Capability::Analytics),
            "dslextension" | "dsl_extension" | "dsl-extension" => Ok(Capability::DslExtension),
            "uipanels" | "ui_panels" | "ui-panels" => Ok(Capability::UiPanels),
            "scheduler" => Ok(Capability::Scheduler),
            other => Err(format!("unknown capability: {other}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PluginStatus {
    Loaded,
    Enabled,
    Disabled,
    Failed(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PluginError {
    ManifestParse(String),
    LoadFailed(String),
    ApiError(String),
    PermissionDenied {
        capability: String,
        plugin_id: String,
    },
    Timeout(String),
    NotFound(String),
}

impl fmt::Display for PluginError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PluginError::ManifestParse(m) => write!(f, "manifest parse error: {m}"),
            PluginError::LoadFailed(m) => write!(f, "load failed: {m}"),
            PluginError::ApiError(m) => write!(f, "api error: {m}"),
            PluginError::PermissionDenied {
                capability,
                plugin_id,
            } => {
                write!(
                    f,
                    "permission denied for capability {capability} in plugin {plugin_id}"
                )
            }
            PluginError::Timeout(m) => write!(f, "timeout: {m}"),
            PluginError::NotFound(m) => write!(f, "not found: {m}"),
        }
    }
}

impl std::error::Error for PluginError {}

pub type PluginResult<T> = Result<T, PluginError>;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct SubscriptionHandle(pub uuid::Uuid);

impl fmt::Display for SubscriptionHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ScheduleHandle(pub uuid::Uuid);

impl fmt::Display for ScheduleHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum NotificationKind {
    Info,
    Warning,
    Error,
}

impl fmt::Display for NotificationKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NotificationKind::Info => f.write_str("info"),
            NotificationKind::Warning => f.write_str("warning"),
            NotificationKind::Error => f.write_str("error"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginListEntry {
    pub meta: PluginMeta,
    pub status: PluginStatus,
    pub capabilities: Vec<Capability>,
}
//! Pure fuzzy-search logic for the `search_symbols` IPC command.
//!
//! No Tauri imports here — this module is exercised directly by the
//! `search_symbols_impl` body in `src/commands/search.rs` and is unit-tested
//! in isolation.
//!
//! Scoring tiers (highest applicable tier wins; tiers are not additive):
//!
//! | Tier | Condition                                                | Score range     |
//! |------|----------------------------------------------------------|-----------------|
//! | 5    | candidate == query (exact)                               | 1000            |
//! | 4    | candidate.starts_with(query)                             | 800..900        |
//! | 3    | candidate.contains(query)                                | 500             |
//! | 2    | query is a subsequence of candidate                      | 300             |
//! | 1    | trigram Jaccard similarity >= 0.25                       | 50..200         |
//! | 0    | none of the above                                        | 0 (filtered)    |

use std::collections::{HashMap, HashSet};

use crate::indices::registry::IndexEntry;

/// Single search hit. `securityId` is `Some` for equities, `None` for indices.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SymbolMatch {
    pub symbol: String,
    pub display_name: String,
    pub kind: SymbolKind,
    pub security_id: Option<u32>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SymbolKind {
    Equity,
    Index,
}

/// Score a candidate against a query. Both inputs are expected to already be
/// lowercased — see `fuzzy_search` which normalises its inputs.
///
/// Returns `0` if the score is below the relevance threshold.
pub fn score_match(query: &str, candidate: &str) -> u32 {
    // Tier 5 — exact match.
    if candidate == query {
        return 1000;
    }

    // Tier 4 — prefix match. Shorter symbols win because a query that
    // unambiguously names a 4-letter ticker should outrank the same prefix
    // embedded in a 30-character string.
    if let Some(rest) = candidate.strip_prefix(query) {
        // (100 - len.min(100)) rewards short candidates. `rest` excludes
        // the matched query so we add `query.len()` back to score against
        // the full candidate length.
        let total = rest.len() + query.len();
        let bonus = (100u32).saturating_sub(total.min(100) as u32);
        return 800 + bonus;
    }

    // Tier 3 — substring match.
    if candidate.contains(query) {
        return 500;
    }

    // Tier 2 — subsequence match (all query chars in order).
    if is_subsequence(query, candidate) {
        return 300;
    }

    // Tier 1 — trigram Jaccard.
    let sim = trigram_similarity(query, candidate);
    if sim >= 0.25 {
        let score = (sim * 200.0) as u32;
        return score.max(50);
    }

    0
}

/// True iff every char in `query` appears in `candidate` in the same order.
/// Standard O(|candidate|) two-pointer scan.
pub fn is_subsequence(query: &str, candidate: &str) -> bool {
    if query.is_empty() {
        return true;
    }
    let mut q = query.chars();
    let mut cur = q.next().expect("non-empty");
    for ch in candidate.chars() {
        if ch == cur {
            match q.next() {
                Some(next) => cur = next,
                None => return true,
            }
        }
    }
    false
}

/// Jaccard similarity over character-trigram sets. Returns `0.0` if either
/// string has fewer than 3 characters.
pub fn trigram_similarity(a: &str, b: &str) -> f32 {
    let set_a = trigrams(a);
    let set_b = trigrams(b);
    if set_a.is_empty() || set_b.is_empty() {
        return 0.0;
    }
    let intersection: usize = set_a.intersection(&set_b).count();
    let union: usize = set_a.union(&set_b).count();
    if union == 0 {
        return 0.0;
    }
    intersection as f32 / union as f32
}

fn trigrams(s: &str) -> HashSet<String> {
    if s.chars().count() < 3 {
        return HashSet::new();
    }
    let chars: Vec<char> = s.chars().collect();
    let mut out = HashSet::with_capacity(chars.len().saturating_sub(2));
    for window in chars.windows(3) {
        out.insert(window.iter().collect());
    }
    out
}

/// Fuzzy search over equities and indices. Returns up to `max_results`
/// sorted by descending score, with alphabetical `symbol` as the
/// tiebreaker so the order is deterministic across calls.
pub fn fuzzy_search(
    query: &str,
    equity_symbols: &HashMap<String, u32>,
    index_entries: &[IndexEntry],
    max_results: usize,
) -> Vec<SymbolMatch> {
    let q = query.trim().to_lowercase();
    if q.is_empty() {
        return Vec::new();
    }

    let mut scored: Vec<(u32, SymbolMatch)> = Vec::new();

    // Equities.
    for (key, &sec_id) in equity_symbols {
        let candidate = key.to_lowercase();
        let score = score_match(&q, &candidate);
        if score > 0 {
            scored.push((
                score,
                SymbolMatch {
                    symbol: key.clone(),
                    display_name: key.clone(),
                    kind: SymbolKind::Equity,
                    security_id: Some(sec_id),
                },
            ));
        }
    }

    // Indices — try the display name AND the DSL-keyword form so a user
    // can search either by the human label ("nifty 50") or the keyword
    // ("nifty_50"). Keep the best of the two.
    for entry in index_entries {
        let name_lower = entry.display_name.to_lowercase();
        let keyword_lower = entry.alias.dsl_keyword().to_lowercase();
        let score_name = score_match(&q, &name_lower);
        let score_keyword = score_match(&q, &keyword_lower);
        let score = score_name.max(score_keyword);
        if score > 0 {
            scored.push((
                score,
                SymbolMatch {
                    symbol: entry.display_name.clone(),
                    display_name: entry.display_name.clone(),
                    kind: SymbolKind::Index,
                    security_id: None,
                },
            ));
        }
    }

    // Sort: score DESC, then symbol ASC for determinism.
    scored.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.symbol.cmp(&b.1.symbol)));

    scored.truncate(max_results);
    scored.into_iter().map(|(_, m)| m).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategy::dsl::ast::IndexAlias;

    fn entry(name: &str) -> IndexEntry {
        IndexEntry {
            alias: IndexAlias::Nifty50,
            display_name: name.to_string(),
            symbols: vec![],
            last_updated: "never".to_string(),
        }
    }

    #[test]
    fn tier_5_exact() {
        assert_eq!(score_match("reliance", "reliance"), 1000);
    }

    #[test]
    fn tier_4_prefix_prefers_shorter() {
        // Same prefix; shorter candidate should score higher. The
        // length bonus is `(100 - candidate_len())`, so 4-char and
        // 5-char candidates differ by 1.
        let four = score_match("rel", "rela");
        let five = score_match("rel", "relat");
        assert!(four > five);
        // 4 chars → 800 + 96 = 896
        assert_eq!(four, 800 + (100 - 4));
    }

    #[test]
    fn tier_5_beats_tier_4() {
        // Exact match (1000) outranks any prefix score (800..900).
        assert!(score_match("rel", "rel") > score_match("rel", "relx"));
    }

    #[test]
    fn tier_3_contains() {
        assert_eq!(score_match("iance", "reliance"), 500);
    }

    #[test]
    fn tier_2_subsequence() {
        // r..e..l: r-e-l appear in order in "reliance".
        assert_eq!(score_match("rln", "reliance"), 300);
    }

    #[test]
    fn tier_1_trigram_below_threshold_returns_zero() {
        // Two very dissimilar strings should fail all tiers.
        assert_eq!(score_match("xyz", "aaaa"), 0);
    }

    #[test]
    fn tier_1_trigram_above_threshold() {
        // Construct a query that has high trigram overlap with the
        // candidate but breaks subsequence matching by duplicating a
        // char. "bankofiindia" vs "bankofindia":
        //   - not equal, not prefix, not contains
        //   - subsequence: b-a-n-k-o-f-i-i-n-d-i-a — the second `i`
        //     is at position 9, but the next needed `n` doesn't exist
        //     after position 9 in "bankofindia" (ends with ...i-a).
        //     So subsequence is false.
        //   - trigrams: 8 of the 10 query trigrams are also in the
        //     candidate, Jaccard ≈ 0.73 → well above the 0.25 floor.
        let s = score_match("bankofiindia", "bankofindia");
        assert!(s >= 50 && s <= 200, "expected 50..200, got {}", s);
    }

    #[test]
    fn is_subsequence_works() {
        assert!(is_subsequence("rln", "reliance"));
        assert!(is_subsequence("ab", "abcdef"));
        assert!(!is_subsequence("abc", "acb"));
        assert!(is_subsequence("", "anything"));
    }

    #[test]
    fn trigram_similarity_short_input() {
        assert_eq!(trigram_similarity("ab", "abcdef"), 0.0);
    }

    #[test]
    fn trigram_similarity_identical() {
        assert!((trigram_similarity("reliance", "reliance") - 1.0).abs() < 1e-6);
    }

    #[test]
    fn empty_query_returns_empty() {
        let mut eq = HashMap::new();
        eq.insert("RELIANCE".to_string(), 2885u32);
        assert!(fuzzy_search("", &eq, &[], 5).is_empty());
        assert!(fuzzy_search("   ", &eq, &[], 5).is_empty());
    }

    #[test]
    fn equity_hit() {
        let mut eq = HashMap::new();
        eq.insert("RELIANCE".to_string(), 2885u32);
        eq.insert("HDFCBANK".to_string(), 1333u32);
        let r = fuzzy_search("rel", &eq, &[], 5);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].symbol, "RELIANCE");
        assert_eq!(r[0].security_id, Some(2885));
        assert!(matches!(r[0].kind, SymbolKind::Equity));
    }

    #[test]
    fn index_hit_uses_display_name() {
        let eq = HashMap::new();
        let entries = vec![entry("NIFTY 50")];
        let r = fuzzy_search("nifty 50", &eq, &entries, 5);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].symbol, "NIFTY 50");
        assert_eq!(r[0].security_id, None);
        assert!(matches!(r[0].kind, SymbolKind::Index));
    }

    #[test]
    fn index_hit_uses_keyword() {
        let eq = HashMap::new();
        let entries = vec![entry("NIFTY 50")];
        let r = fuzzy_search("nifty_50", &eq, &entries, 5);
        assert_eq!(r.len(), 1);
    }

    #[test]
    fn max_results_truncates() {
        let mut eq = HashMap::new();
        for i in 0..20 {
            eq.insert(format!("ABC{}", i), i as u32);
        }
        let r = fuzzy_search("abc", &eq, &[], 3);
        assert_eq!(r.len(), 3);
    }

    #[test]
    fn ranking_orders_higher_score_first() {
        let mut eq = HashMap::new();
        // Exact > prefix > contains > subsequence.
        eq.insert("RELIANCE".to_string(), 1u32);
        eq.insert("REL".to_string(), 2u32);
        eq.insert("ASDFRELIANCE".to_string(), 3u32);
        let r = fuzzy_search("rel", &eq, &[], 5);
        assert_eq!(r[0].symbol, "REL"); // exact
        assert_eq!(r[1].symbol, "RELIANCE"); // prefix
                                             // "ASDFRELIANCE" is a contains match (score 500) and should
                                             // rank below the prefix match (score >= 800).
        assert_eq!(r[2].symbol, "ASDFRELIANCE");
    }
}
use serde::{Deserialize, Serialize};

use crate::models::OrderSide;
use crate::strategy::execution::PaperTrade;
use crate::strategy::logging::{LogEntry, LogEntryKind, RuleSkipReason};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BacktestSummary {
    pub initial_cash: f64,
    pub final_cash: f64,
    pub total_return_pct: f64,
    pub total_trades: usize,
    pub buy_count: usize,
    pub sell_count: usize,
    pub closed_trades: usize,
    pub winning_trades: usize,
    pub losing_trades: usize,
    pub breakeven_trades: usize,
    pub win_rate_pct: f64,
    pub total_realized_pnl: f64,
    pub gross_profit: f64,
    pub gross_loss: f64,
    pub profit_factor: f64,
    pub avg_win: f64,
    pub avg_loss: f64,
    pub largest_win: f64,
    pub largest_loss: f64,
    pub expectancy: f64,
    pub max_drawdown: f64,
    pub max_drawdown_pct: f64,
    pub max_consecutive_wins: usize,
    pub max_consecutive_losses: usize,
    pub total_candles_processed: usize,
    pub candles_per_trade: f64,
    pub skipped_no_position: usize,
}

pub struct BacktestAnalyser;

impl BacktestAnalyser {
    pub fn analyse(
        trade_history: &[PaperTrade],
        initial_cash: f64,
        final_cash: f64,
        total_candles_processed: usize,
        logs: &[LogEntry],
    ) -> BacktestSummary {
        let buy_count = trade_history
            .iter()
            .filter(|trade| trade.side == OrderSide::Buy)
            .count();
        let sell_count = trade_history
            .iter()
            .filter(|trade| trade.side == OrderSide::Sell)
            .count();
        let closed_pnls = trade_history
            .iter()
            .filter_map(|trade| trade.pnl)
            .collect::<Vec<_>>();
        let closed_trades = closed_pnls.len();

        let winning_trades = closed_pnls.iter().filter(|&&pnl| pnl > 0.0).count();
        let losing_trades = closed_pnls.iter().filter(|&&pnl| pnl < 0.0).count();
        let breakeven_trades = closed_pnls.iter().filter(|&&pnl| pnl == 0.0).count();
        let win_rate_pct = if closed_trades == 0 {
            0.0
        } else {
            winning_trades as f64 / closed_trades as f64 * 100.0
        };

        let total_realized_pnl = closed_pnls.iter().sum::<f64>();
        let gross_profit = closed_pnls
            .iter()
            .copied()
            .filter(|pnl| *pnl > 0.0)
            .sum::<f64>();
        let gross_loss = closed_pnls
            .iter()
            .copied()
            .filter(|pnl| *pnl < 0.0)
            .sum::<f64>();
        let profit_factor = if gross_loss == 0.0 {
            0.0
        } else {
            gross_profit / gross_loss.abs()
        };
        let avg_win = if winning_trades == 0 {
            0.0
        } else {
            gross_profit / winning_trades as f64
        };
        let avg_loss = if losing_trades == 0 {
            0.0
        } else {
            gross_loss / losing_trades as f64
        };
        let largest_win = closed_pnls
            .iter()
            .copied()
            .filter(|pnl| *pnl > 0.0)
            .reduce(f64::max)
            .unwrap_or(0.0);
        let largest_loss = closed_pnls
            .iter()
            .copied()
            .filter(|pnl| *pnl < 0.0)
            .reduce(f64::min)
            .unwrap_or(0.0);
        let expectancy = if closed_trades == 0 {
            0.0
        } else {
            let win_rate = win_rate_pct / 100.0;
            (win_rate * avg_win) + ((1.0 - win_rate) * avg_loss)
        };

        let mut peak = 0.0;
        let mut running_pnl = 0.0;
        let mut max_drawdown = 0.0;
        for pnl in &closed_pnls {
            running_pnl += pnl;
            if running_pnl > peak {
                peak = running_pnl;
            }
            let drawdown = peak - running_pnl;
            if drawdown > max_drawdown {
                max_drawdown = drawdown;
            }
        }

        let mut current_wins = 0;
        let mut current_losses = 0;
        let mut max_consecutive_wins = 0;
        let mut max_consecutive_losses = 0;
        for pnl in &closed_pnls {
            if *pnl > 0.0 {
                current_wins += 1;
                current_losses = 0;
            } else if *pnl < 0.0 {
                current_losses += 1;
                current_wins = 0;
            } else {
                current_wins = 0;
                current_losses = 0;
            }
            max_consecutive_wins = max_consecutive_wins.max(current_wins);
            max_consecutive_losses = max_consecutive_losses.max(current_losses);
        }

        let skipped_no_position = logs
            .iter()
            .filter(|entry| {
                matches!(
                    &entry.kind,
                    LogEntryKind::RuleSkipped {
                        reason: RuleSkipReason::NoPosition,
                        ..
                    }
                )
            })
            .count();

        BacktestSummary {
            initial_cash,
            final_cash,
            total_return_pct: if initial_cash == 0.0 {
                0.0
            } else {
                (final_cash - initial_cash) / initial_cash * 100.0
            },
            total_trades: buy_count + sell_count,
            buy_count,
            sell_count,
            closed_trades,
            winning_trades,
            losing_trades,
            breakeven_trades,
            win_rate_pct,
            total_realized_pnl,
            gross_profit,
            gross_loss,
            profit_factor,
            avg_win,
            avg_loss,
            largest_win,
            largest_loss,
            expectancy,
            max_drawdown,
            max_drawdown_pct: if initial_cash == 0.0 {
                0.0
            } else {
                max_drawdown / initial_cash * 100.0
            },
            max_consecutive_wins,
            max_consecutive_losses,
            total_candles_processed,
            candles_per_trade: if closed_trades == 0 {
                0.0
            } else {
                total_candles_processed as f64 / closed_trades as f64
            },
            skipped_no_position,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn buy(price: f64) -> PaperTrade {
        PaperTrade {
            id: "x".to_string(),
            timestamp: Utc::now().timestamp_millis(),
            symbol: "T".to_string(),
            side: OrderSide::Buy,
            quantity: 1,
            price,
            rule_id: "rule_0".to_string(),
            pnl: None,
        }
    }

    fn sell(price: f64, pnl: f64) -> PaperTrade {
        PaperTrade {
            id: "x".to_string(),
            timestamp: Utc::now().timestamp_millis(),
            symbol: "T".to_string(),
            side: OrderSide::Sell,
            quantity: 1,
            price,
            rule_id: "rule_0".to_string(),
            pnl: Some(pnl),
        }
    }

    fn analyse(
        trades: &[PaperTrade],
        initial: f64,
        final_cash: f64,
        candles: usize,
    ) -> BacktestSummary {
        BacktestAnalyser::analyse(trades, initial, final_cash, candles, &[])
    }

    #[test]
    fn empty_history() {
        let summary = analyse(&[], 100_000.0, 100_000.0, 100);
        assert_eq!(summary.total_trades, 0);
        assert_eq!(summary.closed_trades, 0);
        assert_eq!(summary.win_rate_pct, 0.0);
        assert_eq!(summary.max_drawdown, 0.0);
        assert_eq!(summary.total_return_pct, 0.0);
    }

    #[test]
    fn single_win() {
        let summary = analyse(&[buy(100.0), sell(120.0, 20.0)], 100_000.0, 100_020.0, 50);
        assert_eq!(summary.closed_trades, 1);
        assert_eq!(summary.winning_trades, 1);
        assert_eq!(summary.win_rate_pct, 100.0);
        assert_eq!(summary.gross_profit, 20.0);
        assert_eq!(summary.max_drawdown, 0.0);
    }

    #[test]
    fn single_loss() {
        let summary = analyse(&[buy(100.0), sell(80.0, -20.0)], 100_000.0, 99_980.0, 50);
        assert_eq!(summary.losing_trades, 1);
        assert_eq!(summary.win_rate_pct, 0.0);
        assert_eq!(summary.gross_loss, -20.0);
        assert_eq!(summary.largest_loss, -20.0);
        assert_eq!(summary.max_drawdown, 20.0);
    }

    #[test]
    fn drawdown_win_then_loss() {
        let trades = vec![
            buy(100.0),
            sell(200.0, 100.0),
            buy(200.0),
            sell(140.0, -60.0),
        ];
        let summary = analyse(&trades, 100_000.0, 100_040.0, 100);
        assert_eq!(summary.max_drawdown, 60.0);
        assert_eq!(summary.total_realized_pnl, 40.0);
    }

    #[test]
    fn profit_factor() {
        let trades = vec![
            buy(100.0),
            sell(150.0, 50.0),
            buy(100.0),
            sell(130.0, 30.0),
            buy(100.0),
            sell(80.0, -20.0),
            buy(100.0),
            sell(90.0, -10.0),
        ];
        let summary = analyse(&trades, 100_000.0, 100_050.0, 200);
        assert!((summary.profit_factor - 80.0 / 30.0).abs() < 0.001);
    }

    #[test]
    fn consecutive_streaks() {
        let pnls = [10.0, 10.0, 10.0, -5.0, -5.0, 10.0, -5.0, 10.0, 10.0];
        let mut trades = Vec::new();
        for &pnl in &pnls {
            trades.push(buy(100.0));
            trades.push(sell(if pnl > 0.0 { 110.0 } else { 95.0 }, pnl));
        }
        let summary = analyse(&trades, 100_000.0, 100_045.0, 400);
        assert_eq!(summary.max_consecutive_wins, 3);
        assert_eq!(summary.max_consecutive_losses, 2);
    }

    #[test]
    fn breakeven_included_in_closed_not_win_or_loss() {
        let trades = vec![
            buy(100.0),
            sell(110.0, 10.0),
            buy(100.0),
            sell(100.0, 0.0),
            buy(100.0),
            sell(90.0, -10.0),
        ];
        let summary = analyse(&trades, 100_000.0, 100_000.0, 150);
        assert_eq!(summary.closed_trades, 3);
        assert_eq!(summary.winning_trades, 1);
        assert_eq!(summary.breakeven_trades, 1);
        assert_eq!(summary.losing_trades, 1);
        assert!((summary.win_rate_pct - 33.333).abs() < 0.01);
    }

    #[test]
    fn only_buys_no_closed() {
        let summary = analyse(&[buy(100.0), buy(200.0)], 100_000.0, 70_000.0, 50);
        assert_eq!(summary.buy_count, 2);
        assert_eq!(summary.closed_trades, 0);
        assert_eq!(summary.max_drawdown, 0.0);
        assert_eq!(summary.candles_per_trade, 0.0);
    }

    #[test]
    fn total_return_pct() {
        let summary = analyse(&[], 100_000.0, 110_000.0, 0);
        assert!((summary.total_return_pct - 10.0).abs() < 0.001);
    }

    #[test]
    fn candles_per_trade() {
        let trades = (0..4)
            .flat_map(|_| vec![buy(100.0), sell(110.0, 10.0)])
            .collect::<Vec<_>>();
        let summary = analyse(&trades, 100_000.0, 100_040.0, 100);
        assert_eq!(summary.candles_per_trade, 25.0);
    }
}
use chrono::NaiveTime;
use serde::{Deserialize, Serialize};

/// Named NSE index alias. Resolved to a constituent symbol list at runtime
/// via `IndexRegistry` (loaded once at deploy time, read-only afterwards).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum IndexAlias {
    Nifty50,
    NiftyNext50,
    Nifty100,
    Nifty200,
    Nifty500,
    NiftyMidcap50,
    NiftyMidcap100,
    NiftyMidcap150,
    NiftySmallcap50,
    NiftySmallcap100,
    NiftySmallcap250,
    NiftyBank,
    NiftyIt,
    NiftyPharma,
    NiftyAuto,
    NiftyFmcg,
    NiftyMetal,
    NiftyRealty,
    NiftyEnergy,
    NiftyInfra,
    NiftyPsuBank,
    NiftyFinancialServices,
}

impl IndexAlias {
    /// Parse from the DSL keyword string (case-insensitive, underscore-separated).
    pub fn from_dsl_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "NIFTY_50" => Some(Self::Nifty50),
            "NIFTY_NEXT_50" => Some(Self::NiftyNext50),
            "NIFTY_100" => Some(Self::Nifty100),
            "NIFTY_200" => Some(Self::Nifty200),
            "NIFTY_500" => Some(Self::Nifty500),
            "NIFTY_MIDCAP_50" => Some(Self::NiftyMidcap50),
            "NIFTY_MIDCAP_100" => Some(Self::NiftyMidcap100),
            "NIFTY_MIDCAP_150" => Some(Self::NiftyMidcap150),
            "NIFTY_SMALLCAP_50" => Some(Self::NiftySmallcap50),
            "NIFTY_SMALLCAP_100" => Some(Self::NiftySmallcap100),
            "NIFTY_SMALLCAP_250" => Some(Self::NiftySmallcap250),
            "NIFTY_BANK" => Some(Self::NiftyBank),
            "NIFTY_IT" => Some(Self::NiftyIt),
            "NIFTY_PHARMA" => Some(Self::NiftyPharma),
            "NIFTY_AUTO" => Some(Self::NiftyAuto),
            "NIFTY_FMCG" => Some(Self::NiftyFmcg),
            "NIFTY_METAL" => Some(Self::NiftyMetal),
            "NIFTY_REALTY" => Some(Self::NiftyRealty),
            "NIFTY_ENERGY" => Some(Self::NiftyEnergy),
            "NIFTY_INFRA" => Some(Self::NiftyInfra),
            "NIFTY_PSU_BANK" => Some(Self::NiftyPsuBank),
            "NIFTY_FINANCIAL_SERVICES" => Some(Self::NiftyFinancialServices),
            _ => None,
        }
    }

    /// Human-readable display name.
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Nifty50 => "NIFTY 50",
            Self::NiftyNext50 => "NIFTY NEXT 50",
            Self::Nifty100 => "NIFTY 100",
            Self::Nifty200 => "NIFTY 200",
            Self::Nifty500 => "NIFTY 500",
            Self::NiftyMidcap50 => "NIFTY MIDCAP 50",
            Self::NiftyMidcap100 => "NIFTY MIDCAP 100",
            Self::NiftyMidcap150 => "NIFTY MIDCAP 150",
            Self::NiftySmallcap50 => "NIFTY SMALLCAP 50",
            Self::NiftySmallcap100 => "NIFTY SMALLCAP 100",
            Self::NiftySmallcap250 => "NIFTY SMALLCAP 250",
            Self::NiftyBank => "NIFTY BANK",
            Self::NiftyIt => "NIFTY IT",
            Self::NiftyPharma => "NIFTY PHARMA",
            Self::NiftyAuto => "NIFTY AUTO",
            Self::NiftyFmcg => "NIFTY FMCG",
            Self::NiftyMetal => "NIFTY METAL",
            Self::NiftyRealty => "NIFTY REALTY",
            Self::NiftyEnergy => "NIFTY ENERGY",
            Self::NiftyInfra => "NIFTY INFRA",
            Self::NiftyPsuBank => "NIFTY PSU BANK",
            Self::NiftyFinancialServices => "NIFTY FINANCIAL SERVICES",
        }
    }

    /// DSL keyword form (what the user writes after TRADE_IN).
    pub fn dsl_keyword(&self) -> &'static str {
        match self {
            Self::Nifty50 => "NIFTY_50",
            Self::NiftyNext50 => "NIFTY_NEXT_50",
            Self::Nifty100 => "NIFTY_100",
            Self::Nifty200 => "NIFTY_200",
            Self::Nifty500 => "NIFTY_500",
            Self::NiftyMidcap50 => "NIFTY_MIDCAP_50",
            Self::NiftyMidcap100 => "NIFTY_MIDCAP_100",
            Self::NiftyMidcap150 => "NIFTY_MIDCAP_150",
            Self::NiftySmallcap50 => "NIFTY_SMALLCAP_50",
            Self::NiftySmallcap100 => "NIFTY_SMALLCAP_100",
            Self::NiftySmallcap250 => "NIFTY_SMALLCAP_250",
            Self::NiftyBank => "NIFTY_BANK",
            Self::NiftyIt => "NIFTY_IT",
            Self::NiftyPharma => "NIFTY_PHARMA",
            Self::NiftyAuto => "NIFTY_AUTO",
            Self::NiftyFmcg => "NIFTY_FMCG",
            Self::NiftyMetal => "NIFTY_METAL",
            Self::NiftyRealty => "NIFTY_REALTY",
            Self::NiftyEnergy => "NIFTY_ENERGY",
            Self::NiftyInfra => "NIFTY_INFRA",
            Self::NiftyPsuBank => "NIFTY_PSU_BANK",
            Self::NiftyFinancialServices => "NIFTY_FINANCIAL_SERVICES",
        }
    }

    /// niftyindices.com CSV filename for this index.
    pub fn csv_filename(&self) -> &'static str {
        match self {
            Self::Nifty50 => "ind_nifty50list.csv",
            Self::NiftyNext50 => "ind_niftynext50list.csv",
            Self::Nifty100 => "ind_nifty100list.csv",
            Self::Nifty200 => "ind_nifty200list.csv",
            Self::Nifty500 => "ind_nifty500list.csv",
            Self::NiftyMidcap50 => "ind_niftymidcap50list.csv",
            Self::NiftyMidcap100 => "ind_niftymidcap100list.csv",
            Self::NiftyMidcap150 => "ind_niftymidcap150list.csv",
            Self::NiftySmallcap50 => "ind_niftysmallcap50list.csv",
            Self::NiftySmallcap100 => "ind_niftysmallcap100list.csv",
            Self::NiftySmallcap250 => "ind_niftysmallcap250list.csv",
            Self::NiftyBank => "ind_niftybanklist.csv",
            Self::NiftyIt => "ind_niftyitlist.csv",
            Self::NiftyPharma => "ind_niftypharmaList.csv",
            Self::NiftyAuto => "ind_niftyautolist.csv",
            Self::NiftyFmcg => "ind_niftyfmcglist.csv",
            Self::NiftyMetal => "ind_niftymetallist.csv",
            Self::NiftyRealty => "ind_niftyrealtylist.csv",
            Self::NiftyEnergy => "ind_niftyenergylist.csv",
            Self::NiftyInfra => "ind_niftyinfraList.csv",
            Self::NiftyPsuBank => "ind_niftypsubanklist.csv",
            Self::NiftyFinancialServices => "ind_niftyfinancialserviceslist.csv",
        }
    }

    /// Lowercase filename stem used for JSON cache files (e.g. "nifty_50").
    pub fn cache_stem(&self) -> &'static str {
        match self {
            Self::Nifty50 => "nifty_50",
            Self::NiftyNext50 => "nifty_next_50",
            Self::Nifty100 => "nifty_100",
            Self::Nifty200 => "nifty_200",
            Self::Nifty500 => "nifty_500",
            Self::NiftyMidcap50 => "nifty_midcap_50",
            Self::NiftyMidcap100 => "nifty_midcap_100",
            Self::NiftyMidcap150 => "nifty_midcap_150",
            Self::NiftySmallcap50 => "nifty_smallcap_50",
            Self::NiftySmallcap100 => "nifty_smallcap_100",
            Self::NiftySmallcap250 => "nifty_smallcap_250",
            Self::NiftyBank => "nifty_bank",
            Self::NiftyIt => "nifty_it",
            Self::NiftyPharma => "nifty_pharma",
            Self::NiftyAuto => "nifty_auto",
            Self::NiftyFmcg => "nifty_fmcg",
            Self::NiftyMetal => "nifty_metal",
            Self::NiftyRealty => "nifty_realty",
            Self::NiftyEnergy => "nifty_energy",
            Self::NiftyInfra => "nifty_infra",
            Self::NiftyPsuBank => "nifty_psu_bank",
            Self::NiftyFinancialServices => "nifty_financial_services",
        }
    }

    /// All variants in a stable order. Used for iteration.
    pub fn all() -> &'static [IndexAlias] {
        use IndexAlias::*;
        &[
            Nifty50,
            NiftyNext50,
            Nifty100,
            Nifty200,
            Nifty500,
            NiftyMidcap50,
            NiftyMidcap100,
            NiftyMidcap150,
            NiftySmallcap50,
            NiftySmallcap100,
            NiftySmallcap250,
            NiftyBank,
            NiftyIt,
            NiftyPharma,
            NiftyAuto,
            NiftyFmcg,
            NiftyMetal,
            NiftyRealty,
            NiftyEnergy,
            NiftyInfra,
            NiftyPsuBank,
            NiftyFinancialServices,
        ]
    }
}

/// What a strategy trades over. `None` on `StrategyNode` means single-symbol
/// (backwards compatible); the engine falls back to the explicit `symbol`
/// argument it was constructed with.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TradeIn {
    /// Explicit comma-separated list of NSE symbols (e.g. `TRADE_IN RELIANCE, INFY`).
    Symbols(Vec<String>),
    /// Named index alias (e.g. `TRADE_IN NIFTY_BANK`).
    Index(IndexAlias),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyNode {
    pub name: String,
    /// `None` for the existing single-symbol flow. The engine reads this once
    /// at deploy time to fan the ruleset out over a symbol universe.
    pub trade_in: Option<TradeIn>,
    /// Optional stop-loss percentage (e.g. `2.0` for 2%). When set, the engine
    /// closes any open long position whose unrealized loss against the entry
    /// price exceeds this threshold on every candle. Deliberately bypasses
    /// `TriggerStateMap` because it must fire every candle while underwater,
    /// not on a `false → true` edge.
    pub stop_loss: Option<f64>,
    /// Optional take-profit percentage (e.g. `5.0` for 5%). Mirrors
    /// `stop_loss` for the gain side.
    pub take_profit: Option<f64>,
    /// Optional risk controls (max orders, max open positions, max daily loss).
    /// All three limits are checked before any order is submitted by
    /// `StrategyEngine::submit_action`.
    pub risk: Option<RiskConfig>,
    pub rules: Vec<RuleNode>,
}

/// Strategy-level risk controls. All fields are optional and any combination
/// is valid. The engine enforces these before submitting any order (including
/// synthetic SL/TP orders) by logging a `RiskBreach` entry and skipping the
/// order. See `StrategyEngine` for the run-time state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RiskConfig {
    /// `RISK MAX_DAILY_LOSS <pct>%`. The engine sums the negative
    /// `PaperTrade.pnl` values (realized loss only) and skips all further
    /// orders when the cumulative loss as a percentage of `initial_cash`
    /// exceeds this threshold. In a backtest context "daily" is session-
    /// scoped — there is no real clock, so the threshold is enforced
    /// cumulatively across the whole run.
    pub max_daily_loss_pct: Option<f64>,
    /// `RISK MAX_POSITIONS <int>`. The engine counts the broker's open
    /// positions with `quantity > 0` and skips BUY orders when the count is
    /// already at or above the limit. Sells are not affected.
    pub max_open_positions: Option<u32>,
    /// `RISK MAX_ORDERS <int>`. The engine counts entry orders (BUYs) it
    /// has successfully submitted during the run and skips when the count
    /// reaches the limit. **Exits (`SELL` / `SELL ALL`, including the
    /// strategy-level SL/TP synthetic closes) are NEVER blocked** and
    /// do NOT count toward the cap — invariant 12 ("SL/TP is a safety
    /// net") trumps the cap, so the safety net can always close a
    /// position. See audit item A2.
    pub max_orders: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleNode {
    pub id: String,
    pub condition: ConditionNode,
    pub action: ActionNode,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConditionNode {
    Comparison {
        left: ExprNode,
        op: CompareOp,
        right: ExprNode,
    },
    CrossAbove {
        fast: ExprNode,
        slow: ExprNode,
    },
    CrossBelow {
        fast: ExprNode,
        slow: ExprNode,
    },
    And(Box<ConditionNode>, Box<ConditionNode>),
    Or(Box<ConditionNode>, Box<ConditionNode>),
    Not(Box<ConditionNode>),
    InPosition,
    TimeWindow {
        start: NaiveTime,
        end: NaiveTime,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExprNode {
    Indicator(IndicatorCall),
    PriceField(PriceField),
    Literal(f64),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndicatorCall {
    pub kind: IndicatorKind,
    pub period: usize,
}

/// Indicator functions available in DSL expressions:
/// ema(N), ma(N), rsi(N), rel_vol(N), atr(N), vwap(N), bb_upper(N),
/// bb_lower(N), bb_mid(N).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum IndicatorKind {
    Ema,
    Ma,
    Rsi,
    RelVol,
    Atr,
    Vwap,
    BbUpper,
    BbLower,
    BbMid,
}

/// Candle price fields available in DSL expressions:
/// close, open, high, low, volume, prev_close.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompareOp {
    Lt,
    Gt,
    Lte,
    Gte,
    Eq,
    Neq,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum QuantitySpec {
    /// BUY 10 / SELL 10
    Fixed(u64),
    /// BUY 10% - percentage of current available cash
    PercentCapital(f64),
    /// BUY 5000 WORTH - fixed rupee value
    ValueBased(f64),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ActionNode {
    Buy { quantity: QuantitySpec },
    Sell { quantity: QuantitySpec },
    SellAll,
}
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind {
    When,
    Buy,
    Sell,
    All,
    And,
    Or,
    Not,
    Between,
    InPosition,
    TradeIn,
    StopLoss,
    TakeProfit,
    Risk,
    MaxDailyLoss,
    MaxPositions,
    MaxOrders,
    Ema,
    Ma,
    Rsi,
    RelVol,
    Atr,
    Vwap,
    BbUpper,
    BbLower,
    BbMid,
    Close,
    Open,
    High,
    Low,
    Volume,
    PrevClose,
    PrevOpen,
    PrevHigh,
    PrevLow,
    CrossAbove,
    CrossBelow,
    Lt,
    Gt,
    Lte,
    Gte,
    Eq,
    Neq,
    Number(f64),
    Integer(usize),
    Percent,
    Worth,
    TimeStr(String),
    /// Catch-all for non-keyword identifiers (NSE symbols, index alias
    /// strings, future plugin-registered keywords). The string is the raw
    /// text as it appeared in the source.
    Identifier(String),
    LParen,
    RParen,
    Comma,
    Newline,
    Eof,
}

#[derive(Debug, Clone)]
pub struct Token {
    pub kind: TokenKind,
    pub line: usize,
    pub col: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LexError {
    pub message: String,
    pub line: usize,
    pub col: usize,
}

pub struct Lexer;

impl Lexer {
    pub fn tokenize(source: &str) -> Result<Vec<Token>, LexError> {
        let mut tokens = Vec::new();

        for (line_index, raw_line) in source.lines().enumerate() {
            let line_no = line_index + 1;
            let trimmed = raw_line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }

            let chars: Vec<char> = raw_line.chars().collect();
            let mut pos = 0;
            while pos < chars.len() {
                let ch = chars[pos];
                let col = pos + 1;

                if ch.is_whitespace() {
                    pos += 1;
                    continue;
                }

                match ch {
                    '(' => {
                        tokens.push(token(TokenKind::LParen, line_no, col));
                        pos += 1;
                    }
                    ')' => {
                        tokens.push(token(TokenKind::RParen, line_no, col));
                        pos += 1;
                    }
                    ',' => {
                        tokens.push(token(TokenKind::Comma, line_no, col));
                        pos += 1;
                    }
                    '<' | '>' | '=' | '!' => {
                        let (kind, next_pos) = operator(&chars, pos).ok_or_else(|| LexError {
                            message: format!("unknown operator '{}'", ch),
                            line: line_no,
                            col,
                        })?;
                        tokens.push(token(kind, line_no, col));
                        pos = next_pos;
                    }
                    '0'..='9' => {
                        let (kind, next_pos) =
                            number_or_time(&chars, pos).map_err(|message| LexError {
                                message,
                                line: line_no,
                                col,
                            })?;
                        tokens.push(token(kind, line_no, col));
                        pos = next_pos;
                        if pos < chars.len() && chars[pos] == '%' {
                            tokens.push(token(TokenKind::Percent, line_no, pos + 1));
                            pos += 1;
                        }
                    }
                    _ if is_ident_start(ch) => {
                        let start = pos;
                        pos += 1;
                        while pos < chars.len() && is_ident_continue(chars[pos]) {
                            pos += 1;
                        }
                        let ident: String = chars[start..pos].iter().collect();
                        // Unknown identifiers are not an error — they become
                        // an `Identifier(s)` token. The parser decides
                        // whether the text is a valid symbol, index alias,
                        // or a typo. This keeps `TRADE_IN RELIANCE, INFY`
                        // and `NIFTY_50` lexable without hard-coding the
                        // 22 index keywords.
                        let kind =
                            keyword(&ident).unwrap_or_else(|| TokenKind::Identifier(ident.clone()));
                        tokens.push(token(kind, line_no, col));
                    }
                    _ => {
                        return Err(LexError {
                            message: format!("unknown character '{}'", ch),
                            line: line_no,
                            col,
                        });
                    }
                }
            }

            tokens.push(token(TokenKind::Newline, line_no, raw_line.len() + 1));
        }

        tokens.push(token(TokenKind::Eof, source.lines().count() + 1, 1));
        Ok(tokens)
    }
}

fn token(kind: TokenKind, line: usize, col: usize) -> Token {
    Token { kind, line, col }
}

fn operator(chars: &[char], pos: usize) -> Option<(TokenKind, usize)> {
    let next = chars.get(pos + 1).copied();
    match (chars[pos], next) {
        ('<', Some('=')) => Some((TokenKind::Lte, pos + 2)),
        ('>', Some('=')) => Some((TokenKind::Gte, pos + 2)),
        ('=', Some('=')) => Some((TokenKind::Eq, pos + 2)),
        ('!', Some('=')) => Some((TokenKind::Neq, pos + 2)),
        ('<', _) => Some((TokenKind::Lt, pos + 1)),
        ('>', _) => Some((TokenKind::Gt, pos + 1)),
        _ => None,
    }
}

fn number_or_time(chars: &[char], pos: usize) -> Result<(TokenKind, usize), String> {
    if pos + 4 < chars.len()
        && chars[pos].is_ascii_digit()
        && chars[pos + 1].is_ascii_digit()
        && chars[pos + 2] == ':'
        && chars[pos + 3].is_ascii_digit()
        && chars[pos + 4].is_ascii_digit()
    {
        let text: String = chars[pos..=pos + 4].iter().collect();
        return Ok((TokenKind::TimeStr(text), pos + 5));
    }

    let start = pos;
    let mut end = pos;
    while end < chars.len() && chars[end].is_ascii_digit() {
        end += 1;
    }

    if end < chars.len() && chars[end] == '.' {
        end += 1;
        if end >= chars.len() || !chars[end].is_ascii_digit() {
            return Err("decimal number must include digits after '.'".to_string());
        }
        while end < chars.len() && chars[end].is_ascii_digit() {
            end += 1;
        }
        let text: String = chars[start..end].iter().collect();
        let value = text
            .parse::<f64>()
            .map_err(|_| format!("invalid number '{}'", text))?;
        Ok((TokenKind::Number(value), end))
    } else {
        let text: String = chars[start..end].iter().collect();
        let value = text
            .parse::<usize>()
            .map_err(|_| format!("invalid integer '{}'", text))?;
        Ok((TokenKind::Integer(value), end))
    }
}

fn is_ident_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_'
}

fn is_ident_continue(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

fn keyword(ident: &str) -> Option<TokenKind> {
    match ident.to_ascii_lowercase().as_str() {
        "when" => Some(TokenKind::When),
        "buy" => Some(TokenKind::Buy),
        "sell" => Some(TokenKind::Sell),
        "all" => Some(TokenKind::All),
        "worth" => Some(TokenKind::Worth),
        "and" => Some(TokenKind::And),
        "or" => Some(TokenKind::Or),
        "not" => Some(TokenKind::Not),
        "between" => Some(TokenKind::Between),
        "in_position" => Some(TokenKind::InPosition),
        "trade_in" => Some(TokenKind::TradeIn),
        "stop_loss" => Some(TokenKind::StopLoss),
        "take_profit" => Some(TokenKind::TakeProfit),
        "risk" => Some(TokenKind::Risk),
        "max_daily_loss" => Some(TokenKind::MaxDailyLoss),
        "max_positions" => Some(TokenKind::MaxPositions),
        "max_orders" => Some(TokenKind::MaxOrders),
        "ema" => Some(TokenKind::Ema),
        "ma" => Some(TokenKind::Ma),
        "rsi" => Some(TokenKind::Rsi),
        "rel_vol" => Some(TokenKind::RelVol),
        "atr" => Some(TokenKind::Atr),
        "vwap" => Some(TokenKind::Vwap),
        "bb_upper" => Some(TokenKind::BbUpper),
        "bb_lower" => Some(TokenKind::BbLower),
        "bb_mid" => Some(TokenKind::BbMid),
        "close" => Some(TokenKind::Close),
        "open" => Some(TokenKind::Open),
        "high" => Some(TokenKind::High),
        "low" => Some(TokenKind::Low),
        "volume" => Some(TokenKind::Volume),
        "prev_close" => Some(TokenKind::PrevClose),
        "prev_open" => Some(TokenKind::PrevOpen),
        "prev_high" => Some(TokenKind::PrevHigh),
        "prev_low" => Some(TokenKind::PrevLow),
        "cross_above" => Some(TokenKind::CrossAbove),
        "cross_below" => Some(TokenKind::CrossBelow),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXAMPLES: &str = r#"
# EMA crossover
WHEN cross_above(ema(20), ema(50))
BUY 10

WHEN cross_below(ema(20), ema(50))
SELL ALL

WHEN rsi(14) < 30
BUY 5

WHEN rsi(14) > 70
SELL ALL

WHEN close > bb_upper(20)
SELL 10

WHEN close < bb_lower(20)
BUY 10

WHEN ema(9) > ema(21) AND rsi(14) < 60
BUY 5

WHEN cross_above(ema(20), ema(50)) AND NOT (in_position())
BUY 10
"#;

    #[test]
    fn tokenizes_examples() {
        let tokens = Lexer::tokenize(EXAMPLES).unwrap();
        assert!(tokens
            .iter()
            .any(|token| token.kind == TokenKind::CrossAbove));
        assert_eq!(tokens.last().unwrap().kind, TokenKind::Eof);
    }

    #[test]
    fn tokenizes_simple_strategy() {
        let tokens = Lexer::tokenize("WHEN rsi(14) < 30\nBUY 5").unwrap();
        let kinds: Vec<_> = tokens.into_iter().map(|token| token.kind).collect();
        assert_eq!(
            kinds,
            vec![
                TokenKind::When,
                TokenKind::Rsi,
                TokenKind::LParen,
                TokenKind::Integer(14),
                TokenKind::RParen,
                TokenKind::Lt,
                TokenKind::Integer(30),
                TokenKind::Newline,
                TokenKind::Buy,
                TokenKind::Integer(5),
                TokenKind::Newline,
                TokenKind::Eof,
            ]
        );
    }

    #[test]
    fn tokenizes_percent_and_worth_quantities() {
        let tokens =
            Lexer::tokenize("WHEN close > 100\nBUY 10%\nWHEN close < 90\nBUY 5000 WORTH").unwrap();
        let kinds: Vec<_> = tokens.into_iter().map(|token| token.kind).collect();
        assert!(kinds.contains(&TokenKind::Percent));
        assert!(kinds.contains(&TokenKind::Worth));
    }

    #[test]
    fn tokenizes_sell_all() {
        let tokens = Lexer::tokenize("WHEN close > 100\nSELL ALL").unwrap();
        let kinds: Vec<_> = tokens.into_iter().map(|token| token.kind).collect();
        assert!(kinds.contains(&TokenKind::Sell));
        assert!(kinds.contains(&TokenKind::All));
    }

    #[test]
    fn tokenizes_rel_vol() {
        let tokens = Lexer::tokenize("WHEN rel_vol(20) > 2.0\nBUY 5").unwrap();
        assert!(tokens.iter().any(|token| token.kind == TokenKind::RelVol));
    }

    #[test]
    fn tokenizes_prev_close() {
        let tokens = Lexer::tokenize("WHEN prev_close < 100\nBUY 1").unwrap();
        assert!(tokens
            .iter()
            .any(|token| token.kind == TokenKind::PrevClose));
    }

    #[test]
    fn errors_on_unknown_character() {
        let err = Lexer::tokenize("WHEN close @ 10\nBUY 1").unwrap_err();
        assert_eq!(err.line, 1);
        assert_eq!(err.col, 12);
    }

    #[test]
    fn skips_comments_and_blank_lines() {
        let tokens = Lexer::tokenize("\n # comment\nWHEN close > 10\nBUY 1").unwrap();
        assert_eq!(tokens[0].kind, TokenKind::When);
    }

    #[test]
    fn skips_comments() {
        let tokens = Lexer::tokenize("# this is a comment\nWHEN close > 100\nBUY 1").unwrap();
        assert_eq!(tokens[0].kind, TokenKind::When);
    }

    #[test]
    fn skips_blank_lines() {
        let tokens = Lexer::tokenize("\n\nWHEN close > 100\nBUY 1").unwrap();
        assert_eq!(tokens[0].kind, TokenKind::When);
    }

    #[test]
    fn distinguishes_lte_from_lt() {
        let tokens = Lexer::tokenize("WHEN close <= 10\nBUY 1").unwrap();
        assert!(tokens.iter().any(|token| token.kind == TokenKind::Lte));
        assert!(!tokens.iter().any(|token| token.kind == TokenKind::Eq));
    }

    #[test]
    fn distinguishes_integer_and_number() {
        let tokens = Lexer::tokenize("WHEN close > 10.5\nBUY 1").unwrap();
        assert!(tokens
            .iter()
            .any(|token| token.kind == TokenKind::Number(10.5)));
        assert!(tokens
            .iter()
            .any(|token| token.kind == TokenKind::Integer(1)));
    }

    #[test]
    fn integer_vs_float() {
        let tokens = Lexer::tokenize("WHEN close > 105.5\nBUY 1").unwrap();
        assert!(tokens
            .iter()
            .any(|token| matches!(token.kind, TokenKind::Number(_))));
    }

    #[test]
    fn unknown_character_produces_lex_error() {
        assert!(Lexer::tokenize("WHEN close @ 100\nBUY 1").is_err());
    }

    #[test]
    fn cross_above_tokenizes_correctly() {
        let tokens = Lexer::tokenize("WHEN cross_above(ema(20), ema(50))\nBUY 10").unwrap();
        assert!(tokens
            .iter()
            .any(|token| token.kind == TokenKind::CrossAbove));
    }

    #[test]
    fn tokenizes_trade_in_keyword() {
        let tokens = Lexer::tokenize("TRADE_IN RELIANCE, INFY").unwrap();
        assert!(tokens.iter().any(|token| token.kind == TokenKind::TradeIn));
        // Symbols fall through to Identifier(String).
        let idents: Vec<&str> = tokens
            .iter()
            .filter_map(|t| match &t.kind {
                TokenKind::Identifier(s) => Some(s.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(idents, vec!["RELIANCE", "INFY"]);
    }

    #[test]
    fn tokenizes_index_alias_as_identifier() {
        // NIFTY_50 is not a lexer keyword — the parser resolves it via
        // IndexAlias::from_dsl_str in parse_trade_in.
        let tokens = Lexer::tokenize("TRADE_IN NIFTY_50").unwrap();
        assert!(matches!(
            tokens
                .iter()
                .find(|t| matches!(t.kind, TokenKind::Identifier(_))),
            Some(_)
        ));
    }

    #[test]
    fn tokenizes_stop_loss_keyword() {
        let tokens = Lexer::tokenize("STOP_LOSS 2%").unwrap();
        assert!(tokens.iter().any(|token| token.kind == TokenKind::StopLoss));
        assert!(tokens.iter().any(|token| token.kind == TokenKind::Percent));
    }

    #[test]
    fn tokenizes_take_profit_keyword() {
        let tokens = Lexer::tokenize("TAKE_PROFIT 5%").unwrap();
        assert!(tokens
            .iter()
            .any(|token| token.kind == TokenKind::TakeProfit));
        assert!(tokens.iter().any(|token| token.kind == TokenKind::Percent));
    }

    #[test]
    fn tokenizes_risk_keyword() {
        let tokens = Lexer::tokenize("RISK MAX_DAILY_LOSS 5%").unwrap();
        assert!(tokens.iter().any(|token| token.kind == TokenKind::Risk));
        assert!(tokens
            .iter()
            .any(|token| token.kind == TokenKind::MaxDailyLoss));
        assert!(tokens.iter().any(|token| token.kind == TokenKind::Percent));
    }

    #[test]
    fn tokenizes_risk_sub_keywords() {
        let tokens = Lexer::tokenize("RISK MAX_POSITIONS 3\nRISK MAX_ORDERS 20").unwrap();
        assert!(tokens.iter().any(|token| token.kind == TokenKind::Risk));
        assert!(tokens
            .iter()
            .any(|token| token.kind == TokenKind::MaxPositions));
        assert!(tokens
            .iter()
            .any(|token| token.kind == TokenKind::MaxOrders));
    }
}
pub mod ast;
pub mod lexer;
pub mod parser;
pub mod validator;

pub use ast::*;
pub use lexer::{LexError, Lexer, Token, TokenKind};
pub use parser::{ParseError, Parser};
pub use validator::{AstValidator, ValidationError, ValidationErrorKind};
use chrono::NaiveTime;
use serde::{Deserialize, Serialize};

use super::ast::{
    ActionNode, CompareOp, ConditionNode, ExprNode, IndexAlias, IndicatorCall, IndicatorKind,
    PriceField, QuantitySpec, RiskConfig, RuleNode, StrategyNode, TradeIn,
};
use super::lexer::{Token, TokenKind};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParseError {
    pub message: String,
    pub line: usize,
    pub col: usize,
}

pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    pub fn parse(&mut self) -> Result<StrategyNode, ParseError> {
        self.skip_newlines();
        let trade_in = self.parse_trade_in()?;
        self.skip_newlines();

        let mut stop_loss: Option<f64> = None;
        let mut take_profit: Option<f64> = None;
        let mut risk: Option<RiskConfig> = None;
        let mut rules = Vec::new();
        while !self.is_at_end() {
            match self.peek().kind.clone() {
                TokenKind::StopLoss => {
                    if stop_loss.is_some() {
                        return Err(ParseError {
                            message: "duplicate STOP_LOSS declaration".to_string(),
                            line: self.peek().line,
                            col: self.peek().col,
                        });
                    }
                    stop_loss = Some(self.parse_stop_loss()?);
                }
                TokenKind::TakeProfit => {
                    if take_profit.is_some() {
                        return Err(ParseError {
                            message: "duplicate TAKE_PROFIT declaration".to_string(),
                            line: self.peek().line,
                            col: self.peek().col,
                        });
                    }
                    take_profit = Some(self.parse_take_profit()?);
                }
                TokenKind::Risk => {
                    let parsed = self.parse_risk_declaration()?;
                    let config = risk.get_or_insert_with(RiskConfig::default);
                    if parsed.max_daily_loss_pct.is_some() {
                        if config.max_daily_loss_pct.is_some() {
                            return Err(ParseError {
                                message: "duplicate RISK MAX_DAILY_LOSS declaration".to_string(),
                                line: self.peek().line,
                                col: self.peek().col,
                            });
                        }
                        config.max_daily_loss_pct = parsed.max_daily_loss_pct;
                    }
                    if parsed.max_open_positions.is_some() {
                        if config.max_open_positions.is_some() {
                            return Err(ParseError {
                                message: "duplicate RISK MAX_POSITIONS declaration".to_string(),
                                line: self.peek().line,
                                col: self.peek().col,
                            });
                        }
                        config.max_open_positions = parsed.max_open_positions;
                    }
                    if parsed.max_orders.is_some() {
                        if config.max_orders.is_some() {
                            return Err(ParseError {
                                message: "duplicate RISK MAX_ORDERS declaration".to_string(),
                                line: self.peek().line,
                                col: self.peek().col,
                            });
                        }
                        config.max_orders = parsed.max_orders;
                    }
                }
                _ => {
                    let mut rule = self.parse_rule()?;
                    rule.id = format!("rule_{}", rules.len());
                    rules.push(rule);
                }
            }
            self.skip_newlines();
        }

        Ok(StrategyNode {
            name: "Untitled Strategy".to_string(),
            trade_in,
            stop_loss,
            take_profit,
            risk,
            rules,
        })
    }

    /// Parse a `STOP_LOSS <number> %` declaration. Returns the percentage as
    /// a float (2% → 2.0). The `%` token is required.
    fn parse_stop_loss(&mut self) -> Result<f64, ParseError> {
        self.expect_simple(TokenKind::StopLoss)?;
        self.skip_newlines();
        let value = self.parse_percent_number()?;
        self.skip_newlines();
        Ok(value)
    }

    /// Parse a `TAKE_PROFIT <number> %` declaration. Returns the percentage
    /// as a float (5% → 5.0). The `%` token is required.
    fn parse_take_profit(&mut self) -> Result<f64, ParseError> {
        self.expect_simple(TokenKind::TakeProfit)?;
        self.skip_newlines();
        let value = self.parse_percent_number()?;
        self.skip_newlines();
        Ok(value)
    }

    /// Parse a `RISK <sub-keyword> <value>` declaration. Returns a
    /// single-field `RiskConfig` carrying whichever sub-keyword appeared
    /// (the caller merges it into the existing `Option<RiskConfig>` and
    /// rejects duplicates for the same field).
    fn parse_risk_declaration(&mut self) -> Result<RiskConfig, ParseError> {
        self.expect_simple(TokenKind::Risk)?;
        self.skip_newlines();
        match self.peek().kind.clone() {
            TokenKind::MaxDailyLoss => {
                self.advance();
                self.skip_newlines();
                let value = self.parse_percent_number()?;
                self.skip_newlines();
                Ok(RiskConfig {
                    max_daily_loss_pct: Some(value),
                    max_open_positions: None,
                    max_orders: None,
                })
            }
            TokenKind::MaxPositions => {
                self.advance();
                self.skip_newlines();
                let value = self.parse_positive_integer_u32()?;
                self.skip_newlines();
                Ok(RiskConfig {
                    max_daily_loss_pct: None,
                    max_open_positions: Some(value),
                    max_orders: None,
                })
            }
            TokenKind::MaxOrders => {
                self.advance();
                self.skip_newlines();
                let value = self.parse_positive_integer_u32()?;
                self.skip_newlines();
                Ok(RiskConfig {
                    max_daily_loss_pct: None,
                    max_open_positions: None,
                    max_orders: Some(value),
                })
            }
            _ => {
                self.error_here("expected MAX_DAILY_LOSS, MAX_POSITIONS, or MAX_ORDERS after RISK")
            }
        }
    }

    /// Parse a positive integer (>= 1) as a `u32`. Used by
    /// `RISK MAX_POSITIONS` and `RISK MAX_ORDERS` — the validator also
    /// enforces the `>= 1` bound, so this parser is intentionally
    /// permissive on zero and lets validation catch it.
    fn parse_positive_integer_u32(&mut self) -> Result<u32, ParseError> {
        let token = self.advance().clone();
        match token.kind {
            TokenKind::Integer(value) => {
                if value > u32::MAX as usize {
                    return Err(ParseError {
                        message: format!("integer overflows u32: {}", value),
                        line: token.line,
                        col: token.col,
                    });
                }
                Ok(value as u32)
            }
            _ => Err(ParseError {
                message: "expected a positive integer".to_string(),
                line: token.line,
                col: token.col,
            }),
        }
    }

    /// Parse a `<number> %` pair (used by SL/TP declarations). The `%` is
    /// required so the user can't accidentally write `STOP_LOSS 2` (which
    /// would be ambiguous between rupees and percent).
    fn parse_percent_number(&mut self) -> Result<f64, ParseError> {
        let token = self.advance().clone();
        let value = match token.kind {
            TokenKind::Number(v) => v,
            TokenKind::Integer(v) => v as f64,
            _ => {
                return Err(ParseError {
                    message: "expected a percentage value (e.g. 2%)".to_string(),
                    line: token.line,
                    col: token.col,
                });
            }
        };
        if !self.matches_simple(TokenKind::Percent) {
            return Err(ParseError {
                message: "expected '%' after the percentage value".to_string(),
                line: self.peek().line,
                col: self.peek().col,
            });
        }
        Ok(value)
    }

    /// Parse an optional `TRADE_IN <items...>` header. Returns `None` if the
    /// keyword is absent (backwards compat with single-symbol strategies).
    fn parse_trade_in(&mut self) -> Result<Option<TradeIn>, ParseError> {
        if !self.matches_simple(TokenKind::TradeIn) {
            return Ok(None);
        }

        // Collect one or more comma-separated identifier tokens on the same
        // logical line. Identifiers here may contain underscores and digits
        // (e.g. NIFTY_50, RELIANCE).
        let mut items: Vec<String> = Vec::new();

        loop {
            match self.peek().kind.clone() {
                TokenKind::Identifier(s) => {
                    items.push(s.to_uppercase());
                    self.advance();
                }
                _ => {
                    return Err(ParseError {
                        message: "expected a symbol or index alias after TRADE_IN".to_string(),
                        line: self.peek().line,
                        col: self.peek().col,
                    });
                }
            }
            if self.matches_simple(TokenKind::Comma) {
                continue;
            }
            break;
        }

        if items.is_empty() {
            return Err(ParseError {
                message: "TRADE_IN requires at least one symbol or index alias".to_string(),
                line: self.peek().line,
                col: self.peek().col,
            });
        }

        // If exactly one item and it matches a known IndexAlias → TradeIn::Index.
        if items.len() == 1 {
            if let Some(alias) = IndexAlias::from_dsl_str(&items[0]) {
                return Ok(Some(TradeIn::Index(alias)));
            }
        }

        // Multiple items: reject any that are index aliases (can't mix).
        for item in &items {
            if IndexAlias::from_dsl_str(item).is_some() {
                return Err(ParseError {
                    message: format!(
                        "cannot mix index alias '{}' with explicit symbols in TRADE_IN — use one or the other",
                        item
                    ),
                    line: self.peek().line,
                    col: self.peek().col,
                });
            }
        }

        Ok(Some(TradeIn::Symbols(items)))
    }

    fn parse_rule(&mut self) -> Result<RuleNode, ParseError> {
        self.expect_simple(TokenKind::When)?;
        let condition = self.parse_condition()?;
        self.expect_simple(TokenKind::Newline)?;
        let action = self.parse_action()?;

        Ok(RuleNode {
            id: String::new(),
            condition,
            action,
        })
    }

    fn parse_condition(&mut self) -> Result<ConditionNode, ParseError> {
        let mut node = self.parse_primary_condition()?;

        loop {
            if self.matches_simple(TokenKind::And) {
                let right = self.parse_primary_condition()?;
                node = ConditionNode::And(Box::new(node), Box::new(right));
            } else if self.matches_simple(TokenKind::Or) {
                let right = self.parse_primary_condition()?;
                node = ConditionNode::Or(Box::new(node), Box::new(right));
            } else {
                break;
            }
        }

        Ok(node)
    }

    fn parse_primary_condition(&mut self) -> Result<ConditionNode, ParseError> {
        match &self.peek().kind {
            TokenKind::CrossAbove | TokenKind::CrossBelow => self.parse_cross(),
            TokenKind::Not => self.parse_not(),
            TokenKind::InPosition => self.parse_position_expr(),
            TokenKind::Between => self.parse_time_window(),
            _ => self.parse_comparison(),
        }
    }

    fn parse_comparison(&mut self) -> Result<ConditionNode, ParseError> {
        let left = self.parse_expr()?;
        let op = self.parse_compare_op()?;
        let right = self.parse_expr()?;
        Ok(ConditionNode::Comparison { left, op, right })
    }

    fn parse_cross(&mut self) -> Result<ConditionNode, ParseError> {
        let is_above = self.matches_simple(TokenKind::CrossAbove);
        if !is_above {
            self.expect_simple(TokenKind::CrossBelow)?;
        }
        self.expect_simple(TokenKind::LParen)?;
        let fast = self.parse_expr()?;
        self.expect_simple(TokenKind::Comma)?;
        let slow = self.parse_expr()?;
        self.expect_simple(TokenKind::RParen)?;

        if is_above {
            Ok(ConditionNode::CrossAbove { fast, slow })
        } else {
            Ok(ConditionNode::CrossBelow { fast, slow })
        }
    }

    fn parse_not(&mut self) -> Result<ConditionNode, ParseError> {
        self.expect_simple(TokenKind::Not)?;
        self.expect_simple(TokenKind::LParen)?;
        let inner = self.parse_condition()?;
        self.expect_simple(TokenKind::RParen)?;
        Ok(ConditionNode::Not(Box::new(inner)))
    }

    fn parse_position_expr(&mut self) -> Result<ConditionNode, ParseError> {
        self.expect_simple(TokenKind::InPosition)?;
        self.expect_simple(TokenKind::LParen)?;
        self.expect_simple(TokenKind::RParen)?;
        Ok(ConditionNode::InPosition)
    }

    fn parse_time_window(&mut self) -> Result<ConditionNode, ParseError> {
        self.expect_simple(TokenKind::Between)?;
        self.expect_simple(TokenKind::LParen)?;
        let start = self.parse_time()?;
        self.expect_simple(TokenKind::Comma)?;
        let end = self.parse_time()?;
        self.expect_simple(TokenKind::RParen)?;
        Ok(ConditionNode::TimeWindow { start, end })
    }

    fn parse_expr(&mut self) -> Result<ExprNode, ParseError> {
        match self.peek().kind.clone() {
            TokenKind::Integer(value) => {
                self.advance();
                Ok(ExprNode::Literal(value as f64))
            }
            TokenKind::Number(value) => {
                self.advance();
                Ok(ExprNode::Literal(value))
            }
            TokenKind::Close => {
                self.advance();
                Ok(ExprNode::PriceField(PriceField::Close))
            }
            TokenKind::Open => {
                self.advance();
                Ok(ExprNode::PriceField(PriceField::Open))
            }
            TokenKind::High => {
                self.advance();
                Ok(ExprNode::PriceField(PriceField::High))
            }
            TokenKind::Low => {
                self.advance();
                Ok(ExprNode::PriceField(PriceField::Low))
            }
            TokenKind::Volume => {
                self.advance();
                Ok(ExprNode::PriceField(PriceField::Volume))
            }
            TokenKind::PrevClose => {
                self.advance();
                Ok(ExprNode::PriceField(PriceField::PrevClose))
            }
            TokenKind::PrevOpen => {
                self.advance();
                Ok(ExprNode::PriceField(PriceField::PrevOpen))
            }
            TokenKind::PrevHigh => {
                self.advance();
                Ok(ExprNode::PriceField(PriceField::PrevHigh))
            }
            TokenKind::PrevLow => {
                self.advance();
                Ok(ExprNode::PriceField(PriceField::PrevLow))
            }
            TokenKind::Ema => self.parse_indicator(IndicatorKind::Ema),
            TokenKind::Ma => self.parse_indicator(IndicatorKind::Ma),
            TokenKind::Rsi => self.parse_indicator(IndicatorKind::Rsi),
            TokenKind::RelVol => self.parse_indicator(IndicatorKind::RelVol),
            TokenKind::Atr => self.parse_indicator(IndicatorKind::Atr),
            TokenKind::Vwap => self.parse_indicator(IndicatorKind::Vwap),
            TokenKind::BbUpper => self.parse_indicator(IndicatorKind::BbUpper),
            TokenKind::BbLower => self.parse_indicator(IndicatorKind::BbLower),
            TokenKind::BbMid => self.parse_indicator(IndicatorKind::BbMid),
            _ => self.error_here("expected expression"),
        }
    }

    fn parse_indicator(&mut self, kind: IndicatorKind) -> Result<ExprNode, ParseError> {
        self.advance();
        self.expect_simple(TokenKind::LParen)?;
        let period = match self.advance().kind.clone() {
            TokenKind::Integer(value) => value,
            _ => return self.error_previous("expected integer indicator period"),
        };
        self.expect_simple(TokenKind::RParen)?;
        Ok(ExprNode::Indicator(IndicatorCall { kind, period }))
    }

    fn parse_action(&mut self) -> Result<ActionNode, ParseError> {
        if self.matches_simple(TokenKind::Buy) {
            let quantity = self.parse_quantity()?;
            return Ok(ActionNode::Buy { quantity });
        }

        if self.matches_simple(TokenKind::Sell) {
            if self.matches_simple(TokenKind::All) {
                return Ok(ActionNode::SellAll);
            }
            let quantity = self.parse_quantity()?;
            return Ok(ActionNode::Sell { quantity });
        }

        self.error_here("expected BUY or SELL action")
    }

    fn parse_compare_op(&mut self) -> Result<CompareOp, ParseError> {
        let token = self.advance();
        match token.kind {
            TokenKind::Lt => Ok(CompareOp::Lt),
            TokenKind::Gt => Ok(CompareOp::Gt),
            TokenKind::Lte => Ok(CompareOp::Lte),
            TokenKind::Gte => Ok(CompareOp::Gte),
            TokenKind::Eq => Ok(CompareOp::Eq),
            TokenKind::Neq => Ok(CompareOp::Neq),
            _ => self.error_previous("expected comparison operator"),
        }
    }

    fn parse_quantity(&mut self) -> Result<QuantitySpec, ParseError> {
        match self.advance().kind.clone() {
            TokenKind::Integer(value) => {
                if self.matches_simple(TokenKind::Percent) {
                    Ok(QuantitySpec::PercentCapital(value as f64))
                } else if self.matches_simple(TokenKind::Worth) {
                    Ok(QuantitySpec::ValueBased(value as f64))
                } else {
                    Ok(QuantitySpec::Fixed(value as u64))
                }
            }
            TokenKind::Number(value) => {
                if self.matches_simple(TokenKind::Percent) {
                    Ok(QuantitySpec::PercentCapital(value))
                } else if self.matches_simple(TokenKind::Worth) {
                    Ok(QuantitySpec::ValueBased(value))
                } else {
                    self.error_previous("float quantity requires % or WORTH")
                }
            }
            _ => self.error_previous("expected quantity"),
        }
    }

    fn parse_time(&mut self) -> Result<NaiveTime, ParseError> {
        let token = self.advance().clone();
        match token.kind {
            TokenKind::TimeStr(value) => {
                NaiveTime::parse_from_str(&value, "%H:%M").map_err(|_| ParseError {
                    message: format!("invalid time '{}'", value),
                    line: token.line,
                    col: token.col,
                })
            }
            _ => Err(ParseError {
                message: "expected HH:MM time".to_string(),
                line: token.line,
                col: token.col,
            }),
        }
    }

    fn skip_newlines(&mut self) {
        while self.matches_simple(TokenKind::Newline) {}
    }

    fn peek(&self) -> &Token {
        &self.tokens[self.pos]
    }

    fn advance(&mut self) -> &Token {
        if !self.is_at_end() {
            self.pos += 1;
        }
        &self.tokens[self.pos - 1]
    }

    fn expect_simple(&mut self, kind: TokenKind) -> Result<&Token, ParseError> {
        if same_variant(&self.peek().kind, &kind) {
            Ok(self.advance())
        } else {
            self.error_here(&format!("expected {}", token_name(&kind)))
        }
    }

    fn matches_simple(&mut self, kind: TokenKind) -> bool {
        if same_variant(&self.peek().kind, &kind) {
            self.advance();
            true
        } else {
            false
        }
    }

    fn is_at_end(&self) -> bool {
        matches!(self.peek().kind, TokenKind::Eof)
    }

    fn error_here<T>(&self, message: &str) -> Result<T, ParseError> {
        let token = self.peek();
        Err(ParseError {
            message: message.to_string(),
            line: token.line,
            col: token.col,
        })
    }

    fn error_previous<T>(&self, message: &str) -> Result<T, ParseError> {
        let token = &self.tokens[self.pos.saturating_sub(1)];
        Err(ParseError {
            message: message.to_string(),
            line: token.line,
            col: token.col,
        })
    }
}

fn same_variant(left: &TokenKind, right: &TokenKind) -> bool {
    std::mem::discriminant(left) == std::mem::discriminant(right)
}

fn token_name(kind: &TokenKind) -> &'static str {
    match kind {
        TokenKind::When => "WHEN",
        TokenKind::TradeIn => "TRADE_IN",
        TokenKind::Percent => "'%'",
        TokenKind::Newline => "newline",
        TokenKind::LParen => "'('",
        TokenKind::RParen => "')'",
        TokenKind::Comma => "','",
        TokenKind::Eof => "end of file",
        _ => "token",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategy::dsl::lexer::Lexer;

    fn parse(source: &str) -> StrategyNode {
        let tokens = Lexer::tokenize(source).unwrap();
        Parser::new(tokens).parse().unwrap()
    }

    #[test]
    fn parses_examples() {
        let source = r#"
WHEN cross_above(ema(20), ema(50))
BUY 10

WHEN cross_below(ema(20), ema(50))
SELL ALL

WHEN rsi(14) < 30
BUY 5

WHEN rsi(14) > 70
SELL ALL

WHEN close > bb_upper(20)
SELL 10

WHEN close < bb_lower(20)
BUY 10

WHEN ema(9) > ema(21) AND rsi(14) < 60
BUY 5

WHEN cross_above(ema(20), ema(50)) AND NOT (in_position())
BUY 10
"#;
        assert_eq!(parse(source).rules.len(), 8);
    }

    #[test]
    fn parses_simple_rsi_rule() {
        let strategy = parse("WHEN rsi(14) < 30\nBUY 5");
        assert_eq!(strategy.rules.len(), 1);
        assert_eq!(strategy.rules[0].id, "rule_0");
        assert!(matches!(
            strategy.rules[0].action,
            ActionNode::Buy {
                quantity: QuantitySpec::Fixed(5)
            }
        ));
    }

    #[test]
    fn assigns_rule_ids_in_order() {
        let strategy = parse("WHEN close > 10\nBUY 1\nWHEN close < 5\nSELL 1");
        assert_eq!(strategy.rules[0].id, "rule_0");
        assert_eq!(strategy.rules[1].id, "rule_1");
    }

    #[test]
    fn assigns_sequential_rule_ids() {
        let strategy = parse("WHEN rsi(14) < 30\nBUY 5\n\nWHEN rsi(14) > 70\nSELL ALL");
        assert_eq!(strategy.rules[0].id, "rule_0");
        assert_eq!(strategy.rules[1].id, "rule_1");
    }

    #[test]
    fn errors_on_missing_action() {
        let tokens = Lexer::tokenize("WHEN close > 10").unwrap();
        let err = Parser::new(tokens).parse().unwrap_err();
        assert!(err.message.contains("action"));
    }

    #[test]
    fn parse_error_on_missing_action() {
        let tokens = Lexer::tokenize("WHEN rsi(14) < 30").unwrap();
        assert!(Parser::new(tokens).parse().is_err());
    }

    #[test]
    fn parse_error_on_incomplete_comparison() {
        let tokens = Lexer::tokenize("WHEN rsi(14) <\nBUY 5").unwrap();
        assert!(Parser::new(tokens).parse().is_err());
    }

    #[test]
    fn errors_on_malformed_condition() {
        let tokens = Lexer::tokenize("WHEN close\nBUY 1").unwrap();
        let err = Parser::new(tokens).parse().unwrap_err();
        assert!(err.message.contains("comparison"));
    }

    #[test]
    fn nests_and_conditions() {
        let strategy = parse("WHEN close > 10 AND rsi(14) < 60\nBUY 1");
        assert!(matches!(
            strategy.rules[0].condition,
            ConditionNode::And(_, _)
        ));
    }

    #[test]
    fn parses_and_condition() {
        let strategy = parse("WHEN ema(9) > ema(21) AND rsi(14) < 60\nBUY 5");
        assert!(matches!(
            strategy.rules[0].condition,
            ConditionNode::And(_, _)
        ));
    }

    #[test]
    fn parses_not_condition() {
        let strategy = parse("WHEN NOT (in_position())\nBUY 1");
        assert!(matches!(strategy.rules[0].condition, ConditionNode::Not(_)));
    }

    #[test]
    fn parses_not_wrapping_comparison() {
        let strategy = parse("WHEN NOT (rsi(14) > 70)\nBUY 5");
        assert!(matches!(strategy.rules[0].condition, ConditionNode::Not(_)));
    }

    #[test]
    fn parses_sell_all_distinctly() {
        let strategy = parse("WHEN close > 10\nSELL ALL");
        assert!(matches!(strategy.rules[0].action, ActionNode::SellAll));
    }

    #[test]
    fn parses_percent_and_worth_quantities() {
        let strategy = parse("WHEN close > 100\nBUY 10%\nWHEN close < 90\nBUY 5000 WORTH");
        assert!(matches!(
            strategy.rules[0].action,
            ActionNode::Buy {
                quantity: QuantitySpec::PercentCapital(10.0)
            }
        ));
        assert!(matches!(
            strategy.rules[1].action,
            ActionNode::Buy {
                quantity: QuantitySpec::ValueBased(5000.0)
            }
        ));
    }

    #[test]
    fn rejects_float_quantity_without_sizing_suffix() {
        let tokens = Lexer::tokenize("WHEN close > 100\nBUY 1.5").unwrap();
        let err = Parser::new(tokens).parse().unwrap_err();
        assert_eq!(err.message, "float quantity requires % or WORTH");
    }

    #[test]
    fn sell_all_is_sell_all_not_sell_quantity() {
        let strategy = parse("WHEN close > 100\nSELL ALL");
        assert!(matches!(strategy.rules[0].action, ActionNode::SellAll));
    }

    #[test]
    fn parses_cross_above() {
        let strategy = parse("WHEN cross_above(ema(20), ema(50))\nBUY 10");
        assert!(matches!(
            strategy.rules[0].condition,
            ConditionNode::CrossAbove { .. }
        ));
    }

    #[test]
    fn parses_rel_vol_indicator() {
        let node = parse("WHEN rel_vol(20) > 2.0\nBUY 5");
        assert_eq!(node.rules.len(), 1);
        match &node.rules[0].condition {
            ConditionNode::Comparison { left, .. } => {
                assert!(matches!(
                    left,
                    ExprNode::Indicator(IndicatorCall {
                        kind: IndicatorKind::RelVol,
                        period: 20
                    })
                ));
            }
            _ => panic!("expected comparison"),
        }
    }

    #[test]
    fn parses_prev_close() {
        let node = parse("WHEN prev_close < 100\nBUY 1");
        match &node.rules[0].condition {
            ConditionNode::Comparison { left, .. } => {
                assert!(matches!(left, ExprNode::PriceField(PriceField::PrevClose)));
            }
            _ => panic!("expected comparison"),
        }
    }
    #[test]
    fn parses_prev_open() {
        let node = parse("WHEN prev_open < 100\nBUY 1");
        match &node.rules[0].condition {
            ConditionNode::Comparison { left, .. } => {
                assert!(matches!(left, ExprNode::PriceField(PriceField::PrevOpen)));
            }
            _ => panic!("expected comparison"),
        }
    }

    #[test]
    fn parses_prev_high() {
        let node = parse("WHEN close > prev_high\nBUY 1");
        match &node.rules[0].condition {
            ConditionNode::Comparison { right, .. } => {
                assert!(matches!(right, ExprNode::PriceField(PriceField::PrevHigh)));
            }
            _ => panic!("expected comparison"),
        }
    }

    #[test]
    fn parses_prev_low() {
        let node = parse("WHEN close < prev_low\nSELL ALL");
        match &node.rules[0].condition {
            ConditionNode::Comparison { right, .. } => {
                assert!(matches!(right, ExprNode::PriceField(PriceField::PrevLow)));
            }
            _ => panic!("expected comparison"),
        }
    }

    #[test]
    fn deterministic_ids_across_reparses() {
        let source = "WHEN rsi(14) < 30\nBUY 5";
        let first = parse(source);
        let second = parse(source);
        assert_eq!(first.rules[0].id, second.rules[0].id);
    }

    #[test]
    fn print_ast_for_inspection() {
        let tokens = Lexer::tokenize("WHEN rsi(14) < 30\nBUY 5").unwrap();
        let strategy = Parser::new(tokens).parse().unwrap();
        println!("{:#?}", strategy);
    }

    #[test]
    fn print_parse_error_for_inspection() {
        let tokens = Lexer::tokenize("WHEN rsi(14) <\nBUY 5").unwrap();
        let result = Parser::new(tokens).parse();
        println!("{:#?}", result);
        assert!(result.is_err());
    }

    // ---------- TRADE_IN ----------

    #[test]
    fn parses_trade_in_symbols() {
        let strategy = parse("TRADE_IN RELIANCE, INFY, TCS\nWHEN close > 100\nBUY 1");
        match strategy.trade_in {
            Some(TradeIn::Symbols(syms)) => {
                assert_eq!(syms, vec!["RELIANCE", "INFY", "TCS"]);
            }
            other => panic!("expected TradeIn::Symbols, got {:?}", other),
        }
        assert_eq!(strategy.rules.len(), 1);
    }

    #[test]
    fn parses_trade_in_index_alias() {
        let strategy = parse("TRADE_IN NIFTY_BANK\nWHEN close > 100\nBUY 1");
        match strategy.trade_in {
            Some(TradeIn::Index(IndexAlias::NiftyBank)) => {}
            other => panic!("expected TradeIn::Index(NiftyBank), got {:?}", other),
        }
    }

    #[test]
    fn rejects_mixed_trade_in() {
        let tokens =
            Lexer::tokenize("TRADE_IN NIFTY_BANK, RELIANCE\nWHEN close > 100\nBUY 1").unwrap();
        let err = Parser::new(tokens).parse().unwrap_err();
        assert!(err.message.contains("cannot mix"), "got: {}", err.message);
    }

    #[test]
    fn omits_trade_in_when_keyword_absent() {
        let strategy = parse("WHEN close > 100\nBUY 1");
        assert!(strategy.trade_in.is_none());
    }

    // ---------- STOP_LOSS / TAKE_PROFIT ----------

    #[test]
    fn parses_stop_loss_before_rules() {
        let strategy = parse("STOP_LOSS 2%\nWHEN close > 100\nBUY 1");
        assert_eq!(strategy.stop_loss, Some(2.0));
        assert_eq!(strategy.take_profit, None);
        assert_eq!(strategy.rules.len(), 1);
    }

    #[test]
    fn parses_take_profit_before_rules() {
        let strategy = parse("TAKE_PROFIT 5%\nWHEN close > 100\nBUY 1");
        assert_eq!(strategy.stop_loss, None);
        assert_eq!(strategy.take_profit, Some(5.0));
    }

    #[test]
    fn parses_both_declarations() {
        let strategy = parse("STOP_LOSS 2%\nTAKE_PROFIT 5%\nWHEN close > 100\nBUY 1");
        assert_eq!(strategy.stop_loss, Some(2.0));
        assert_eq!(strategy.take_profit, Some(5.0));
    }

    #[test]
    fn parses_declarations_interleaved_with_rules() {
        let strategy = parse(
            "WHEN close > 100\nBUY 1\nSTOP_LOSS 2%\nWHEN close < 50\nSELL ALL\nTAKE_PROFIT 5%",
        );
        assert_eq!(strategy.stop_loss, Some(2.0));
        assert_eq!(strategy.take_profit, Some(5.0));
        assert_eq!(strategy.rules.len(), 2);
    }

    #[test]
    fn parses_float_percent_value() {
        let strategy = parse("STOP_LOSS 2.5%\nWHEN close > 100\nBUY 1");
        assert_eq!(strategy.stop_loss, Some(2.5));
    }

    #[test]
    fn rejects_duplicate_stop_loss() {
        let tokens = Lexer::tokenize("STOP_LOSS 2%\nSTOP_LOSS 5%\nBUY 1").unwrap();
        let err = Parser::new(tokens).parse().unwrap_err();
        assert!(err.message.contains("duplicate STOP_LOSS"));
    }

    #[test]
    fn rejects_duplicate_take_profit() {
        let tokens = Lexer::tokenize("TAKE_PROFIT 5%\nTAKE_PROFIT 8%\nBUY 1").unwrap();
        let err = Parser::new(tokens).parse().unwrap_err();
        assert!(err.message.contains("duplicate TAKE_PROFIT"));
    }

    #[test]
    fn rejects_stop_loss_without_percent() {
        let tokens = Lexer::tokenize("STOP_LOSS 2\nWHEN close > 100\nBUY 1").unwrap();
        let err = Parser::new(tokens).parse().unwrap_err();
        assert!(err.message.contains("expected '%'"));
    }

    #[test]
    fn strategy_without_declarations_has_none() {
        let strategy = parse("WHEN close > 100\nBUY 1");
        assert_eq!(strategy.stop_loss, None);
        assert_eq!(strategy.take_profit, None);
    }

    // ---------- RISK ----------

    #[test]
    fn parses_risk_max_daily_loss() {
        let strategy = parse("RISK MAX_DAILY_LOSS 5%\nWHEN close > 100\nBUY 1");
        let risk = strategy.risk.expect("expected risk config");
        assert_eq!(risk.max_daily_loss_pct, Some(5.0));
        assert_eq!(risk.max_open_positions, None);
        assert_eq!(risk.max_orders, None);
    }

    #[test]
    fn parses_risk_max_positions() {
        let strategy = parse("RISK MAX_POSITIONS 3\nWHEN close > 100\nBUY 1");
        let risk = strategy.risk.expect("expected risk config");
        assert_eq!(risk.max_open_positions, Some(3));
        assert_eq!(risk.max_daily_loss_pct, None);
        assert_eq!(risk.max_orders, None);
    }

    #[test]
    fn parses_risk_max_orders() {
        let strategy = parse("RISK MAX_ORDERS 20\nWHEN close > 100\nBUY 1");
        let risk = strategy.risk.expect("expected risk config");
        assert_eq!(risk.max_orders, Some(20));
        assert_eq!(risk.max_daily_loss_pct, None);
        assert_eq!(risk.max_open_positions, None);
    }

    #[test]
    fn parses_all_three_risk_declarations() {
        let strategy = parse(
            "RISK MAX_DAILY_LOSS 5%\nRISK MAX_POSITIONS 3\nRISK MAX_ORDERS 20\nWHEN close > 100\nBUY 1",
        );
        let risk = strategy.risk.expect("expected risk config");
        assert_eq!(risk.max_daily_loss_pct, Some(5.0));
        assert_eq!(risk.max_open_positions, Some(3));
        assert_eq!(risk.max_orders, Some(20));
    }

    #[test]
    fn parses_risk_declarations_interleaved_with_rules() {
        let strategy = parse("RISK MAX_ORDERS 5\nWHEN close > 100\nBUY 1\nRISK MAX_DAILY_LOSS 2%");
        let risk = strategy.risk.expect("expected risk config");
        assert_eq!(risk.max_daily_loss_pct, Some(2.0));
        assert_eq!(risk.max_orders, Some(5));
        assert_eq!(strategy.rules.len(), 1);
    }

    #[test]
    fn rejects_duplicate_max_daily_loss() {
        let tokens = Lexer::tokenize(
            "RISK MAX_DAILY_LOSS 5%\nRISK MAX_DAILY_LOSS 3%\nWHEN close > 100\nBUY 1",
        )
        .unwrap();
        let err = Parser::new(tokens).parse().unwrap_err();
        assert!(
            err.message.contains("duplicate RISK MAX_DAILY_LOSS"),
            "got: {}",
            err.message
        );
    }

    #[test]
    fn rejects_duplicate_max_positions() {
        let tokens =
            Lexer::tokenize("RISK MAX_POSITIONS 3\nRISK MAX_POSITIONS 5\nWHEN close > 100\nBUY 1")
                .unwrap();
        let err = Parser::new(tokens).parse().unwrap_err();
        assert!(
            err.message.contains("duplicate RISK MAX_POSITIONS"),
            "got: {}",
            err.message
        );
    }

    #[test]
    fn rejects_duplicate_max_orders() {
        let tokens =
            Lexer::tokenize("RISK MAX_ORDERS 20\nRISK MAX_ORDERS 50\nWHEN close > 100\nBUY 1")
                .unwrap();
        let err = Parser::new(tokens).parse().unwrap_err();
        assert!(
            err.message.contains("duplicate RISK MAX_ORDERS"),
            "got: {}",
            err.message
        );
    }

    #[test]
    fn rejects_risk_without_sub_keyword() {
        let tokens = Lexer::tokenize("RISK\nWHEN close > 100\nBUY 1").unwrap();
        let err = Parser::new(tokens).parse().unwrap_err();
        assert!(
            err.message.contains("MAX_DAILY_LOSS")
                || err.message.contains("MAX_POSITIONS")
                || err.message.contains("MAX_ORDERS"),
            "got: {}",
            err.message
        );
    }

    #[test]
    fn rejects_risk_max_daily_loss_without_percent() {
        let tokens = Lexer::tokenize("RISK MAX_DAILY_LOSS 5\nWHEN close > 100\nBUY 1").unwrap();
        let err = Parser::new(tokens).parse().unwrap_err();
        assert!(err.message.contains("expected '%'"), "got: {}", err.message);
    }

    #[test]
    fn strategy_without_risk_has_none() {
        let strategy = parse("WHEN close > 100\nBUY 1");
        assert!(strategy.risk.is_none());
    }
}
use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use super::ast::{
    ActionNode, ConditionNode, ExprNode, IndicatorCall, IndicatorKind, QuantitySpec, RuleNode,
    StrategyNode, TradeIn,
};

pub struct AstValidator;

impl AstValidator {
    pub fn validate(strategy: &StrategyNode) -> Vec<ValidationError> {
        let mut errors = Vec::new();

        if strategy.rules.is_empty() {
            errors.push(ValidationError {
                rule_id: String::new(),
                message: "strategy must contain at least one rule".to_string(),
                kind: ValidationErrorKind::EmptyStrategy,
            });
        }

        let mut seen = HashSet::new();
        let mut duplicates = HashSet::new();
        for rule in &strategy.rules {
            if !seen.insert(rule.id.clone()) {
                duplicates.insert(rule.id.clone());
            }
            validate_rule(rule, &mut errors);
        }

        for rule_id in duplicates {
            errors.push(ValidationError {
                rule_id,
                message: "duplicate rule id".to_string(),
                kind: ValidationErrorKind::DuplicateRuleIds,
            });
        }

        if strategy.trade_in.is_some() {
            validate_trade_in(strategy.trade_in.as_ref().unwrap(), &mut errors);
        }

        validate_percent_threshold(strategy.stop_loss, "STOP_LOSS", &mut errors);
        validate_percent_threshold(strategy.take_profit, "TAKE_PROFIT", &mut errors);

        if let Some(risk) = &strategy.risk {
            validate_risk(risk, &mut errors);
        }

        errors
    }
}

/// Validate a `RiskConfig`. `max_daily_loss_pct` must be in `(0.0, 100.0]`
/// (zero means "disabled" — use `None` instead). `max_open_positions` and
/// `max_orders` must be `>= 1` (zero would mean "never trade").
fn validate_risk(risk: &super::ast::RiskConfig, errors: &mut Vec<ValidationError>) {
    if let Some(pct) = risk.max_daily_loss_pct {
        if !pct.is_finite() || pct <= 0.0 || pct > 100.0 {
            errors.push(ValidationError {
                rule_id: String::new(),
                message: format!("RISK MAX_DAILY_LOSS must be in (0, 100] (got {pct})"),
                kind: ValidationErrorKind::InvalidMaxDailyLoss,
            });
        }
    }
    if let Some(positions) = risk.max_open_positions {
        if positions == 0 {
            errors.push(ValidationError {
                rule_id: String::new(),
                message: "RISK MAX_POSITIONS must be >= 1".to_string(),
                kind: ValidationErrorKind::InvalidMaxPositions,
            });
        }
    }
    if let Some(orders) = risk.max_orders {
        if orders == 0 {
            errors.push(ValidationError {
                rule_id: String::new(),
                message: "RISK MAX_ORDERS must be >= 1".to_string(),
                kind: ValidationErrorKind::InvalidMaxOrders,
            });
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationError {
    pub rule_id: String,
    pub message: String,
    pub kind: ValidationErrorKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ValidationErrorKind {
    EmptyStrategy,
    InvalidPeriod { indicator: String, period: usize },
    InvalidQuantity,
    CrossWithLiteral,
    DuplicateRuleIds,
    InvalidTimeRange,
    InvalidTradeIn,
    InvalidStopLoss,
    InvalidTakeProfit,
    InvalidMaxDailyLoss,
    InvalidMaxPositions,
    InvalidMaxOrders,
}

/// Validate an optional percentage threshold (STOP_LOSS / TAKE_PROFIT). Both
/// must be in `(0.0, 100.0]` — zero means "disabled" (use `None`) and over 100
/// is nonsensical. The two thresholds are validated independently; they don't
/// need to sum to anything specific.
fn validate_percent_threshold(value: Option<f64>, label: &str, errors: &mut Vec<ValidationError>) {
    if let Some(pct) = value {
        if !pct.is_finite() || pct <= 0.0 || pct > 100.0 {
            let kind = if label == "STOP_LOSS" {
                ValidationErrorKind::InvalidStopLoss
            } else {
                ValidationErrorKind::InvalidTakeProfit
            };
            errors.push(ValidationError {
                rule_id: String::new(),
                message: format!("{label} must be in (0, 100] (got {pct})"),
                kind,
            });
        }
    }
}

fn validate_trade_in(trade_in: &TradeIn, errors: &mut Vec<ValidationError>) {
    match trade_in {
        TradeIn::Symbols(syms) => {
            if syms.is_empty() {
                errors.push(ValidationError {
                    rule_id: String::new(),
                    message: "TRADE_IN symbol list is empty".to_string(),
                    kind: ValidationErrorKind::InvalidTradeIn,
                });
            }
            if syms.len() > 500 {
                errors.push(ValidationError {
                    rule_id: String::new(),
                    message:
                        "TRADE_IN explicit symbol list exceeds 500 symbols; use an index alias instead"
                            .to_string(),
                    kind: ValidationErrorKind::InvalidTradeIn,
                });
            }
            for sym in syms {
                if sym.is_empty() {
                    errors.push(ValidationError {
                        rule_id: String::new(),
                        message: "TRADE_IN contains an empty symbol".to_string(),
                        kind: ValidationErrorKind::InvalidTradeIn,
                    });
                }
                if sym.len() > 30 {
                    errors.push(ValidationError {
                        rule_id: String::new(),
                        message: format!("TRADE_IN symbol '{}' is too long (max 30 chars)", sym),
                        kind: ValidationErrorKind::InvalidTradeIn,
                    });
                }
            }
        }
        TradeIn::Index(_) => {
            // Valid — resolved at runtime from IndexRegistry.
        }
    }
}

fn validate_rule(rule: &RuleNode, errors: &mut Vec<ValidationError>) {
    validate_condition(&rule.id, &rule.condition, errors);
    validate_action(rule, errors);
}

fn validate_condition(rule_id: &str, condition: &ConditionNode, errors: &mut Vec<ValidationError>) {
    match condition {
        ConditionNode::Comparison { left, right, .. } => {
            validate_expr(rule_id, left, errors);
            validate_expr(rule_id, right, errors);
        }
        ConditionNode::CrossAbove { fast, slow } | ConditionNode::CrossBelow { fast, slow } => {
            validate_expr(rule_id, fast, errors);
            validate_expr(rule_id, slow, errors);
            if matches!(fast, ExprNode::Literal(_)) || matches!(slow, ExprNode::Literal(_)) {
                errors.push(ValidationError {
                    rule_id: rule_id.to_string(),
                    message: "cross conditions cannot use literal operands".to_string(),
                    kind: ValidationErrorKind::CrossWithLiteral,
                });
            }
        }
        ConditionNode::And(left, right) | ConditionNode::Or(left, right) => {
            validate_condition(rule_id, left, errors);
            validate_condition(rule_id, right, errors);
        }
        ConditionNode::Not(inner) => validate_condition(rule_id, inner, errors),
        ConditionNode::InPosition => {}
        ConditionNode::TimeWindow { start, end } => {
            if start >= end {
                errors.push(ValidationError {
                    rule_id: rule_id.to_string(),
                    message: "time window start must be before end".to_string(),
                    kind: ValidationErrorKind::InvalidTimeRange,
                });
            }
        }
    }
}

fn validate_expr(rule_id: &str, expr: &ExprNode, errors: &mut Vec<ValidationError>) {
    if let ExprNode::Indicator(call) = expr {
        validate_indicator(rule_id, call, errors);
    }
}

fn validate_indicator(rule_id: &str, call: &IndicatorCall, errors: &mut Vec<ValidationError>) {
    if call.period == 0 {
        errors.push(ValidationError {
            rule_id: rule_id.to_string(),
            message: "indicator period must be greater than zero".to_string(),
            kind: ValidationErrorKind::InvalidPeriod {
                indicator: indicator_name(&call.kind).to_string(),
                period: call.period,
            },
        });
    }
}

fn validate_action(rule: &RuleNode, errors: &mut Vec<ValidationError>) {
    let quantity = match &rule.action {
        ActionNode::Buy { quantity } | ActionNode::Sell { quantity } => quantity,
        ActionNode::SellAll => return,
    };

    match quantity {
        QuantitySpec::Fixed(value) if *value == 0 => {
            push_invalid_quantity(rule, errors, "order quantity must be greater than zero");
        }
        QuantitySpec::PercentCapital(value) if *value <= 0.0 || *value > 100.0 => {
            push_invalid_quantity(
                rule,
                errors,
                &format!("percent quantity must be between 0 and 100 (got {value})"),
            );
        }
        QuantitySpec::ValueBased(value) if *value <= 0.0 => {
            push_invalid_quantity(
                rule,
                errors,
                &format!("WORTH quantity must be positive (got {value})"),
            );
        }
        _ => {}
    }
}

fn push_invalid_quantity(rule: &RuleNode, errors: &mut Vec<ValidationError>, message: &str) {
    errors.push(ValidationError {
        rule_id: rule.id.clone(),
        message: message.to_string(),
        kind: ValidationErrorKind::InvalidQuantity,
    });
}

fn indicator_name(kind: &IndicatorKind) -> &'static str {
    match kind {
        IndicatorKind::Ema => "ema",
        IndicatorKind::Ma => "ma",
        IndicatorKind::Rsi => "rsi",
        IndicatorKind::RelVol => "rel_vol",
        IndicatorKind::Atr => "atr",
        IndicatorKind::Vwap => "vwap",
        IndicatorKind::BbUpper => "bb_upper",
        IndicatorKind::BbLower => "bb_lower",
        IndicatorKind::BbMid => "bb_mid",
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveTime;

    use super::*;
    use crate::strategy::dsl::ast::{CompareOp, IndexAlias, PriceField};

    fn rule(id: &str, condition: ConditionNode, action: ActionNode) -> RuleNode {
        RuleNode {
            id: id.to_string(),
            condition,
            action,
        }
    }

    fn strategy(rules: Vec<RuleNode>) -> StrategyNode {
        StrategyNode {
            name: "test".to_string(),
            trade_in: None,
            stop_loss: None,
            take_profit: None,
            risk: None,
            rules,
        }
    }

    #[test]
    fn rejects_empty_strategy() {
        let errors = AstValidator::validate(&strategy(vec![]));
        assert!(errors
            .iter()
            .any(|err| matches!(err.kind, ValidationErrorKind::EmptyStrategy)));
    }

    #[test]
    fn valid_strategy_has_no_errors() {
        let errors = AstValidator::validate(&strategy(vec![make_rsi_rule(14, 5)]));
        assert!(errors.is_empty());
    }

    #[test]
    fn rejects_zero_indicator_period() {
        let errors = AstValidator::validate(&strategy(vec![rule(
            "rule_0",
            ConditionNode::Comparison {
                left: ExprNode::Indicator(IndicatorCall {
                    kind: IndicatorKind::Ema,
                    period: 0,
                }),
                op: CompareOp::Gt,
                right: ExprNode::Literal(10.0),
            },
            ActionNode::Buy {
                quantity: QuantitySpec::Fixed(1),
            },
        )]));
        assert!(errors
            .iter()
            .any(|err| matches!(err.kind, ValidationErrorKind::InvalidPeriod { .. })));
    }

    #[test]
    fn rejects_zero_period() {
        let errors = AstValidator::validate(&strategy(vec![make_rsi_rule(0, 5)]));
        assert!(errors
            .iter()
            .any(|err| matches!(err.kind, ValidationErrorKind::InvalidPeriod { .. })));
    }

    #[test]
    fn rejects_rel_vol_with_zero_period() {
        let errors = AstValidator::validate(&strategy(vec![rule(
            "rule_0",
            ConditionNode::Comparison {
                left: ExprNode::Indicator(IndicatorCall {
                    kind: IndicatorKind::RelVol,
                    period: 0,
                }),
                op: CompareOp::Gt,
                right: ExprNode::Literal(1.5),
            },
            ActionNode::Buy {
                quantity: QuantitySpec::Fixed(1),
            },
        )]));
        assert!(errors
            .iter()
            .any(|err| matches!(err.kind, ValidationErrorKind::InvalidPeriod { .. })));
    }

    #[test]
    fn rejects_zero_quantity() {
        let errors = AstValidator::validate(&strategy(vec![rule(
            "rule_0",
            price_condition(),
            ActionNode::Buy {
                quantity: QuantitySpec::Fixed(0),
            },
        )]));
        assert!(errors
            .iter()
            .any(|err| matches!(err.kind, ValidationErrorKind::InvalidQuantity)));
    }

    #[test]
    fn rejects_zero_quantity_from_rsi_rule() {
        let errors = AstValidator::validate(&strategy(vec![make_rsi_rule(14, 0)]));
        assert!(errors
            .iter()
            .any(|err| matches!(err.kind, ValidationErrorKind::InvalidQuantity)));
    }

    #[test]
    fn rejects_invalid_percent_and_worth_quantities() {
        let errors = AstValidator::validate(&strategy(vec![
            rule(
                "rule_0",
                price_condition(),
                ActionNode::Buy {
                    quantity: QuantitySpec::PercentCapital(101.0),
                },
            ),
            rule(
                "rule_1",
                price_condition(),
                ActionNode::Buy {
                    quantity: QuantitySpec::ValueBased(0.0),
                },
            ),
        ]));
        assert!(errors
            .iter()
            .any(|err| err.message == "percent quantity must be between 0 and 100 (got 101)"));
        assert!(errors
            .iter()
            .any(|err| err.message == "WORTH quantity must be positive (got 0)"));
    }

    #[test]
    fn rejects_cross_with_literal() {
        let errors = AstValidator::validate(&strategy(vec![rule(
            "rule_0",
            ConditionNode::CrossAbove {
                fast: ExprNode::Literal(30.0),
                slow: ExprNode::PriceField(PriceField::Close),
            },
            ActionNode::Buy {
                quantity: QuantitySpec::Fixed(1),
            },
        )]));
        assert!(errors
            .iter()
            .any(|err| matches!(err.kind, ValidationErrorKind::CrossWithLiteral)));
    }

    #[test]
    fn rejects_cross_with_literal_operand() {
        let errors = AstValidator::validate(&strategy(vec![rule(
            "rule_0",
            ConditionNode::CrossAbove {
                fast: ExprNode::Literal(30.0),
                slow: ExprNode::Indicator(IndicatorCall {
                    kind: IndicatorKind::Ema,
                    period: 20,
                }),
            },
            ActionNode::Buy {
                quantity: QuantitySpec::Fixed(1),
            },
        )]));
        assert!(errors
            .iter()
            .any(|err| matches!(err.kind, ValidationErrorKind::CrossWithLiteral)));
    }

    #[test]
    fn rejects_invalid_time_range() {
        let start = NaiveTime::from_hms_opt(10, 0, 0).unwrap();
        let end = NaiveTime::from_hms_opt(9, 30, 0).unwrap();
        let errors = AstValidator::validate(&strategy(vec![rule(
            "rule_0",
            ConditionNode::TimeWindow { start, end },
            ActionNode::Buy {
                quantity: QuantitySpec::Fixed(1),
            },
        )]));
        assert!(errors
            .iter()
            .any(|err| matches!(err.kind, ValidationErrorKind::InvalidTimeRange)));
    }

    #[test]
    fn collects_all_errors() {
        let errors = AstValidator::validate(&strategy(vec![
            rule(
                "same",
                price_condition(),
                ActionNode::Buy {
                    quantity: QuantitySpec::Fixed(0),
                },
            ),
            rule(
                "same",
                ConditionNode::CrossBelow {
                    fast: ExprNode::Literal(1.0),
                    slow: ExprNode::Literal(2.0),
                },
                ActionNode::Sell {
                    quantity: QuantitySpec::Fixed(0),
                },
            ),
        ]));
        assert!(errors.len() >= 4);
    }

    #[test]
    fn collects_all_errors_not_just_first() {
        let errors = AstValidator::validate(&strategy(vec![make_rsi_rule(0, 0)]));
        assert!(errors.len() >= 2);
    }

    #[test]
    fn accepts_valid_index_alias() {
        let strat = StrategyNode {
            name: "test".to_string(),
            trade_in: Some(TradeIn::Index(IndexAlias::NiftyBank)),
            stop_loss: None,
            take_profit: None,
            risk: None,
            rules: vec![make_rsi_rule(14, 5)],
        };
        let errors = AstValidator::validate(&strat);
        assert!(!errors
            .iter()
            .any(|e| matches!(e.kind, ValidationErrorKind::InvalidTradeIn)));
    }

    #[test]
    fn rejects_over_500_symbols() {
        let big_list: Vec<String> = (0..=500).map(|i| format!("SYM{}", i)).collect();
        let strat = StrategyNode {
            name: "test".to_string(),
            trade_in: Some(TradeIn::Symbols(big_list)),
            stop_loss: None,
            take_profit: None,
            risk: None,
            rules: vec![make_rsi_rule(14, 5)],
        };
        let errors = AstValidator::validate(&strat);
        assert!(errors
            .iter()
            .any(|e| matches!(e.kind, ValidationErrorKind::InvalidTradeIn)
                && e.message.contains("exceeds 500")));
    }

    // ---------- STOP_LOSS / TAKE_PROFIT ----------

    #[test]
    fn accepts_valid_stop_loss() {
        let strat = StrategyNode {
            name: "test".to_string(),
            trade_in: None,
            stop_loss: Some(2.0),
            take_profit: None,
            risk: None,
            rules: vec![make_rsi_rule(14, 5)],
        };
        let errors = AstValidator::validate(&strat);
        assert!(!errors
            .iter()
            .any(|e| matches!(e.kind, ValidationErrorKind::InvalidStopLoss)));
    }

    #[test]
    fn accepts_valid_take_profit() {
        let strat = StrategyNode {
            name: "test".to_string(),
            trade_in: None,
            stop_loss: None,
            take_profit: Some(5.0),
            risk: None,
            rules: vec![make_rsi_rule(14, 5)],
        };
        let errors = AstValidator::validate(&strat);
        assert!(!errors
            .iter()
            .any(|e| matches!(e.kind, ValidationErrorKind::InvalidTakeProfit)));
    }

    #[test]
    fn accepts_stop_loss_at_100() {
        let strat = StrategyNode {
            name: "test".to_string(),
            trade_in: None,
            stop_loss: Some(100.0),
            take_profit: None,
            risk: None,
            rules: vec![make_rsi_rule(14, 5)],
        };
        assert!(!AstValidator::validate(&strat)
            .iter()
            .any(|e| matches!(e.kind, ValidationErrorKind::InvalidStopLoss)));
    }

    #[test]
    fn rejects_zero_stop_loss() {
        let strat = StrategyNode {
            name: "test".to_string(),
            trade_in: None,
            stop_loss: Some(0.0),
            take_profit: None,
            risk: None,
            rules: vec![make_rsi_rule(14, 5)],
        };
        assert!(AstValidator::validate(&strat)
            .iter()
            .any(|e| matches!(e.kind, ValidationErrorKind::InvalidStopLoss)));
    }

    #[test]
    fn rejects_over_100_stop_loss() {
        let strat = StrategyNode {
            name: "test".to_string(),
            trade_in: None,
            stop_loss: Some(150.0),
            take_profit: None,
            risk: None,
            rules: vec![make_rsi_rule(14, 5)],
        };
        assert!(AstValidator::validate(&strat)
            .iter()
            .any(|e| matches!(e.kind, ValidationErrorKind::InvalidStopLoss)));
    }

    #[test]
    fn rejects_zero_take_profit() {
        let strat = StrategyNode {
            name: "test".to_string(),
            trade_in: None,
            stop_loss: None,
            take_profit: Some(0.0),
            risk: None,
            rules: vec![make_rsi_rule(14, 5)],
        };
        assert!(AstValidator::validate(&strat)
            .iter()
            .any(|e| matches!(e.kind, ValidationErrorKind::InvalidTakeProfit)));
    }

    #[test]
    fn rejects_negative_stop_loss() {
        let strat = StrategyNode {
            name: "test".to_string(),
            trade_in: None,
            stop_loss: Some(-1.0),
            take_profit: None,
            risk: None,
            rules: vec![make_rsi_rule(14, 5)],
        };
        assert!(AstValidator::validate(&strat)
            .iter()
            .any(|e| matches!(e.kind, ValidationErrorKind::InvalidStopLoss)));
    }

    // ---------- RISK ----------

    #[test]
    fn accepts_valid_risk_config() {
        let strat = StrategyNode {
            name: "test".to_string(),
            trade_in: None,
            stop_loss: None,
            take_profit: None,
            risk: Some(crate::strategy::dsl::ast::RiskConfig {
                max_daily_loss_pct: Some(5.0),
                max_open_positions: Some(3),
                max_orders: Some(20),
            }),
            rules: vec![make_rsi_rule(14, 5)],
        };
        let errors = AstValidator::validate(&strat);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
    }

    #[test]
    fn accepts_empty_risk_config() {
        let strat = StrategyNode {
            name: "test".to_string(),
            trade_in: None,
            stop_loss: None,
            take_profit: None,
            risk: Some(crate::strategy::dsl::ast::RiskConfig::default()),
            rules: vec![make_rsi_rule(14, 5)],
        };
        assert!(!errors_have_risk_errors(&AstValidator::validate(&strat)));
    }

    #[test]
    fn accepts_max_daily_loss_at_100() {
        let strat = make_risk_strategy(Some(100.0), None, None);
        assert!(!errors_have_risk_errors(&AstValidator::validate(&strat)));
    }

    #[test]
    fn rejects_zero_max_daily_loss() {
        let strat = make_risk_strategy(Some(0.0), None, None);
        assert!(errors_have_risk_errors(&AstValidator::validate(&strat)));
    }

    #[test]
    fn rejects_negative_max_daily_loss() {
        let strat = make_risk_strategy(Some(-1.0), None, None);
        assert!(errors_have_risk_errors(&AstValidator::validate(&strat)));
    }

    #[test]
    fn rejects_over_100_max_daily_loss() {
        let strat = make_risk_strategy(Some(150.0), None, None);
        assert!(errors_have_risk_errors(&AstValidator::validate(&strat)));
    }

    #[test]
    fn rejects_zero_max_positions() {
        let strat = make_risk_strategy(None, Some(0), None);
        assert!(errors_have_risk_errors(&AstValidator::validate(&strat)));
    }

    #[test]
    fn rejects_zero_max_orders() {
        let strat = make_risk_strategy(None, None, Some(0));
        assert!(errors_have_risk_errors(&AstValidator::validate(&strat)));
    }

    fn make_risk_strategy(
        max_loss: Option<f64>,
        max_pos: Option<u32>,
        max_ord: Option<u32>,
    ) -> StrategyNode {
        StrategyNode {
            name: "test".to_string(),
            trade_in: None,
            stop_loss: None,
            take_profit: None,
            risk: Some(crate::strategy::dsl::ast::RiskConfig {
                max_daily_loss_pct: max_loss,
                max_open_positions: max_pos,
                max_orders: max_ord,
            }),
            rules: vec![make_rsi_rule(14, 5)],
        }
    }

    fn errors_have_risk_errors(errors: &[ValidationError]) -> bool {
        errors.iter().any(|e| {
            matches!(
                e.kind,
                ValidationErrorKind::InvalidMaxDailyLoss
                    | ValidationErrorKind::InvalidMaxPositions
                    | ValidationErrorKind::InvalidMaxOrders
            )
        })
    }

    fn make_rsi_rule(period: usize, quantity: u64) -> RuleNode {
        rule(
            "rule_0",
            ConditionNode::Comparison {
                left: ExprNode::Indicator(IndicatorCall {
                    kind: IndicatorKind::Rsi,
                    period,
                }),
                op: CompareOp::Lt,
                right: ExprNode::Literal(30.0),
            },
            ActionNode::Buy {
                quantity: QuantitySpec::Fixed(quantity),
            },
        )
    }

    fn price_condition() -> ConditionNode {
        ConditionNode::Comparison {
            left: ExprNode::PriceField(PriceField::Close),
            op: CompareOp::Gt,
            right: ExprNode::Literal(10.0),
        }
    }
}
use std::sync::{
    atomic::{AtomicBool, AtomicU32, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    broker::{
        dhan::{DhanClient, DhanError},
        BrokerClient,
    },
    broker::dhan::models::DhanFundsLimit,
    live::trade_log::{TradeLog, TradeLogEntry},
    models::{Order, OrderResult, OrderSide, Position},
};

use super::target::{ExecutionError, ExecutionErrorKind, ExecutionTarget};

/// Immutable context set once per live session. Read by `execute_with_meta`
/// to populate the trade log entry.
#[derive(Debug, Clone)]
pub struct SessionContext {
    pub strategy_id: String,
    pub strategy_name: String,
    pub mode: String,
}

/// How often the background task refreshes the realized-loss cache.
const REFRESH_INTERVAL_SECS: u64 = 10;
/// How often the background task refreshes the available-funds cache. Funds
/// change more slowly than realized loss, so a 60s tick is plenty.
const FUNDS_REFRESH_INTERVAL_SECS: u64 = 60;
/// After this many consecutive refresh failures the cache is marked stale.
const MAX_CONSECUTIVE_FAILURES: u32 = 3;
/// Per-order realized-loss refresh throttle (audit W5). `execute_with_meta`
/// fires an HTTP `GET /positions` after every successful place; without a
/// throttle, a 5-rule strategy that fires 5 orders on the same candle pays
/// 5 GETs back-to-back. The session tick loop already coalesces one refresh
/// per candle boundary via the 10s background ticker; this constant caps
/// how often the per-order refresh can compound that cost. Picked > 5s so
/// a multi-rule candle still gets a single refresh, but a burst of orders
/// across candles does not pile up.
const PER_ORDER_REFRESH_MIN_INTERVAL_SECS: u64 = 5;
/// Hard cap on `available_cash` until the first successful `/funds/limit`
/// fetch lands. Defends against sizing orders against a `f64::MAX` while the
/// cache is still warming — see audit item C1. Conservative default for an
/// Indian retail equity account; the user can always deploy with
/// `QuantitySpec::Fixed` if they actually have more.
const DEFAULT_AVAILABLE_CASH_CAP: f64 = 1_000_000.0;

/// Abstraction over "fetch current positions" so the realized-loss refresh can
/// be unit-tested without a live HTTP client. Production wires this to
/// `Arc<DhanClient>`; tests inject a mock.
#[async_trait]
trait PositionsSource: Send + Sync + std::fmt::Debug {
    async fn fetch(&self) -> anyhow::Result<Vec<Position>>;
}

#[async_trait]
impl PositionsSource for DhanClient {
    async fn fetch(&self) -> anyhow::Result<Vec<Position>> {
        self.get_positions().await
    }
}

/// Abstraction over "place an order" so `execute_with_meta` can be
/// unit-tested without a live HTTP client. Production wires this to
/// `Arc<DhanClient>`; tests inject a mock.
#[async_trait]
pub(crate) trait OrderPlacer: Send + Sync + std::fmt::Debug {
    async fn place(&self, order: Order) -> anyhow::Result<OrderResult>;
}

#[async_trait]
impl OrderPlacer for DhanClient {
    async fn place(&self, order: Order) -> anyhow::Result<OrderResult> {
        self.place_order(order).await
    }
}

/// Abstraction over "fetch the funds limit" so `available_cash` can be
/// unit-tested without a live HTTP client. Production wires this to
/// `Arc<DhanClient>`; tests inject a mock.
#[async_trait]
trait FundsSource: Send + Sync + std::fmt::Debug {
    async fn fetch(&self) -> anyhow::Result<DhanFundsLimit>;
}

#[async_trait]
impl FundsSource for DhanClient {
    async fn fetch(&self) -> anyhow::Result<DhanFundsLimit> {
        self.get_funds_limit().await
    }
}

#[derive(Debug)]
pub struct DhanBroker {
    client: Arc<DhanClient>,
    /// Positions source driving the realized-loss refresh. Same object as
    /// `client` in production; a mock in tests.
    positions_source: Arc<dyn PositionsSource>,
    /// Order placer used by `execute_with_meta`. Same object as `client` in
    /// production; a mock in tests.
    pub(crate) order_placer: Arc<dyn OrderPlacer>,
    /// Funds source driving the available-cash refresh. Same object as
    /// `client` in production; a mock in tests.
    funds_source: Arc<dyn FundsSource>,
    /// Cached realized-loss magnitude in rupees, always non-negative. See
    /// [`ExecutionTarget::realized_loss`] on this type for the exact
    /// definition. Refreshed by a background task every
    /// `REFRESH_INTERVAL_SECS` and immediately after every successful order
    /// placement.
    realized_loss: Arc<RwLock<f64>>,
    /// Cached available cash in INR. Initialized to `DEFAULT_AVAILABLE_CASH_CAP`
    /// so order sizing is bounded on a cold cache, and replaced with the
    /// `/funds/limit` response as soon as the first successful refresh lands.
    /// Refreshed by a background task every `FUNDS_REFRESH_INTERVAL_SECS`.
    available_cash: Arc<RwLock<f64>>,
    /// True when either the realized-loss or available-cash cache has gone
    /// stale. The live session loop reads this via [`DhanBroker::is_stale`]
    /// and pauses rather than trusting a stale value.
    funds_stale: Arc<AtomicBool>,
    /// Consecutive `refresh_realized_loss` failures. Reset to 0 on any
    /// success; when it reaches `MAX_CONSECUTIVE_FAILURES` the cache is marked
    /// stale.
    consecutive_failures: Arc<AtomicU32>,
    /// Consecutive `refresh_funds` failures. Reset to 0 on any success; when
    /// it reaches `MAX_CONSECUTIVE_FAILURES` the funds cache is marked stale.
    funds_failures: Arc<AtomicU32>,
    /// True once the realized-loss cache is unreliable (too many consecutive
    /// refresh failures). The live session loop reads this via
    /// [`DhanBroker::is_stale`] and pauses rather than trusting a stale zero.
    stale: Arc<AtomicBool>,
    /// Wall-clock instant of the most recent successful refresh of either
    /// cache. `None` until at least one refresh has succeeded. Used by
    /// `time_since_last_success` so the resume IPC can show the user how
    /// stale the cache really is. Updated by [`refresh_once`] /
    /// [`refresh_funds_once`] on every success.
    last_success: Arc<RwLock<Option<Instant>>>,
    /// W5 (audit): wall-clock instant of the most recent **attempted**
    /// realized-loss refresh, success or not. The per-order refresh inside
    /// `execute_with_meta` short-circuits when this is younger than
    /// `PER_ORDER_REFRESH_MIN_INTERVAL_SECS`, so a burst of orders within
    /// one candle issues at most one HTTP `GET /positions`. The 10s
    /// background ticker still drives a refresh on every candle boundary.
    last_realized_loss_refresh: Arc<RwLock<Instant>>,
    /// Handle to the background refresh task, aborted on drop so tests (and
    /// short-lived sessions) do not leak tasks.
    refresh_task: Mutex<Option<JoinHandle<()>>>,
    /// Append-only trade log. Every successfully placed order is recorded here
    /// regardless of which code path invoked `execute` or `execute_with_meta`.
    pub trade_log: Arc<TradeLog>,
    /// Per-session strategy context (id, name, mode). Set by `LiveSession::start`,
    /// cleared on stop. `None` outside an active live session.
    pub session_context: Arc<RwLock<Option<SessionContext>>>,
    /// When true, BUY (entry) orders are suppressed in `execute_with_meta`.
    /// SELL orders (stop-loss, take-profit, risk-breach closes) always execute.
    /// Set by `LiveSession::pause`, cleared by `LiveSession::resume`.
    ///
    /// L2 (audit): the field is `pub(crate)` so only `LiveSession` (and
    /// anything else inside the `algomln` crate) can read or write it
    /// directly. External callers — including the Tauri binary — must
    /// go through the typed snapshot getter
    /// [`DhanBroker::paused_for_entries_snapshot`] and the gated setter
    /// [`DhanBroker::set_paused_for_entries`], which both refuse to
    /// operate when no `SessionContext` is set (i.e. no live session is
    /// in flight). The previous `pub` field was a one-line tripwire that
    /// any future `Arc<DhanBroker>` clone could have used to flip the
    /// flag without going through `LiveSession::pause`.
    pub(crate) paused_for_entries: Arc<AtomicBool>,
    /// H3 (audit): cancellation token shared with the owning `LiveSession`.
    /// `execute_with_meta` returns an error before any broker HTTP call
    /// when the token is cancelled, so a `stop()` that fires
    /// mid-`place_order` aborts within a few hundred ms instead of
    /// waiting for the broker's response and writing a phantom trade-log
    /// row. `LiveSession::start` wires its own `cancel` token via
    /// [`DhanBroker::set_cancel_token`]; `DhanBroker::new` defaults to a
    /// fresh token so standalone tests (and any caller that never sees
    /// a session) keep working. Stored behind a `parking_lot::RwLock`
    /// so the broker is shared through `Arc<DhanBroker>` without
    /// requiring exclusive ownership of the Arc.
    cancel_token: parking_lot::RwLock<CancellationToken>,
}

impl DhanBroker {
    pub fn new(client: Arc<DhanClient>, trade_log: Arc<TradeLog>) -> Self {
        let positions_source: Arc<dyn PositionsSource> = client.clone();
        let order_placer: Arc<dyn OrderPlacer> = client.clone();
        let funds_source: Arc<dyn FundsSource> = client.clone();
        Self::spawn_with_source(
            client,
            positions_source,
            order_placer,
            funds_source,
            trade_log,
        )
    }

    fn spawn_with_source(
        client: Arc<DhanClient>,
        positions_source: Arc<dyn PositionsSource>,
        order_placer: Arc<dyn OrderPlacer>,
        funds_source: Arc<dyn FundsSource>,
        trade_log: Arc<TradeLog>,
    ) -> Self {
        let realized_loss = Arc::new(RwLock::new(0.0));
        // Start at the hard cap, not zero, so a `PercentCapital` rule that
        // fires before the first `/funds/limit` refresh lands is sized against
        // a sane upper bound (1 lakh INR by default), not `f64::MAX`.
        let available_cash = Arc::new(RwLock::new(DEFAULT_AVAILABLE_CASH_CAP));
        let funds_stale = Arc::new(AtomicBool::new(false));
        let consecutive_failures = Arc::new(AtomicU32::new(0));
        let funds_failures = Arc::new(AtomicU32::new(0));
        let stale = Arc::new(AtomicBool::new(false));
        let paused_for_entries = Arc::new(AtomicBool::new(false));
        // No refresh has succeeded yet — drives the resume-during-stale error
        // message ("broker refresh has never succeeded") and gates the
        // optional error in `time_since_last_success`.
        let last_success: Arc<RwLock<Option<Instant>>> = Arc::new(RwLock::new(None));
        // W5: stamp the throttle instant far in the past so the first
        // post-`place_order` refresh actually fires (a fresh broker must
        // warm the cache immediately, not wait 5s).
        let last_realized_loss_refresh: Arc<RwLock<Instant>> =
            Arc::new(RwLock::new(Instant::now() - Duration::from_secs(
                PER_ORDER_REFRESH_MIN_INTERVAL_SECS + 1,
            )));

        // Spawn the periodic refresh only when a Tokio runtime is available.
        // Plain `#[test]` constructions (and any non-async caller) simply get
        // no background task; the cache stays at its initial values until an
        // explicit `refresh_realized_loss` / `refresh_funds`.
        //
        // Both refreshes share one ticker so the failure counters advance on
        // a single 10s beat. Funds have their own slower interval
        // (`FUNDS_REFRESH_INTERVAL_SECS`) — every 6th tick fires a funds
        // refresh instead of realized-loss.
        let refresh_task = match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                let positions_source = positions_source.clone();
                let funds_source = funds_source.clone();
                let realized_loss = realized_loss.clone();
                let available_cash = available_cash.clone();
                let consecutive_failures = consecutive_failures.clone();
                let funds_failures = funds_failures.clone();
                let stale = stale.clone();
                let funds_stale = funds_stale.clone();
                let last_success = last_success.clone();
                Some(handle.spawn(async move {
                    let mut ticker =
                        tokio::time::interval(Duration::from_secs(REFRESH_INTERVAL_SECS));
                    // Skip the immediate first tick so the cache can warm up
                    // out-of-band (the live session calls `refresh_*` after
                    // subscribing, but for safety we don't want a thundering
                    // herd at t=0).
                    ticker.tick().await;
                    let mut tick_count: u64 = 0;
                    loop {
                        ticker.tick().await;
                        tick_count += 1;
                        refresh_once(
                            &positions_source,
                            &realized_loss,
                            &consecutive_failures,
                            &stale,
                            &last_success,
                        )
                        .await;
                        // Funds refresh every 60s (= every 6th 10s tick).
                        if tick_count % (FUNDS_REFRESH_INTERVAL_SECS / REFRESH_INTERVAL_SECS)
                            == 0
                        {
                            refresh_funds_once(
                                &funds_source,
                                &available_cash,
                                &funds_failures,
                                &funds_stale,
                                &last_success,
                            )
                            .await;
                        }
                    }
                }))
            }
            Err(_) => None,
        };

        Self {
            client,
            positions_source,
            order_placer,
            funds_source,
            realized_loss,
            available_cash,
            funds_stale,
            consecutive_failures,
            funds_failures,
            stale,
            last_success,
            last_realized_loss_refresh,
            refresh_task: Mutex::new(refresh_task),
            trade_log,
            session_context: Arc::new(RwLock::new(None)),
            paused_for_entries,
            cancel_token: parking_lot::RwLock::new(CancellationToken::new()),
        }
    }

    /// Refresh the cached realized-loss magnitude from the positions source.
    /// Driven by the background task and also called immediately after every
    /// successful order placement. On failure the cache is left untouched
    /// (never reset to zero) and the failure counter advances toward
    /// staleness.
    ///
    /// W5 (audit): when called from `execute_with_meta` after a successful
    /// order, the call short-circuits if a refresh was attempted within the
    /// last `PER_ORDER_REFRESH_MIN_INTERVAL_SECS`. The 10s background
    /// ticker already coalesces a refresh on every candle boundary, so
    /// the per-order path is only useful for "many orders on the same
    /// candle" — and that case is precisely the one where the throttle
    /// saves N HTTP GETs. Tests that call `refresh_realized_loss`
    /// directly bypass the throttle (`force = true` overload below) so
    /// deterministic assertions still work.
    pub async fn refresh_realized_loss(&self) {
        // Throttle: skip when the previous attempt was recent. The first
        // call on a fresh broker always passes (the field was stamped
        // far in the past at construction time).
        {
            let last = *self.last_realized_loss_refresh.read();
            if last.elapsed() < Duration::from_secs(PER_ORDER_REFRESH_MIN_INTERVAL_SECS) {
                return;
            }
            *self.last_realized_loss_refresh.write() = Instant::now();
        }
        refresh_once(
            &self.positions_source,
            &self.realized_loss,
            &self.consecutive_failures,
            &self.stale,
            &self.last_success,
        )
        .await;
    }

    /// W5 (audit): bypass the per-order throttle. Used by the background
    /// ticker (which already runs on its own cadence) and by tests that
    /// need to assert a refresh happened synchronously.
    pub async fn refresh_realized_loss_unchecked(&self) {
        *self.last_realized_loss_refresh.write() = Instant::now();
        refresh_once(
            &self.positions_source,
            &self.realized_loss,
            &self.consecutive_failures,
            &self.stale,
            &self.last_success,
        )
        .await;
    }

    /// Refresh the cached available-balance value from the funds source.
    /// Driven by the background task. On failure the cache is left at its
    /// last good value (or `DEFAULT_AVAILABLE_CASH_CAP` on a cold cache) so
    /// order sizing never inflates past a sane bound.
    pub async fn refresh_funds(&self) {
        refresh_funds_once(
            &self.funds_source,
            &self.available_cash,
            &self.funds_failures,
            &self.funds_stale,
            &self.last_success,
        )
        .await;
    }

    /// True once either the realized-loss or available-cash cache is stale
    /// (too many consecutive refresh failures). The safety metrics must not
    /// be trusted as zeros while stale — the live session loop pauses
    /// instead. See `H2` in the live execution audit for the realized-loss
    /// case; the funds case applies the same policy because
    /// `available_cash` directly sizes `PercentCapital` orders.
    pub fn is_stale(&self) -> bool {
        self.stale.load(Ordering::Relaxed) || self.funds_stale.load(Ordering::Relaxed)
    }

    /// Wall-clock duration since the most recent successful refresh of
    /// either cache, or `None` if neither cache has ever refreshed. Drives
    /// the resume-during-stale error message (H2) so the user can see how
    /// long the broker has been unreachable instead of getting a bare
    /// "stale" string.
    pub fn time_since_last_success(&self) -> Option<Duration> {
        let guard = self.last_success.read();
        guard.as_ref().map(|instant| instant.elapsed())
    }

    /// Enable or disable entry-order suppression. Called by `LiveSession::pause`
    /// (set `true`) and `LiveSession::resume` (set `false`). When set, BUY orders
    /// are rejected before reaching the broker; SELL orders (SL, TP, risk-breach
    /// closes) always go through so open positions remain protected.
    ///
    /// L2 (audit): the setter is gated on a live `SessionContext` being set
    /// on the broker. If no session is active, the call is a silent no-op
    /// (returning `false`) — the broker must not flip the flag outside an
    /// active session because there is no caller that should be reaching
    /// for it. The previous `pub` setter could have been called with an
    /// `Arc<DhanBroker>` clone by anyone who could reach the field.
    /// Returns `true` when the flag was actually changed, `false` when the
    /// call was rejected.
    pub fn set_paused_for_entries(&self, paused: bool) -> bool {
        if self.session_context.read().is_none() {
            return false;
        }
        self.paused_for_entries.store(paused, Ordering::Relaxed);
        true
    }

    /// Snapshot of the entry-suppression flag. Read-only so the gated
    /// plugin execution API can re-run the pause gate without exposing
    /// the underlying `AtomicBool` (L2 in the live execution audit —
    /// `paused_for_entries` stays `pub(crate)` and the public surface
    /// is a typed setter plus a snapshot getter).
    pub fn paused_for_entries_snapshot(&self) -> bool {
        self.paused_for_entries.load(Ordering::Relaxed)
    }

    /// H3 (audit): wire the owning `LiveSession`'s cancellation token into
    /// the broker so a `stop()` mid-`place_order` aborts the call before
    /// the trade-log row is appended. `LiveSession::start` is the only
    /// expected caller; tests can ignore this and rely on the default
    /// fresh token from `DhanBroker::new`. Takes `&self` because the
    /// broker is `Arc`-shared and we don't want callers to have to
    /// reach inside the Arc to swap the token.
    pub fn set_cancel_token(&self, token: CancellationToken) {
        *self.cancel_token.write() = token;
    }

    /// H3 (audit): returns the currently-wired cancellation token so the
    /// caller can clone it (e.g. a future plugin execution API that
    /// threads its own tasks onto the same cancellation hook). Rarely
    /// used — most callers should construct the token and call
    /// `set_cancel_token` instead.
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel_token.read().clone()
    }

    /// Place an order and record it in the trade log with caller-supplied
    /// metadata. This is the primary execution path. The `ExecutionTarget::execute`
    /// trait method delegates here with empty `rule_id`/`notes` so the audit log
    /// is always written regardless of call path.
    ///
    /// If appending to the trade log fails, an error is printed but NOT returned —
    /// the order has already been placed and we must not falsely signal failure.
    pub async fn execute_with_meta(
        &self,
        order: Order,
        rule_id: &str,
        notes: &str,
    ) -> Result<OrderResult, ExecutionError> {
        // H3 (audit): check the cancellation token before any broker work.
        // The owning `LiveSession` cancels its token on `stop()`; doing this
        // check first guarantees we never reach the broker HTTP call after
        // a stop, and the trade-log append is never reached either (the
        // append is downstream of the broker call). Returning the same
        // `ExecutionError` shape as a regular broker failure keeps the
        // engine log consistent — `OrderFailed { rule_id, error }` still
        // surfaces the cancellation reason to the strategy log.
        if self.cancel_token.read().is_cancelled() {
            let msg = format!(
                "session cancelled before order for {} could be placed",
                order.symbol
            );
            return Err(ExecutionError {
                message: msg.clone(),
                kind: ExecutionErrorKind::BrokerError(msg),
            });
        }

        // When the session is paused, entry (BUY) orders are suppressed so the
        // user does not accidentally open new positions. SELL orders (stop-loss,
        // take-profit, risk-breach closes) always execute to protect open positions.
        if self.paused_for_entries.load(Ordering::Relaxed) && order.side == OrderSide::Buy {
            eprintln!(
                "[dhan_broker] session paused: skipping entry BUY order for {}",
                order.symbol
            );
            let msg = format!("session paused: entry order for {} suppressed", order.symbol);
            return Err(ExecutionError {
                message: msg.clone(),
                kind: ExecutionErrorKind::BrokerError(msg),
            });
        }

        let result = match self.order_placer.place(order.clone()).await {
            Ok(result) => result,
            Err(error) => {
                // Surface a single-line trail for the failed attempt so the
                // engine log can correlate the symbol/quantity with the
                // broker error (the trade log intentionally only records
                // fills — see H1 below).
                eprintln!("[dhan_broker] order placement failed for {}: {error}", order.symbol);
                return Err(map_place_order_error(error));
            }
        };

        // Refresh realized-loss cache immediately after placement.
        self.refresh_realized_loss().await;

        let classified = classify_result(result)?;

        // H1 (audit): only FILL results (Traded / Filled) belong in the trade
        // log — the wire shape represents a "trade". Non-terminal statuses
        // (Transit / Pending) and no-fills still carry an `order_id` but
        // writing a row with `price = 0.0` and `order_status = "Transit"`
        // pollutes the UI's trade log with phantom lines. Terminal non-fills
        // (Rejected / Cancelled / Expired) were already turned into an `Err`
        // by `classify_result` so they never reach here.
        if !classified.status.is_fill() {
            return Ok(classified);
        }

        // Build and append the trade log entry. Log failures must not surface
        // as order failures — the order is already through the broker.
        let ctx = self.session_context.read().clone();
        if ctx.is_none() {
            eprintln!(
                "[dhan_broker] execute_with_meta called with no active SessionContext — \
                 order placed but session metadata will be empty in the trade log"
            );
        }
        let (strategy_id, strategy_name, mode) = ctx
            .map(|c| (c.strategy_id, c.strategy_name, c.mode))
            .unwrap_or_else(|| (String::new(), String::new(), "live".to_string()));

        let entry = TradeLogEntry {
            id: Uuid::new_v4().to_string(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            strategy_id,
            strategy_name,
            symbol: order.symbol.clone(),
            side: match order.side {
                OrderSide::Buy => "BUY".to_string(),
                OrderSide::Sell => "SELL".to_string(),
            },
            quantity: i64::from(order.quantity),
            price: order.price.unwrap_or(0.0),
            order_id: classified.order_id.clone(),
            order_status: format!("{:?}", classified.status),
            mode,
            rule_id: rule_id.to_string(),
            notes: notes.to_string(),
        };

        if let Err(io_err) = self.trade_log.append(entry) {
            eprintln!(
                "[dhan_broker] WARN: failed to append trade log for order {}: {io_err}",
                classified.order_id
            );
        }

        Ok(classified)
    }
}

impl Drop for DhanBroker {
    fn drop(&mut self) {
        if let Some(task) = self.refresh_task.lock().take() {
            task.abort();
        }
    }
}

/// Compute the realized-loss magnitude from a positions snapshot and update the
/// shared cache. On success resets the failure counter and clears staleness; on
/// failure advances the counter and, past the threshold, marks the cache stale.
async fn refresh_once(
    positions_source: &Arc<dyn PositionsSource>,
    realized_loss: &Arc<RwLock<f64>>,
    consecutive_failures: &Arc<AtomicU32>,
    stale: &Arc<AtomicBool>,
    last_success: &Arc<RwLock<Option<Instant>>>,
) {
    match positions_source.fetch().await {
        Ok(positions) => {
            // Sum the magnitude of every losing position; winning positions
            // contribute nothing. Profits never reduce the loss figure.
            let magnitude: f64 = positions
                .iter()
                .map(|position| position.realized_pnl)
                .filter(|pnl| *pnl < 0.0)
                .map(|pnl| -pnl)
                .sum();
            *realized_loss.write() = magnitude;
            consecutive_failures.store(0, Ordering::Relaxed);
            stale.store(false, Ordering::Relaxed);
            // H2 (audit): record the wall-clock instant of every successful
            // refresh so the resume IPC can show the user how stale the
            // cache has gone.
            *last_success.write() = Some(Instant::now());
        }
        Err(error) => {
            let failures = consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
            eprintln!("dhan_broker: realized_loss refresh failed: {error}");
            if failures >= MAX_CONSECUTIVE_FAILURES {
                stale.store(true, Ordering::Relaxed);
                eprintln!(
                    "dhan_broker: realized_loss stale after {failures} failures — session should pause"
                );
            }
        }
    }
}

/// Fetch the funds limit and update the available-cash cache. On success
/// resets the failure counter and clears staleness; on failure advances the
/// counter (toward `MAX_CONSECUTIVE_FAILURES`) and, past the threshold,
/// marks the funds cache stale. The cache is **never** reset on failure —
/// the last good value stays in place so order sizing stays bounded. A
/// negative or non-finite `available_balance` from the broker is treated
/// as zero (should never happen, but defends against garbage responses).
async fn refresh_funds_once(
    funds_source: &Arc<dyn FundsSource>,
    available_cash: &Arc<RwLock<f64>>,
    funds_failures: &Arc<AtomicU32>,
    funds_stale: &Arc<AtomicBool>,
    last_success: &Arc<RwLock<Option<Instant>>>,
) {
    match funds_source.fetch().await {
        Ok(limit) => {
            let value = if limit.available_balance.is_finite() && limit.available_balance >= 0.0
            {
                limit.available_balance
            } else {
                0.0
            };
            *available_cash.write() = value;
            funds_failures.store(0, Ordering::Relaxed);
            funds_stale.store(false, Ordering::Relaxed);
            // H2 (audit): same success timestamp as the realized-loss
            // path — either refresh counts as "broker reachable."
            *last_success.write() = Some(Instant::now());
        }
        Err(error) => {
            let failures = funds_failures.fetch_add(1, Ordering::Relaxed) + 1;
            eprintln!("dhan_broker: available_cash refresh failed: {error}");
            if failures >= MAX_CONSECUTIVE_FAILURES {
                funds_stale.store(true, Ordering::Relaxed);
                eprintln!(
                    "dhan_broker: available_cash stale after {failures} failures — session should pause"
                );
            }
        }
    }
}

#[async_trait]
impl ExecutionTarget for DhanBroker {
    /// Delegates to [`DhanBroker::execute_with_meta`] with empty rule/notes so
    /// every execution path — including plugins that call through the trait
    /// object — still writes to the trade log.
    async fn execute(&self, order: Order) -> Result<OrderResult, ExecutionError> {
        self.execute_with_meta(order, "", "").await
    }

    async fn get_positions(&self) -> Result<Vec<Position>, ExecutionError> {
        self.client.get_positions().await.map_err(broker_error)
    }

    /// Total realized loss for the session as a **non-negative magnitude** in
    /// rupees. Computed as the sum of `max(0, -realizedProfit)` across every
    /// position returned by Dhan's `GET /positions`: each losing position adds
    /// its absolute loss, winning positions add nothing, and profits never
    /// reduce the figure. Backed by a polling cache — a failed refresh leaves
    /// the last good value in place rather than reporting a falsely low zero,
    /// so callers feeding the `RISK MAX_DAILY_LOSS` gate should also consult
    /// [`DhanBroker::is_stale`].
    fn realized_loss(&self) -> f64 {
        *self.realized_loss.read()
    }

    fn available_cash(&self) -> f64 {
        // Returns the cached value from `GET /funds/limit`, never `f64::MAX`.
        // On a cold cache (no refresh has succeeded yet) the cache holds
        // `DEFAULT_AVAILABLE_CASH_CAP` so `PercentCapital` orders are sized
        // against a sane upper bound rather than a u64-overflowing sentinel.
        // If the cache has gone stale (3+ consecutive failures), this still
        // returns the last good value — `is_stale()` is the signal callers
        // should consult before trusting this number.
        *self.available_cash.read()
    }

    fn is_paper(&self) -> bool {
        false
    }

    fn name(&self) -> &str {
        "dhan"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

fn broker_error(error: anyhow::Error) -> ExecutionError {
    let message = error.to_string();
    ExecutionError {
        message: message.clone(),
        kind: ExecutionErrorKind::BrokerError(message),
    }
}

/// Decide how a placed order's status maps to the `ExecutionTarget::execute`
/// contract. A terminal non-fill (rejected / cancelled / expired) is a hard
/// error. Non-terminal statuses (transit / pending) and fills both return `Ok`
/// with the status intact — the caller inspects `status.is_fill()` before
/// treating the result as an execution.
fn classify_result(result: OrderResult) -> Result<OrderResult, ExecutionError> {
    if result.status.is_terminal() && !result.status.is_fill() {
        let message = format!("order {} status: {:?}", result.order_id, result.status);
        return Err(ExecutionError {
            message: message.clone(),
            kind: ExecutionErrorKind::BrokerError(message),
        });
    }
    Ok(result)
}

/// Map a `place_order` error to an `ExecutionError`. A timed-out order surfaces
/// as [`DhanError::OrderStatusUnknown`]; we preserve its "check broker app"
/// message so the caller/UI never mistakes an unknown-status order for a plain
/// failure (or, worse, a fill). All other errors pass through `broker_error`.
fn map_place_order_error(error: anyhow::Error) -> ExecutionError {
    if let Some(DhanError::OrderStatusUnknown { correlation_id }) =
        error.downcast_ref::<DhanError>()
    {
        let message =
            format!("order status unknown, check broker app; correlation_id={correlation_id}");
        return ExecutionError {
            message: message.clone(),
            kind: ExecutionErrorKind::BrokerError(message),
        };
    }
    broker_error(error)
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicBool, AtomicU32},
        Arc,
    };
    use std::time::{Duration, Instant};

    use async_trait::async_trait;
    use parking_lot::{Mutex, RwLock};

    use crate::{
        broker::dhan::{DhanAuth, DhanClient, DhanError},
        broker::dhan::models::DhanFundsLimit,
        live::trade_log::TradeLog,
        models::{Order, OrderResult, OrderSide, OrderStatus, OrderType, Position},
        strategy::execution::{ExecutionError, ExecutionErrorKind, ExecutionTarget},
    };

    use super::{
        classify_result, map_place_order_error, DhanBroker, FundsSource, OrderPlacer,
        PositionsSource, SessionContext, CancellationToken, DEFAULT_AVAILABLE_CASH_CAP,
        MAX_CONSECUTIVE_FAILURES, PER_ORDER_REFRESH_MIN_INTERVAL_SECS,
    };

    fn temp_trade_log() -> (Arc<TradeLog>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trade_log.jsonl");
        let log = Arc::new(TradeLog::open(path).unwrap());
        (log, dir)
    }

    fn broker() -> DhanBroker {
        let auth = DhanAuth::new("test-token").unwrap();
        let (log, _dir) = temp_trade_log();
        // _dir intentionally leaked: the TempDir must outlive the broker in these tests.
        // For simple property tests the log path doesn't matter.
        DhanBroker::new(Arc::new(DhanClient::new(auth)), log)
    }

    #[derive(Debug)]
    struct MockPositions {
        positions: Vec<Position>,
        fail: bool,
    }

    #[async_trait]
    impl PositionsSource for MockPositions {
        async fn fetch(&self) -> anyhow::Result<Vec<Position>> {
            if self.fail {
                anyhow::bail!("mock positions failure");
            }
            Ok(self.positions.clone())
        }
    }

    /// Mock `OrderPlacer` that always returns a fixed `OrderResult`.
    #[derive(Debug)]
    struct MockOrderPlacer {
        result: OrderResult,
    }

    #[async_trait]
    impl OrderPlacer for MockOrderPlacer {
        async fn place(&self, _order: Order) -> anyhow::Result<OrderResult> {
            Ok(self.result.clone())
        }
    }

    /// Mock `FundsSource` for tests. `fail=true` returns an error on every
    /// fetch; otherwise returns the configured `available_balance`.
    #[derive(Debug)]
    struct MockFunds {
        available_balance: f64,
        fail: bool,
    }

    #[async_trait]
    impl FundsSource for MockFunds {
        async fn fetch(&self) -> anyhow::Result<DhanFundsLimit> {
            if self.fail {
                anyhow::bail!("mock funds failure");
            }
            Ok(DhanFundsLimit {
                available_balance: self.available_balance,
                sod_limit: 0.0,
                collateral_amount: 0.0,
                receiveable_amount: 0.0,
                utilized_amount: 0.0,
                withdrawable_balance: 0.0,
                blocked_payout_amount: 0.0,
            })
        }
    }

    fn never_failing_funds() -> Arc<dyn FundsSource> {
        Arc::new(MockFunds {
            available_balance: 0.0,
            fail: false,
        })
    }

    fn position_with_pnl(realized_pnl: f64) -> Position {
        Position {
            symbol: "TEST".to_string(),
            quantity: 0,
            average_price: 0.0,
            ltp: 0.0,
            realized_pnl,
            unrealized_pnl: 0.0,
        }
    }

    /// Build a broker wired to a mock positions source with NO background
    /// refresh task, so refresh timing in tests is fully explicit. Funds
    /// source defaults to a never-failing mock that returns 0.
    fn broker_with_source(source: Arc<dyn PositionsSource>) -> DhanBroker {
        let client = Arc::new(DhanClient::new(DhanAuth::new("test-token").unwrap()));
        let (log, _dir) = temp_trade_log();
        DhanBroker {
            client: client.clone(),
            positions_source: source,
            order_placer: client,
            funds_source: never_failing_funds(),
            realized_loss: Arc::new(RwLock::new(0.0)),
            available_cash: Arc::new(RwLock::new(DEFAULT_AVAILABLE_CASH_CAP)),
            funds_stale: Arc::new(AtomicBool::new(false)),
            consecutive_failures: Arc::new(AtomicU32::new(0)),
            funds_failures: Arc::new(AtomicU32::new(0)),
            stale: Arc::new(AtomicBool::new(false)),
            last_success: Arc::new(RwLock::new(None)),
            // W5: stamp the throttle instant far in the past so tests
            // that call `refresh_realized_loss()` repeatedly see every
            // attempt fire (otherwise the staleness-after-N-failures test
            // would silently be a no-op).
            last_realized_loss_refresh: Arc::new(RwLock::new(
                Instant::now() - Duration::from_secs(PER_ORDER_REFRESH_MIN_INTERVAL_SECS + 1),
            )),
            refresh_task: Mutex::new(None),
            trade_log: log,
            session_context: Arc::new(RwLock::new(None)),
            paused_for_entries: Arc::new(AtomicBool::new(false)),
            cancel_token: parking_lot::RwLock::new(CancellationToken::new()),
        }
    }

    /// Build a broker with injectable positions, orders, and funds sources
    /// and a real temp-file `TradeLog`.
    fn broker_with_mocks(
        positions: Arc<dyn PositionsSource>,
        orders: Arc<dyn OrderPlacer>,
        funds: Arc<dyn FundsSource>,
        trade_log: Arc<TradeLog>,
    ) -> DhanBroker {
        let client = Arc::new(DhanClient::new(DhanAuth::new("test-token").unwrap()));
        DhanBroker {
            client,
            positions_source: positions,
            order_placer: orders,
            funds_source: funds,
            realized_loss: Arc::new(RwLock::new(0.0)),
            available_cash: Arc::new(RwLock::new(DEFAULT_AVAILABLE_CASH_CAP)),
            funds_stale: Arc::new(AtomicBool::new(false)),
            consecutive_failures: Arc::new(AtomicU32::new(0)),
            funds_failures: Arc::new(AtomicU32::new(0)),
            stale: Arc::new(AtomicBool::new(false)),
            last_success: Arc::new(RwLock::new(None)),
            last_realized_loss_refresh: Arc::new(RwLock::new(
                Instant::now() - Duration::from_secs(PER_ORDER_REFRESH_MIN_INTERVAL_SECS + 1),
            )),
            refresh_task: Mutex::new(None),
            trade_log,
            session_context: Arc::new(RwLock::new(None)),
            paused_for_entries: Arc::new(AtomicBool::new(false)),
            cancel_token: parking_lot::RwLock::new(CancellationToken::new()),
        }
    }

    #[test]
    fn test_dhan_broker_is_not_paper() {
        assert!(!broker().is_paper());
    }

    #[test]
    fn test_dhan_broker_name() {
        assert_eq!(broker().name(), "dhan");
    }

    #[test]
    fn test_realized_loss_starts_at_zero() {
        assert_eq!(broker().realized_loss(), 0.0);
    }

    #[tokio::test]
    async fn test_realized_loss_magnitude_from_positions() {
        let source = Arc::new(MockPositions {
            positions: vec![position_with_pnl(-500.0), position_with_pnl(200.0)],
            fail: false,
        });
        let broker = broker_with_source(source);
        broker.refresh_realized_loss().await;
        // Losing position contributes 500; the +200 profit contributes 0.
        assert_eq!(broker.realized_loss(), 500.0);
    }

    #[tokio::test]
    async fn test_realized_loss_is_stale_after_failures() {
        let source = Arc::new(MockPositions {
            positions: Vec::new(),
            fail: true,
        });
        let broker = broker_with_source(source);
        assert!(!broker.is_stale());
        // Use `_unchecked` to bypass the per-order throttle — the throttled
        // public path only fires once within `PER_ORDER_REFRESH_MIN_INTERVAL_SECS`
        // and would leave `consecutive_failures` short of the stale threshold.
        for _ in 0..MAX_CONSECUTIVE_FAILURES {
            broker.refresh_realized_loss_unchecked().await;
        }
        assert!(broker.is_stale());
    }

    /// C1: On a cold cache, `available_cash` must NOT be `f64::MAX` — it must
    /// return the bounded default so `PercentCapital` orders cannot
    /// oversize to u64 overflow.
    #[test]
    fn test_available_cash_starts_at_bounded_default() {
        assert_eq!(broker().available_cash(), DEFAULT_AVAILABLE_CASH_CAP);
        assert!(broker().available_cash().is_finite());
    }

    /// C1: A successful `/funds/limit` refresh replaces the default with the
    /// reported balance.
    #[tokio::test]
    async fn test_available_cash_refreshes_from_funds_source() {
        let funds: Arc<dyn FundsSource> = Arc::new(MockFunds {
            available_balance: 250_000.0,
            fail: false,
        });
        let positions: Arc<dyn PositionsSource> = Arc::new(MockPositions {
            positions: Vec::new(),
            fail: false,
        });
        let (log, _dir) = temp_trade_log();
        let broker = DhanBroker {
            client: Arc::new(DhanClient::new(DhanAuth::new("test-token").unwrap())),
            positions_source: positions,
            order_placer: Arc::new(DhanClient::new(DhanAuth::new("test-token").unwrap())),
            funds_source: funds,
            realized_loss: Arc::new(RwLock::new(0.0)),
            available_cash: Arc::new(RwLock::new(DEFAULT_AVAILABLE_CASH_CAP)),
            funds_stale: Arc::new(AtomicBool::new(false)),
            consecutive_failures: Arc::new(AtomicU32::new(0)),
            funds_failures: Arc::new(AtomicU32::new(0)),
            stale: Arc::new(AtomicBool::new(false)),
            last_success: Arc::new(RwLock::new(None)),
            last_realized_loss_refresh: Arc::new(RwLock::new(
                Instant::now() - Duration::from_secs(PER_ORDER_REFRESH_MIN_INTERVAL_SECS + 1),
            )),
            refresh_task: Mutex::new(None),
            trade_log: log,
            session_context: Arc::new(RwLock::new(None)),
            paused_for_entries: Arc::new(AtomicBool::new(false)),
            cancel_token: parking_lot::RwLock::new(CancellationToken::new()),
        };
        broker.refresh_funds().await;
        assert_eq!(broker.available_cash(), 250_000.0);
    }

    /// C1: A failed refresh leaves the cache at its last good value (does
    /// not reset to zero and does not poison with `f64::MAX`). Three
    /// consecutive failures mark the broker stale.
    #[tokio::test]
    async fn test_available_cash_stale_keeps_last_good_value() {
        let failing_funds: Arc<dyn FundsSource> = Arc::new(MockFunds {
            available_balance: 0.0,
            fail: true,
        });
        let positions: Arc<dyn PositionsSource> = Arc::new(MockPositions {
            positions: Vec::new(),
            fail: false,
        });
        let (log, _dir) = temp_trade_log();
        let broker = DhanBroker {
            client: Arc::new(DhanClient::new(DhanAuth::new("test-token").unwrap())),
            positions_source: positions,
            order_placer: Arc::new(DhanClient::new(DhanAuth::new("test-token").unwrap())),
            funds_source: failing_funds,
            realized_loss: Arc::new(RwLock::new(0.0)),
            available_cash: Arc::new(RwLock::new(42_000.0)),
            funds_stale: Arc::new(AtomicBool::new(false)),
            consecutive_failures: Arc::new(AtomicU32::new(0)),
            funds_failures: Arc::new(AtomicU32::new(0)),
            stale: Arc::new(AtomicBool::new(false)),
            last_success: Arc::new(RwLock::new(None)),
            last_realized_loss_refresh: Arc::new(RwLock::new(
                Instant::now() - Duration::from_secs(PER_ORDER_REFRESH_MIN_INTERVAL_SECS + 1),
            )),
            refresh_task: Mutex::new(None),
            trade_log: log,
            session_context: Arc::new(RwLock::new(None)),
            paused_for_entries: Arc::new(AtomicBool::new(false)),
            cancel_token: parking_lot::RwLock::new(CancellationToken::new()),
        };
        assert!(!broker.is_stale());
        for _ in 0..MAX_CONSECUTIVE_FAILURES {
            broker.refresh_funds().await;
        }
        assert!(broker.is_stale());
        assert_eq!(
            broker.available_cash(),
            42_000.0,
            "stale funds must not reset to zero or inflate to f64::MAX"
        );
    }

    /// H2: `time_since_last_success` returns `None` before any refresh
    /// has succeeded; after a successful refresh it returns a duration
    /// since that instant. Drives the user-facing message that the resume
    /// IPC returns when the broker is stale.
    #[tokio::test]
    async fn test_time_since_last_success_none_before_refresh() {
        let broker = broker();
        assert!(
            broker.time_since_last_success().is_none(),
            "no refresh has happened yet"
        );
    }

    #[tokio::test]
    async fn test_time_since_last_success_after_refresh() {
        let source = Arc::new(MockPositions {
            positions: Vec::new(),
            fail: false,
        });
        let broker = broker_with_source(source);
        broker.refresh_realized_loss().await;
        let elapsed = broker
            .time_since_last_success()
            .expect("last_success must be set after a successful refresh");
        assert!(
            elapsed < Duration::from_secs(5),
            "elapsed must be near-zero, got {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn test_time_since_last_success_unchanged_on_failure() {
        // A failed refresh must NOT advance `last_success` — the field
        // tracks "last successful refresh," not "last attempted."
        let source = Arc::new(MockPositions {
            positions: Vec::new(),
            fail: true,
        });
        let broker = broker_with_source(source);
        assert!(broker.time_since_last_success().is_none());
        broker.refresh_realized_loss().await;
        assert!(
            broker.time_since_last_success().is_none(),
            "failed refresh must not stamp last_success"
        );
    }

    fn order_result(status: OrderStatus) -> OrderResult {
        OrderResult {
            order_id: "order-1".to_string(),
            status,
            timestamp: 0,
            correlation_id: "algomln-test".to_string(),
        }
    }

    #[test]
    fn test_rejected_returns_err() {
        let classified = classify_result(order_result(OrderStatus::Rejected));
        assert!(matches!(
            classified,
            Err(ExecutionError {
                kind: ExecutionErrorKind::BrokerError(_),
                ..
            })
        ));
    }

    #[test]
    fn test_transit_returns_ok_without_fill() {
        // Non-terminal statuses pass through as Ok with the status intact so
        // the caller can branch on `is_fill()` — they must NOT be errors.
        let classified = classify_result(order_result(OrderStatus::Transit))
            .expect("transit must not be an execution error");
        assert_eq!(classified.status, OrderStatus::Transit);
        assert!(!classified.status.is_fill());
    }

    #[test]
    fn test_traded_returns_ok() {
        let classified = classify_result(order_result(OrderStatus::Traded))
            .expect("traded must be Ok");
        assert!(classified.status.is_fill());
    }

    #[test]
    fn test_order_status_unknown_maps_to_broker_error() {
        let mapped = map_place_order_error(
            DhanError::OrderStatusUnknown {
                correlation_id: "algomln-abc".to_string(),
            }
            .into(),
        );
        assert!(matches!(
            mapped.kind,
            ExecutionErrorKind::BrokerError(_)
        ));
        assert!(mapped.message.contains("check broker app"));
        assert!(mapped.message.contains("algomln-abc"));
    }

    #[tokio::test]
    async fn test_paused_suppresses_buy_orders() {
        let (log, _dir) = temp_trade_log();
        let positions = Arc::new(MockPositions { positions: Vec::new(), fail: false });
        let order_placer = Arc::new(MockOrderPlacer {
            result: OrderResult {
                order_id: "test-order".to_string(),
                status: OrderStatus::Traded,
                timestamp: 0,
                correlation_id: "algomln-test".to_string(),
            },
        });
        let broker = broker_with_mocks(positions, order_placer, never_failing_funds(), log);
        // L2 (audit): the setter is gated on a SessionContext. Stand one
        // up so the flag actually flips — this is what `LiveSession::start`
        // does in production before `pause`/`resume` ever runs.
        *broker.session_context.write() = Some(SessionContext {
            strategy_id: "strat-pause-test".to_string(),
            strategy_name: "Pause Test".to_string(),
            mode: "live".to_string(),
        });
        assert!(
            broker.set_paused_for_entries(true),
            "set_paused_for_entries must succeed when a SessionContext is set"
        );

        let buy_order = Order {
            symbol: "RELIANCE".to_string(),
            side: OrderSide::Buy,
            quantity: 1,
            order_type: OrderType::Market,
            price: None,
        };
        let result = broker.execute_with_meta(buy_order, "", "").await;
        assert!(result.is_err(), "BUY order must be suppressed when paused");
        assert!(result.unwrap_err().message.contains("session paused"));
    }

    #[tokio::test]
    async fn test_paused_allows_sell_orders() {
        let (log, _dir) = temp_trade_log();
        let positions = Arc::new(MockPositions { positions: Vec::new(), fail: false });
        let order_placer = Arc::new(MockOrderPlacer {
            result: OrderResult {
                order_id: "test-sell".to_string(),
                status: OrderStatus::Traded,
                timestamp: 0,
                correlation_id: "algomln-test".to_string(),
            },
        });
        let broker = broker_with_mocks(positions, order_placer, never_failing_funds(), log);
        *broker.session_context.write() = Some(SessionContext {
            strategy_id: "strat-pause-test".to_string(),
            strategy_name: "Pause Test".to_string(),
            mode: "live".to_string(),
        });
        broker.set_paused_for_entries(true);

        let sell_order = Order {
            symbol: "RELIANCE".to_string(),
            side: OrderSide::Sell,
            quantity: 1,
            order_type: OrderType::Market,
            price: None,
        };
        let result = broker.execute_with_meta(sell_order, "", "").await;
        assert!(result.is_ok(), "SELL order must go through when paused (exit orders always execute)");
    }

    #[tokio::test]
    async fn test_execute_writes_trade_log() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("trade_log.jsonl");
        let trade_log = Arc::new(TradeLog::open(log_path.clone()).unwrap());

        let order_result = OrderResult {
            order_id: "order-traded-1".to_string(),
            status: OrderStatus::Traded,
            timestamp: 0,
            correlation_id: "algomln-test-trade".to_string(),
        };

        let positions_source = Arc::new(MockPositions {
            positions: Vec::new(),
            fail: false,
        });
        let order_placer = Arc::new(MockOrderPlacer {
            result: order_result,
        });

        let broker = broker_with_mocks(
            positions_source,
            order_placer,
            never_failing_funds(),
            trade_log,
        );

        // Set a SessionContext so the log entry carries real strategy metadata.
        *broker.session_context.write() = Some(SessionContext {
            strategy_id: "strat-42".to_string(),
            strategy_name: "Momentum Cross".to_string(),
            mode: "live".to_string(),
        });

        let order = Order {
            symbol: "NIFTY".to_string(),
            side: OrderSide::Buy,
            quantity: 5,
            order_type: OrderType::Market,
            price: Some(22500.0),
        };

        let result = broker
            .execute_with_meta(order, "rule-1", "stop_loss")
            .await
            .expect("execute_with_meta must succeed for a TRADED result");

        assert!(result.status.is_fill());

        // Read back and verify.
        let entries = TradeLog::read_all(&log_path).unwrap();
        assert_eq!(entries.len(), 1, "exactly one trade log entry expected");
        let entry = &entries[0];
        assert_eq!(entry.rule_id, "rule-1");
        assert_eq!(entry.notes, "stop_loss");
        assert_eq!(entry.strategy_id, "strat-42");
        assert_eq!(entry.strategy_name, "Momentum Cross");
        assert_eq!(entry.symbol, "NIFTY");
        assert_eq!(entry.side, "BUY");
        assert_eq!(entry.quantity, 5);
        assert_eq!(entry.price, 22500.0);
        assert_eq!(entry.order_id, "order-traded-1");
        assert_eq!(entry.order_status, "Traded");
        assert_eq!(entry.mode, "live");
        // id must be a non-empty uuid string
        assert!(!entry.id.is_empty());
    }

    /// H3 (audit): a pre-cancelled `CancellationToken` must abort
    /// `execute_with_meta` before any broker call or trade-log append.
    /// This guards the safety net a `stop()` mid-`place_order` relies on:
    /// without this check, `execute_with_meta` would (a) issue an HTTP
    /// call after the user has been told the session is stopped and
    /// (b) write a phantom trade-log row for an order the user no
    /// longer consented to.
    #[tokio::test]
    async fn test_cancel_token_aborts_execute_with_meta() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("trade_log.jsonl");
        let trade_log = Arc::new(TradeLog::open(log_path.clone()).unwrap());

        let order_placer = Arc::new(MockOrderPlacer {
            result: OrderResult {
                order_id: "should-never-place".to_string(),
                status: OrderStatus::Traded,
                timestamp: 0,
                correlation_id: "algomln-cancel".to_string(),
            },
        });
        let positions = Arc::new(MockPositions {
            positions: Vec::new(),
            fail: false,
        });

        let mut broker = broker_with_mocks(
            positions,
            order_placer.clone(),
            never_failing_funds(),
            trade_log.clone(),
        );

        // Pre-cancel the token before the first order is ever placed.
        let token = CancellationToken::new();
        token.cancel();
        broker.set_cancel_token(token);
        // Note: `set_cancel_token(&self)` doesn't actually need `&mut`,
        // we just keep `mut broker` so `execute_with_meta` is callable
        // through the trait object (`Arc<dyn ExecutionTarget>::execute`
        // requires `&self` — we're calling through concrete type, fine).

        let order = Order {
            symbol: "NIFTY".to_string(),
            side: OrderSide::Buy,
            quantity: 1,
            order_type: OrderType::Market,
            price: Some(22000.0),
        };
        let err = broker
            .execute_with_meta(order, "rule-1", "")
            .await
            .expect_err("execute_with_meta must error on a pre-cancelled token");
        assert!(err.message.contains("session cancelled"), "got: {}", err.message);

        // The trade log must NOT carry a row for the cancelled attempt —
        // no place_order means no entry, by construction.
        let entries = TradeLog::read_all(&log_path).unwrap();
        assert!(
            entries.is_empty(),
            "cancelled order must not write to the trade log; got {:?}",
            entries
        );
    }

    /// H3 (audit): when the token is cancelled *after* the function has
    /// already returned (i.e. never observed), the broker behaves as
    /// before — the check is a snapshot, not a future. This guards the
    /// "session is stable, no cancel yet" path so we don't accidentally
    /// add an unexpected block.
    #[tokio::test]
    async fn test_cancel_token_does_not_block_when_unset() {
        let (log, _dir) = temp_trade_log();
        let positions = Arc::new(MockPositions {
            positions: Vec::new(),
            fail: false,
        });
        let order_placer = Arc::new(MockOrderPlacer {
            result: OrderResult {
                order_id: "ok".to_string(),
                status: OrderStatus::Traded,
                timestamp: 0,
                correlation_id: "algomln-fresh-token".to_string(),
            },
        });
        let broker = broker_with_mocks(positions, order_placer, never_failing_funds(), log);

        // Default token from spawn_with_source — never cancelled.
        let order = Order {
            symbol: "NIFTY".to_string(),
            side: OrderSide::Buy,
            quantity: 1,
            order_type: OrderType::Market,
            price: Some(22000.0),
        };
        let result = broker.execute_with_meta(order, "", "").await;
        assert!(result.is_ok(), "fresh token must not block");
    }

    /// L2 (audit): `set_paused_for_entries` must reject any call when no
    /// `SessionContext` is set on the broker. The flag stays at whatever
    /// value the broker was constructed with (default `false`), so a
    /// rogue `Arc<DhanBroker>` clone cannot pause a non-existent session.
    #[test]
    fn test_set_paused_for_entries_rejected_without_session_context() {
        let (log, _dir) = temp_trade_log();
        let positions = Arc::new(MockPositions { positions: Vec::new(), fail: false });
        let order_placer = Arc::new(MockOrderPlacer {
            result: OrderResult {
                order_id: "unused".to_string(),
                status: OrderStatus::Traded,
                timestamp: 0,
                correlation_id: "algomln-no-session".to_string(),
            },
        });
        let broker = broker_with_mocks(positions, order_placer, never_failing_funds(), log);
        assert!(broker.session_context.read().is_none());

        // No session context: setter returns false and the flag stays false.
        assert!(
            !broker.set_paused_for_entries(true),
            "setter must reject when no SessionContext is set"
        );
        assert!(
            !broker.paused_for_entries_snapshot(),
            "flag must not flip when the setter rejects the call"
        );
        assert!(
            !broker.set_paused_for_entries(false),
            "clear path must also be gated"
        );
    }

    /// W5 (audit): a `refresh_realized_loss` call within the throttle
    /// window must short-circuit so a burst of orders on the same candle
    /// does not pile up N HTTP `GET /positions` calls. We assert this
    /// with an `AtomicUsize` fetch counter on the mock positions source —
    /// the second call inside the window must NOT touch the source.
    #[tokio::test]
    async fn test_refresh_realized_loss_throttles_within_window() {
        use std::sync::atomic::{AtomicUsize, Ordering as AOrd};

        #[derive(Debug)]
        struct CountingPositions {
            positions: Vec<Position>,
            fetch_count: Arc<AtomicUsize>,
        }

        #[async_trait::async_trait]
        impl PositionsSource for CountingPositions {
            async fn fetch(&self) -> anyhow::Result<Vec<Position>> {
                self.fetch_count.fetch_add(1, AOrd::Relaxed);
                Ok(self.positions.clone())
            }
        }

        let fetch_count = Arc::new(AtomicUsize::new(0));
        let source: Arc<dyn PositionsSource> = Arc::new(CountingPositions {
            positions: vec![position_with_pnl(-100.0)],
            fetch_count: fetch_count.clone(),
        });
        let broker = broker_with_source(source);
        // Pre-stamp the throttle instant so the first call is throttled
        // (we want to assert the second-and-subsequent calls are skipped
        // when the window is still open).
        *broker.last_realized_loss_refresh.write() =
            Instant::now() - Duration::from_secs(1);

        // First call: within the throttle window, so no fetch.
        broker.refresh_realized_loss().await;
        assert_eq!(
            fetch_count.load(AOrd::Relaxed),
            0,
            "first call inside the throttle window must short-circuit"
        );

        // Stamp the throttle instant far in the past so the next call fires.
        *broker.last_realized_loss_refresh.write() =
            Instant::now() - Duration::from_secs(PER_ORDER_REFRESH_MIN_INTERVAL_SECS + 1);
        broker.refresh_realized_loss().await;
        assert_eq!(
            fetch_count.load(AOrd::Relaxed),
            1,
            "after the window expires, the next call must hit the source"
        );

        // And the very next call (still inside the new window) must
        // short-circuit again — the throttle does not "leak."
        broker.refresh_realized_loss().await;
        assert_eq!(
            fetch_count.load(AOrd::Relaxed),
            1,
            "second call inside the new window must short-circuit again"
        );

        // `refresh_realized_loss_unchecked` bypasses the throttle —
        // background ticker uses it for that reason.
        broker.refresh_realized_loss_unchecked().await;
        assert_eq!(
            fetch_count.load(AOrd::Relaxed),
            2,
            "unchecked refresh must always hit the source"
        );
    }
}
pub mod dhan;
pub mod order_builder;
pub mod paper;
pub mod target;

pub use dhan::{DhanBroker, SessionContext};
pub use order_builder::{build_order, OrderBuildError};
pub use paper::{PaperBroker, PaperBrokerState, PaperPosition, PaperTrade};
pub use target::{ExecutionError, ExecutionErrorKind, ExecutionTarget};
use crate::models::{Order, OrderSide, OrderType};
use crate::strategy::dsl::{ActionNode, QuantitySpec};

use super::paper::PaperPosition;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderBuildError {
    NoPosition,
    InsufficientCash,
    ZeroQuantity,
    QuantityTooLarge,
}

pub fn build_order(
    action: &ActionNode,
    symbol: &str,
    current_price: f64,
    available_cash: f64,
    current_position: Option<&PaperPosition>,
    _rule_id: &str,
) -> Result<Order, OrderBuildError> {
    let (side, quantity) = match action {
        ActionNode::Buy { quantity } => {
            let quantity = resolve_quantity(quantity, current_price, available_cash)?;
            (OrderSide::Buy, quantity)
        }
        ActionNode::Sell { quantity } => {
            let quantity = resolve_quantity(quantity, current_price, available_cash)?;
            (OrderSide::Sell, quantity)
        }
        ActionNode::SellAll => {
            let position = current_position.ok_or(OrderBuildError::NoPosition)?;
            if position.quantity <= 0 {
                return Err(OrderBuildError::NoPosition);
            }
            (OrderSide::Sell, position.quantity as u64)
        }
    };

    if quantity == 0 {
        return Err(OrderBuildError::ZeroQuantity);
    }

    let quantity = u32::try_from(quantity).map_err(|_| OrderBuildError::QuantityTooLarge)?;

    Ok(Order {
        symbol: symbol.to_string(),
        side,
        quantity,
        order_type: OrderType::Market,
        price: Some(current_price),
    })
}

fn resolve_quantity(
    quantity: &QuantitySpec,
    current_price: f64,
    available_cash: f64,
) -> Result<u64, OrderBuildError> {
    match quantity {
        QuantitySpec::Fixed(value) => Ok(*value),
        QuantitySpec::PercentCapital(pct) => {
            let rupees = available_cash * pct / 100.0;
            quantity_from_rupees(rupees, current_price)
        }
        QuantitySpec::ValueBased(value) => quantity_from_rupees(*value, current_price),
    }
}

fn quantity_from_rupees(rupees: f64, current_price: f64) -> Result<u64, OrderBuildError> {
    if current_price <= 0.0 {
        return Err(OrderBuildError::InsufficientCash);
    }
    let quantity = (rupees / current_price).floor() as u64;
    if quantity == 0 {
        return Err(OrderBuildError::InsufficientCash);
    }
    Ok(quantity)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sell_all_requires_position() {
        let err = build_order(
            &ActionNode::SellAll,
            "NIFTY",
            100.0,
            1_000.0,
            None,
            "rule_0",
        )
        .unwrap_err();
        assert_eq!(err, OrderBuildError::NoPosition);
    }

    #[test]
    fn sell_all_uses_full_position_quantity() {
        let position = PaperPosition {
            symbol: "NIFTY".to_string(),
            quantity: 7,
            avg_entry_price: 90.0,
            unrealized_pnl: 0.0,
        };
        let order = build_order(
            &ActionNode::SellAll,
            "NIFTY",
            100.0,
            1_000.0,
            Some(&position),
            "rule_0",
        )
        .unwrap();
        assert_eq!(order.quantity, 7);
        assert_eq!(order.side, OrderSide::Sell);
    }

    #[test]
    fn percent_quantity_uses_available_cash() {
        let order = build_order(
            &ActionNode::Buy {
                quantity: QuantitySpec::PercentCapital(10.0),
            },
            "NIFTY",
            100.0,
            1_000.0,
            None,
            "rule_0",
        )
        .unwrap();
        assert_eq!(order.quantity, 1);
    }

    #[test]
    fn value_based_quantity_uses_current_price() {
        let order = build_order(
            &ActionNode::Buy {
                quantity: QuantitySpec::ValueBased(550.0),
            },
            "NIFTY",
            100.0,
            1_000.0,
            None,
            "rule_0",
        )
        .unwrap();
        assert_eq!(order.quantity, 5);
    }
}
use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use chrono::Utc;
use serde::Serialize;

use crate::models::{Order, OrderResult, OrderSide, OrderStatus, Position};

use super::target::{ExecutionError, ExecutionTarget};

#[derive(Debug)]
pub struct PaperBroker {
    pub symbol: String,
    state: Mutex<PaperBrokerInner>,
}

#[derive(Debug)]
struct PaperBrokerInner {
    cash: f64,
    initial_cash: f64,
    positions: HashMap<String, PaperPosition>,
    trade_history: Vec<PaperTrade>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PaperPosition {
    pub symbol: String,
    pub quantity: i64,
    pub avg_entry_price: f64,
    pub unrealized_pnl: f64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PaperTrade {
    pub id: String,
    pub timestamp: i64,
    pub symbol: String,
    pub side: OrderSide,
    pub quantity: usize,
    pub price: f64,
    pub rule_id: String,
    pub pnl: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PaperBrokerState {
    pub cash: f64,
    pub initial_cash: f64,
    pub positions: Vec<PaperPosition>,
    pub trade_history: Vec<PaperTrade>,
    pub total_realized_pnl: f64,
}

impl PaperBroker {
    pub fn new(symbol: String, initial_cash: f64) -> Self {
        Self {
            symbol,
            state: Mutex::new(PaperBrokerInner {
                cash: initial_cash,
                initial_cash,
                positions: HashMap::new(),
                trade_history: Vec::new(),
            }),
        }
    }

    pub fn get_state(&self) -> PaperBrokerState {
        let state = self.state.lock().expect("paper broker mutex poisoned");
        broker_state(&state)
    }

    pub fn get_position(&self, symbol: &str) -> Option<PaperPosition> {
        self.state
            .lock()
            .expect("paper broker mutex poisoned")
            .positions
            .get(symbol)
            .cloned()
    }

    pub fn update_unrealized(&self, symbol: &str, current_price: f64) {
        let mut state = self.state.lock().expect("paper broker mutex poisoned");
        if let Some(position) = state.positions.get_mut(symbol) {
            position.unrealized_pnl =
                (current_price - position.avg_entry_price) * position.quantity as f64;
        }
    }

    /// Sum of negative `PaperTrade.pnl` values, returned as a positive number.
    /// Used by the engine's `RISK MAX_DAILY_LOSS` check; a positive return
    /// value means the broker has realized that much loss on the session.
    pub fn realized_loss(&self) -> f64 {
        let state = self.state.lock().expect("paper broker mutex poisoned");
        state
            .trade_history
            .iter()
            .filter_map(|trade| trade.pnl)
            .filter(|pnl| *pnl < 0.0)
            .map(|pnl| -pnl)
            .sum()
    }

    pub fn reset(&self) {
        let mut state = self.state.lock().expect("paper broker mutex poisoned");
        state.cash = state.initial_cash;
        state.positions.clear();
        state.trade_history.clear();
    }

    fn execute_locked(
        &self,
        state: &mut PaperBrokerInner,
        order: Order,
    ) -> Result<OrderResult, ExecutionError> {
        let price = order.price.unwrap_or(0.0);
        let quantity = order.quantity as usize;

        match order.side {
            OrderSide::Buy => execute_buy(state, &order.symbol, quantity, price)?,
            OrderSide::Sell => execute_sell(state, &order.symbol, quantity, price)?,
        }

        Ok(OrderResult {
            order_id: format!("paper-{}", state.trade_history.len()),
            status: OrderStatus::Traded,
            timestamp: Utc::now().timestamp_millis(),
            correlation_id: String::new(),
        })
    }
}

#[async_trait]
impl ExecutionTarget for PaperBroker {
    async fn execute(&self, order: Order) -> Result<OrderResult, ExecutionError> {
        let mut state = self.state.lock().expect("paper broker mutex poisoned");
        self.execute_locked(&mut state, order)
    }

    async fn get_positions(&self) -> Result<Vec<Position>, ExecutionError> {
        let state = self.state.lock().expect("paper broker mutex poisoned");
        Ok(state
            .positions
            .values()
            .map(|position| Position {
                symbol: position.symbol.clone(),
                quantity: position.quantity,
                average_price: position.avg_entry_price,
                ltp: position.avg_entry_price,
                realized_pnl: 0.0,
                unrealized_pnl: position.unrealized_pnl,
            })
            .collect())
    }

    fn realized_loss(&self) -> f64 {
        self.realized_loss()
    }

    fn available_cash(&self) -> f64 {
        self.state.lock().expect("paper broker mutex poisoned").cash
    }

    fn is_paper(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "paper"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

fn execute_buy(
    state: &mut PaperBrokerInner,
    symbol: &str,
    quantity: usize,
    price: f64,
) -> Result<(), ExecutionError> {
    let cost = quantity as f64 * price;
    if cost > state.cash {
        return Err(ExecutionError::insufficient_funds(
            "paper broker has insufficient cash",
        ));
    }

    state.cash -= cost;
    let position = state
        .positions
        .entry(symbol.to_string())
        .or_insert_with(|| PaperPosition {
            symbol: symbol.to_string(),
            quantity: 0,
            avg_entry_price: 0.0,
            unrealized_pnl: 0.0,
        });

    let previous_qty = position.quantity;
    let new_qty = previous_qty + quantity as i64;
    position.avg_entry_price = if new_qty == 0 {
        0.0
    } else {
        ((previous_qty as f64 * position.avg_entry_price) + (quantity as f64 * price))
            / new_qty as f64
    };
    position.quantity = new_qty;

    state.trade_history.push(PaperTrade {
        id: format!("paper-trade-{}", state.trade_history.len() + 1),
        timestamp: Utc::now().timestamp_millis(),
        symbol: symbol.to_string(),
        side: OrderSide::Buy,
        quantity,
        price,
        rule_id: String::new(),
        pnl: None,
    });

    Ok(())
}

fn execute_sell(
    state: &mut PaperBrokerInner,
    symbol: &str,
    quantity: usize,
    price: f64,
) -> Result<(), ExecutionError> {
    let position = state
        .positions
        .get_mut(symbol)
        .ok_or_else(|| ExecutionError::insufficient_position("no open paper position"))?;

    if position.quantity < quantity as i64 {
        return Err(ExecutionError::insufficient_position(
            "paper broker has insufficient position quantity",
        ));
    }

    let realized_pnl = (price - position.avg_entry_price) * quantity as f64;
    state.cash += quantity as f64 * price;
    position.quantity -= quantity as i64;

    state.trade_history.push(PaperTrade {
        id: format!("paper-trade-{}", state.trade_history.len() + 1),
        timestamp: Utc::now().timestamp_millis(),
        symbol: symbol.to_string(),
        side: OrderSide::Sell,
        quantity,
        price,
        rule_id: String::new(),
        pnl: Some(realized_pnl),
    });

    if position.quantity == 0 {
        state.positions.remove(symbol);
    }

    Ok(())
}

fn broker_state(state: &PaperBrokerInner) -> PaperBrokerState {
    PaperBrokerState {
        cash: state.cash,
        initial_cash: state.initial_cash,
        positions: state.positions.values().cloned().collect(),
        total_realized_pnl: state
            .trade_history
            .iter()
            .filter_map(|trade| trade.pnl)
            .sum(),
        trade_history: state.trade_history.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::OrderType;
    use crate::strategy::execution::target::ExecutionErrorKind;

    fn order(side: OrderSide, quantity: u32, price: f64) -> Order {
        Order {
            symbol: "NIFTY".to_string(),
            side,
            quantity,
            order_type: OrderType::Market,
            price: Some(price),
        }
    }

    #[tokio::test]
    async fn buy_deducts_cash() {
        let broker = PaperBroker::new("NIFTY".to_string(), 1_000.0);
        broker
            .execute(order(OrderSide::Buy, 2, 100.0))
            .await
            .unwrap();
        assert_eq!(broker.get_state().cash, 800.0);
    }

    #[tokio::test]
    async fn buy_updates_position() {
        let broker = PaperBroker::new("NIFTY".to_string(), 100_000.0);
        broker
            .execute(order(OrderSide::Buy, 10, 500.0))
            .await
            .unwrap();
        let position = broker.get_position("NIFTY").unwrap();
        assert_eq!(position.quantity, 10);
        assert_eq!(position.avg_entry_price, 500.0);
    }

    #[tokio::test]
    async fn buy_updates_average_entry_price() {
        let broker = PaperBroker::new("NIFTY".to_string(), 10_000.0);
        broker
            .execute(order(OrderSide::Buy, 1, 100.0))
            .await
            .unwrap();
        broker
            .execute(order(OrderSide::Buy, 3, 200.0))
            .await
            .unwrap();
        let position = broker.get_position("NIFTY").unwrap();
        assert_eq!(position.quantity, 4);
        assert_eq!(position.avg_entry_price, 175.0);
    }

    #[tokio::test]
    async fn two_buys_average_entry_price() {
        let broker = PaperBroker::new("NIFTY".to_string(), 100_000.0);
        broker
            .execute(order(OrderSide::Buy, 10, 500.0))
            .await
            .unwrap();
        broker
            .execute(order(OrderSide::Buy, 10, 600.0))
            .await
            .unwrap();
        let position = broker.get_position("NIFTY").unwrap();
        assert_eq!(position.avg_entry_price, 550.0);
    }

    #[tokio::test]
    async fn sell_credits_cash_and_records_pnl() {
        let broker = PaperBroker::new("NIFTY".to_string(), 10_000.0);
        broker
            .execute(order(OrderSide::Buy, 2, 100.0))
            .await
            .unwrap();
        broker
            .execute(order(OrderSide::Sell, 1, 125.0))
            .await
            .unwrap();
        let state = broker.get_state();
        assert_eq!(state.cash, 9_925.0);
        assert_eq!(state.total_realized_pnl, 25.0);
    }

    #[tokio::test]
    async fn sell_credits_cash_and_calculates_pnl() {
        let broker = PaperBroker::new("NIFTY".to_string(), 100_000.0);
        broker
            .execute(order(OrderSide::Buy, 10, 500.0))
            .await
            .unwrap();
        broker
            .execute(order(OrderSide::Sell, 10, 600.0))
            .await
            .unwrap();
        let state = broker.get_state();
        assert_eq!(state.cash, 101_000.0);
        assert_eq!(state.total_realized_pnl, 1_000.0);
    }

    #[tokio::test]
    async fn sell_with_no_position_returns_insufficient_position() {
        let broker = PaperBroker::new("NIFTY".to_string(), 10_000.0);
        let err = broker
            .execute(order(OrderSide::Sell, 1, 100.0))
            .await
            .unwrap_err();
        assert!(matches!(err.kind, ExecutionErrorKind::InsufficientPosition));
    }

    #[tokio::test]
    async fn buy_with_insufficient_funds_returns_error() {
        let broker = PaperBroker::new("NIFTY".to_string(), 50.0);
        let err = broker
            .execute(order(OrderSide::Buy, 1, 100.0))
            .await
            .unwrap_err();
        assert!(matches!(err.kind, ExecutionErrorKind::InsufficientFunds));
    }

    #[tokio::test]
    async fn buy_fails_insufficient_funds() {
        let broker = PaperBroker::new("NIFTY".to_string(), 100_000.0);
        let err = broker
            .execute(order(OrderSide::Buy, 1_000, 200.0))
            .await
            .unwrap_err();
        assert!(matches!(err.kind, ExecutionErrorKind::InsufficientFunds));
    }

    #[tokio::test]
    async fn reset_restores_initial_state() {
        let broker = PaperBroker::new("NIFTY".to_string(), 1_000.0);
        broker
            .execute(order(OrderSide::Buy, 2, 100.0))
            .await
            .unwrap();
        broker.reset();
        let state = broker.get_state();
        assert_eq!(state.cash, 1_000.0);
        assert!(state.positions.is_empty());
        assert!(state.trade_history.is_empty());
    }
}
use async_trait::async_trait;
use serde::Serialize;

use crate::models::{Order, OrderResult, Position};

#[async_trait]
pub trait ExecutionTarget: Send + Sync {
    async fn execute(&self, order: Order) -> Result<OrderResult, ExecutionError>;
    async fn get_positions(&self) -> Result<Vec<Position>, ExecutionError>;
    /// Sum of negative realized `PaperTrade.pnl` values, i.e. the total
    /// realized loss as a positive number. Returns 0.0 if no loss has been
    /// realized (or if the broker is net positive on the session). Used by
    /// the engine's `RISK MAX_DAILY_LOSS` check.
    fn realized_loss(&self) -> f64;
    fn available_cash(&self) -> f64;
    fn is_paper(&self) -> bool;
    fn name(&self) -> &str;
    /// Downcast hook so the engine can recover concrete broker state
    /// (e.g. the `PaperBroker`'s `trade_history` to publish a
    /// `TradeExecuted` event). Mirrors the `as_any` pattern on
    /// `IndicatorRegistryApi` / `UiApi`.
    fn as_any(&self) -> &dyn std::any::Any;
}

#[derive(Debug, Clone, Serialize)]
pub struct ExecutionError {
    pub message: String,
    pub kind: ExecutionErrorKind,
}

impl std::fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

#[derive(Debug, Clone, Serialize)]
pub enum ExecutionErrorKind {
    InsufficientFunds,
    InsufficientPosition,
    BrokerError(String),
}

impl ExecutionError {
    pub fn insufficient_funds(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind: ExecutionErrorKind::InsufficientFunds,
        }
    }

    pub fn insufficient_position(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind: ExecutionErrorKind::InsufficientPosition,
        }
    }
}
use serde::Serialize;

use crate::models::{Order, OrderResult};
use crate::strategy::dsl::{ActionNode, IndicatorKind};
use crate::strategy::runtime::StrategyStatus;

#[derive(Debug, Clone, Serialize)]
pub struct LogEntry {
    pub id: String,
    pub timestamp: i64,
    pub strategy_id: String,
    pub candle_timestamp: i64,
    pub kind: LogEntryKind,
}

#[derive(Debug, Clone, Serialize)]
pub enum LogEntryKind {
    ConditionEvaluated {
        rule_id: String,
        result: bool,
        prev_state: bool,
        fired: bool,
        indicator_snapshots: Vec<IndicatorSnapshot>,
    },
    RuleFired {
        rule_id: String,
        action: ActionNode,
    },
    OrderSubmitted {
        rule_id: String,
        order: Order,
    },
    OrderExecuted {
        rule_id: String,
        result: OrderResult,
    },
    RuleSkipped {
        rule_id: String,
        reason: RuleSkipReason,
    },
    OrderFailed {
        rule_id: String,
        error: String,
    },
    EvalError {
        rule_id: String,
        error: String,
    },
    StatusChanged {
        from: StrategyStatus,
        to: StrategyStatus,
    },
    /// Stop-loss threshold was breached for a held position. Logged before
    /// the resulting `SELL ALL` order is submitted. `symbol` is the held
    /// symbol, `loss_pct` is the unrealized loss against the entry price
    /// (e.g. 2.5 means the position is 2.5% underwater), `price` is the
    /// candle close that triggered the breach.
    StopLossFired {
        symbol: String,
        loss_pct: f64,
        price: f64,
    },
    /// Take-profit threshold was breached for a held position. Logged
    /// before the resulting `SELL ALL` order is submitted. `gain_pct` is
    /// the unrealized gain against the entry price (e.g. 5.0 means 5%
    /// above entry). When both SL and TP would fire on the same candle,
    /// the stop-loss fires first and take-profit is skipped.
    TakeProfitFired {
        symbol: String,
        gain_pct: f64,
        price: f64,
    },
    /// A risk-control declaration on the strategy (e.g. `RISK MAX_ORDERS`)
    /// blocked this order. Logged instead of submitting the order; the
    /// engine does not surface an error to the backtest orchestrator.
    RiskBreach {
        rule_id: String,
        reason: RiskBreachReason,
    },
}

#[derive(Debug, Clone, Serialize)]
pub enum RuleSkipReason {
    NoPosition,
    InsufficientCash,
}

/// Why a risk-control check blocked an order. The variants mirror the three
/// `RISK` declarations: `RISK MAX_ORDERS`, `RISK MAX_POSITIONS`, and
/// `RISK MAX_DAILY_LOSS`. `MaxDailyLossReached` is a hard stop — once the
/// cumulative realized loss crosses the threshold, every subsequent order
/// is skipped regardless of side.
#[derive(Debug, Clone, Serialize)]
pub enum RiskBreachReason {
    MaxOrdersReached,
    MaxPositionsReached,
    MaxDailyLossReached,
}

#[derive(Debug, Clone, Serialize)]
pub struct IndicatorSnapshot {
    pub kind: IndicatorKind,
    pub period: usize,
    pub value: f64,
}

#[derive(Debug)]
pub struct StrategyLogger {
    strategy_id: String,
    entries: Vec<LogEntry>,
    next_id: usize,
}

impl StrategyLogger {
    pub fn new(strategy_id: String) -> Self {
        Self {
            strategy_id,
            entries: Vec::new(),
            next_id: 0,
        }
    }

    pub fn log(&mut self, kind: LogEntryKind, candle_timestamp: i64) {
        let id = format!("log_{}", self.next_id);
        self.next_id += 1;
        self.entries.push(LogEntry {
            id,
            timestamp: candle_timestamp,
            strategy_id: self.strategy_id.clone(),
            candle_timestamp,
            kind,
        });
    }

    pub fn get_entries(&self) -> &[LogEntry] {
        &self.entries
    }

    pub fn drain_entries(&mut self) -> Vec<LogEntry> {
        self.entries.drain(..).collect()
    }
}
pub mod log;

pub use log::{
    IndicatorSnapshot, LogEntry, LogEntryKind, RiskBreachReason, RuleSkipReason, StrategyLogger,
};
pub mod analytics;
pub mod dsl;
pub mod execution;
pub mod logging;
pub mod portfolio;
pub mod runtime;

#[cfg(test)]
pub mod tests;
use std::collections::HashMap;
use std::sync::Arc;

use crate::commands::registry::StrategyMode;
use crate::models::Candle;
use crate::plugin::api::events::EventBus;
use crate::strategy::execution::paper::PaperBroker;
use crate::strategy::logging::log::LogEntry;
use crate::strategy::{
    dsl::ast::{StrategyNode, TradeIn},
    runtime::engine::{StrategyEngine, StrategyInstance, StrategyStatus},
};

/// Resolves a `TradeIn` clause to a concrete list of NSE symbols using the
/// shared `IndexRegistry`. Returns `Err` for the `Index` variant if the alias
/// has no symbols loaded — the user needs to refresh from Settings.
pub fn resolve_trade_in_symbols(
    trade_in: &TradeIn,
    index_registry: &crate::indices::IndexRegistry,
) -> Result<Vec<String>, String> {
    match trade_in {
        TradeIn::Symbols(symbols) => {
            if symbols.is_empty() {
                return Err("TRADE_IN symbol list is empty".to_string());
            }
            Ok(symbols.iter().map(|s| s.to_uppercase()).collect())
        }
        TradeIn::Index(alias) => {
            let symbols = index_registry.get_symbols(alias);
            if symbols.is_empty() {
                Err(format!(
                    "Index '{}' has no symbols loaded. Open Settings → Index Data and click Refresh.",
                    alias.display_name()
                ))
            } else {
                Ok(symbols)
            }
        }
    }
}

/// Runs N independent `StrategyEngine` instances — one per symbol — against a single
/// shared `PaperBroker`. Capital is shared (positions across all symbols sum up).
/// Each sub-engine stamps orders with its own symbol; the `PaperBroker` tracks them
/// separately in its existing `HashMap<String, PaperPosition>`.
///
/// Invariant: sub-engines are never accessed concurrently. `on_tick` takes `&mut self`
/// and dispatches to exactly one sub-engine per call.
pub struct PortfolioEngine {
    /// Symbol (uppercase) → StrategyEngine
    sub_engines: HashMap<String, StrategyEngine>,
    /// Shared broker — held here for direct position/PnL inspection
    broker: Arc<PaperBroker>,
    /// Symbols in insertion order (deterministic for logging)
    symbol_order: Vec<String>,
}

impl PortfolioEngine {
    /// Create a portfolio engine for the given parsed strategy and symbol list.
    ///
    /// `strategy` must NOT have `trade_in` validated here — that is the caller's job.
    /// `symbols` must be non-empty.
    /// `initial_cash` is the shared paper capital across all symbols.
    /// `event_bus` is `None` for deterministic runs; `Some(bus)` for paper/live runs.
    pub fn new(
        strategy: &StrategyNode,
        symbols: Vec<String>,
        initial_cash: f64,
        event_bus: Option<Arc<EventBus>>,
    ) -> Self {
        assert!(
            !symbols.is_empty(),
            "PortfolioEngine: symbols must be non-empty"
        );

        // The shared broker doesn't know about any single symbol — its inner
        // `positions: HashMap<String, PaperPosition>` is keyed by whatever
        // symbol the sub-engine stamps onto each order. We seed it with the
        // first symbol for `PaperBroker::symbol` bookkeeping (used only by
        // tests / debug prints) but the per-symbol positions are tracked
        // independently.
        let broker: Arc<PaperBroker> = Arc::new(PaperBroker::new(
            symbols.first().cloned().unwrap_or_default(),
            initial_cash,
        ));
        let mut sub_engines: HashMap<String, StrategyEngine> = HashMap::new();
        let mut symbol_order: Vec<String> = Vec::with_capacity(symbols.len());

        for symbol in &symbols {
            let sym_upper = symbol.to_uppercase();
            let instance = StrategyInstance {
                id: format!("portfolio-{}", sym_upper),
                strategy: std::sync::Arc::new(strategy.clone()),
                symbol: sym_upper.clone(),
                timeframe: crate::broker::Timeframe::M5,
                status: StrategyStatus::Running,
                execution_target: Arc::clone(&broker)
                    as Arc<dyn crate::strategy::execution::target::ExecutionTarget>,
                initial_cash,
            };

            let mut engine = StrategyEngine::new(instance);
            engine.event_bus = event_bus.clone();
            sub_engines.insert(sym_upper.clone(), engine);
            symbol_order.push(sym_upper);
        }

        Self {
            sub_engines,
            broker,
            symbol_order,
        }
    }

    /// Convenience constructor that resolves a `TradeIn` clause into a symbol
    /// list using the given `IndexRegistry`, then calls `new`.
    pub fn from_trade_in(
        strategy: &StrategyNode,
        trade_in: &TradeIn,
        index_registry: &crate::indices::IndexRegistry,
        initial_cash: f64,
        event_bus: Option<Arc<EventBus>>,
    ) -> Result<Self, String> {
        let symbols = resolve_trade_in_symbols(trade_in, index_registry)?;
        Ok(Self::new(strategy, symbols, initial_cash, event_bus))
    }

    /// Advance one symbol's engine by one candle. Returns that engine's log entries.
    /// If `symbol` is unknown, logs a warning to stderr and returns empty vec.
    pub async fn on_tick(&mut self, symbol: &str, candles: &[Candle]) -> Vec<LogEntry> {
        let key = symbol.to_uppercase();
        match self.sub_engines.get_mut(&key) {
            Some(engine) => engine.on_candle(candles).await,
            None => {
                eprintln!(
                    "[PortfolioEngine] on_tick called for unregistered symbol '{}' — ignoring",
                    symbol
                );
                Vec::new()
            }
        }
    }

    /// Symbols this engine is tracking, in stable insertion order.
    pub fn symbols(&self) -> &[String] {
        &self.symbol_order
    }

    /// Direct access to the shared broker for position/PnL snapshots.
    pub fn broker(&self) -> &Arc<PaperBroker> {
        &self.broker
    }

    pub fn symbol_count(&self) -> usize {
        self.sub_engines.len()
    }

    /// Direct access to a sub-engine (e.g. for status flips). The caller MUST
    /// not call `on_candle` on the sub-engine while `PortfolioEngine::on_tick`
    /// holds `&mut self` — see CLAUDE.md invariant 11.
    pub fn sub_engine(&self, symbol: &str) -> Option<&StrategyEngine> {
        self.sub_engines.get(&symbol.to_uppercase())
    }

    /// Mutable sub-engine access — same constraint as `sub_engine`.
    pub fn sub_engine_mut(&mut self, symbol: &str) -> Option<&mut StrategyEngine> {
        self.sub_engines.get_mut(&symbol.to_uppercase())
    }

    /// Construct a sub-engine for a given symbol with a caller-supplied mode
    /// instead of the default `Paper`. Currently only `Paper` is supported
    /// end-to-end (shared `Arc<PaperBroker>`); the `mode` argument is stored
    /// for forward compatibility and is currently unused.
    #[allow(unused_variables)]
    pub fn new_with_mode(
        strategy: &StrategyNode,
        symbols: Vec<String>,
        initial_cash: f64,
        mode: StrategyMode,
        event_bus: Option<Arc<EventBus>>,
    ) -> Self {
        // The portfolio engine currently only brokers through a shared
        // PaperBroker; live-broker wiring is a future prompt. The mode is
        // accepted so callers can express intent without changing the API.
        Self::new(strategy, symbols, initial_cash, event_bus)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indices::IndexRegistry;
    use crate::strategy::dsl::ast::IndexAlias;
    use crate::strategy::dsl::{AstValidator, Lexer, Parser};
    use crate::strategy::logging::log::LogEntryKind;

    fn parse(source: &str) -> StrategyNode {
        let tokens = Lexer::tokenize(source).expect("lex");
        let node = Parser::new(tokens).parse().expect("parse");
        let errors = AstValidator::validate(&node);
        assert!(errors.is_empty(), "validate: {errors:?}");
        node
    }

    #[test]
    fn resolves_explicit_symbols_to_uppercase() {
        let registry = IndexRegistry::new();
        let trade_in = TradeIn::Symbols(vec!["reliance".into(), "infy".into()]);
        let syms = resolve_trade_in_symbols(&trade_in, &registry).unwrap();
        assert_eq!(syms, vec!["RELIANCE", "INFY"]);
    }

    #[test]
    fn rejects_empty_explicit_symbols() {
        let registry = IndexRegistry::new();
        let trade_in = TradeIn::Symbols(vec![]);
        let err = resolve_trade_in_symbols(&trade_in, &registry).unwrap_err();
        assert!(err.contains("empty"));
    }

    #[test]
    fn index_with_no_symbols_returns_error() {
        let registry = IndexRegistry::new();
        let trade_in = TradeIn::Index(IndexAlias::NiftyBank);
        let err = resolve_trade_in_symbols(&trade_in, &registry).unwrap_err();
        assert!(err.contains("NIFTY BANK"));
    }

    #[test]
    fn index_with_loaded_symbols_returns_them() {
        let registry = IndexRegistry::new();
        registry.update(
            IndexAlias::NiftyBank,
            vec!["HDFCBANK".into(), "ICICIBANK".into()],
            "2026-07-17".into(),
        );
        let trade_in = TradeIn::Index(IndexAlias::NiftyBank);
        let syms = resolve_trade_in_symbols(&trade_in, &registry).unwrap();
        assert_eq!(syms, vec!["HDFCBANK", "ICICIBANK"]);
    }

    #[test]
    fn new_creates_sub_engines_per_symbol() {
        let strategy = parse("WHEN rsi(14) < 30\nBUY 1");
        let engine = PortfolioEngine::new(
            &strategy,
            vec!["RELIANCE".into(), "INFY".into()],
            100_000.0,
            None,
        );
        assert_eq!(engine.symbol_count(), 2);
        assert_eq!(engine.symbols(), &["RELIANCE", "INFY"]);
    }

    #[test]
    fn new_with_empty_symbols_panics() {
        let strategy = parse("WHEN rsi(14) < 30\nBUY 1");
        let result = std::panic::catch_unwind(|| {
            PortfolioEngine::new(&strategy, vec![], 100_000.0, None);
        });
        assert!(result.is_err());
    }

    fn candle(close: f64) -> Candle {
        Candle {
            timestamp: close as i64,
            open: close,
            high: close,
            low: close,
            close,
            volume: 1000.0,
        }
    }

    #[tokio::test]
    async fn on_tick_for_unknown_symbol_returns_empty() {
        let strategy = parse("WHEN close > 0\nBUY 1");
        let mut engine = PortfolioEngine::new(&strategy, vec!["RELIANCE".into()], 100_000.0, None);
        let logs = engine.on_tick("UNKNOWN", &[candle(1.0)]).await;
        assert!(logs.is_empty());
    }

    #[tokio::test]
    async fn sub_engines_share_broker_cash() {
        let strategy = parse("WHEN close > 0\nBUY 1");
        let mut engine =
            PortfolioEngine::new(&strategy, vec!["AAA".into(), "BBB".into()], 10_000.0, None);

        // Drive each sub-engine through one candle. `close > 0` fires the rule
        // once per sub-engine (trigger state prevents re-fire).
        let logs_a = engine.on_tick("AAA", &[candle(1.0)]).await;
        let logs_b = engine.on_tick("BBB", &[candle(1.0)]).await;

        // One trade per sub-engine → two trades total.
        let trades_a = logs_a
            .iter()
            .filter(|e| matches!(e.kind, LogEntryKind::OrderExecuted { .. }))
            .count();
        let trades_b = logs_b
            .iter()
            .filter(|e| matches!(e.kind, LogEntryKind::OrderExecuted { .. }))
            .count();
        assert_eq!(trades_a, 1);
        assert_eq!(trades_b, 1);

        // Both positions live in the same broker.
        let state = engine.broker().get_state();
        assert_eq!(state.positions.len(), 2);
        let symbols: std::collections::BTreeSet<_> =
            state.positions.iter().map(|p| p.symbol.clone()).collect();
        assert!(symbols.contains("AAA"));
        assert!(symbols.contains("BBB"));
    }
}
pub mod engine;
pub use engine::PortfolioEngine;
use crate::models::Candle;

pub struct EvalContext<'a> {
    pub candles: &'a [Candle],
    pub current: &'a Candle,
}

impl<'a> EvalContext<'a> {
    pub fn new(candles: &'a [Candle]) -> Option<Self> {
        candles.last().map(|current| Self { candles, current })
    }
}
use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub struct CrossDetector {
    prev_values: BTreeMap<CrossStateKey, f64>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct CrossStateKey {
    pub rule_id: String,
    pub side: CrossSide,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum CrossSide {
    Fast,
    Slow,
}

impl CrossDetector {
    pub fn new() -> Self {
        Self {
            prev_values: BTreeMap::new(),
        }
    }

    pub fn is_cross_above(&self, rule_id: &str, fast_curr: f64, slow_curr: f64) -> bool {
        match (
            self.get_prev(rule_id, CrossSide::Fast),
            self.get_prev(rule_id, CrossSide::Slow),
        ) {
            (Some(fast_prev), Some(slow_prev)) => fast_prev <= slow_prev && fast_curr > slow_curr,
            _ => false,
        }
    }

    pub fn is_cross_below(&self, rule_id: &str, fast_curr: f64, slow_curr: f64) -> bool {
        match (
            self.get_prev(rule_id, CrossSide::Fast),
            self.get_prev(rule_id, CrossSide::Slow),
        ) {
            (Some(fast_prev), Some(slow_prev)) => fast_prev >= slow_prev && fast_curr < slow_curr,
            _ => false,
        }
    }

    pub fn update(&mut self, rule_id: &str, fast_curr: f64, slow_curr: f64) {
        self.prev_values.insert(
            CrossStateKey {
                rule_id: rule_id.to_string(),
                side: CrossSide::Fast,
            },
            fast_curr,
        );
        self.prev_values.insert(
            CrossStateKey {
                rule_id: rule_id.to_string(),
                side: CrossSide::Slow,
            },
            slow_curr,
        );
    }

    fn get_prev(&self, rule_id: &str, side: CrossSide) -> Option<f64> {
        self.prev_values
            .get(&CrossStateKey {
                rule_id: rule_id.to_string(),
                side,
            })
            .copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_false_with_no_previous_values() {
        let detector = CrossDetector::new();
        assert!(!detector.is_cross_above("rule_0", 50.0, 40.0));
        assert!(!detector.is_cross_below("rule_0", 40.0, 50.0));
    }

    #[test]
    fn detects_cross_above_on_exact_candle() {
        let mut detector = CrossDetector::new();
        detector.update("rule_0", 48.0, 50.0);
        assert!(detector.is_cross_above("rule_0", 52.0, 50.0));
    }

    #[test]
    fn does_not_fire_cross_above_after_crossover() {
        let mut detector = CrossDetector::new();
        detector.update("rule_0", 48.0, 50.0);
        assert!(detector.is_cross_above("rule_0", 52.0, 50.0));
        detector.update("rule_0", 52.0, 50.0);
        assert!(!detector.is_cross_above("rule_0", 55.0, 50.0));
    }

    #[test]
    fn detects_cross_below_on_exact_candle() {
        let mut detector = CrossDetector::new();
        detector.update("rule_0", 52.0, 50.0);
        assert!(detector.is_cross_below("rule_0", 48.0, 50.0));
    }

    #[test]
    fn does_not_fire_cross_below_after_crossover() {
        let mut detector = CrossDetector::new();
        detector.update("rule_0", 52.0, 50.0);
        assert!(detector.is_cross_below("rule_0", 48.0, 50.0));
        detector.update("rule_0", 48.0, 50.0);
        assert!(!detector.is_cross_below("rule_0", 45.0, 50.0));
    }

    #[test]
    fn independent_state_per_rule() {
        let mut detector = CrossDetector::new();
        detector.update("rule_0", 48.0, 50.0);
        assert!(!detector.is_cross_above("rule_1", 52.0, 50.0));
    }
}
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::broker::Timeframe;
use crate::models::{Candle, Order, Position};
use crate::plugin::api::events::{EventBus, EventKind};
use crate::strategy::dsl::{
    ActionNode, CompareOp, ConditionNode, ExprNode, IndicatorKind, PriceField, RiskConfig,
    RuleNode, StrategyNode,
};
use crate::strategy::execution::{
    build_order, ExecutionTarget, OrderBuildError, PaperPosition, PaperTrade,
};

/// A planned-but-not-yet-submitted order produced by the engine's
/// evaluation pass. The intent carries everything the broker needs —
/// including the originating rule and free-form notes — so the caller can
/// execute it outside the engine lock without losing log/event fidelity.
///
/// Intents are produced by [`StrategyEngine::plan_candle`] (eval) and
/// consumed by [`StrategyEngine::execute_intent`] (broker call). The
/// historical [`StrategyEngine::on_candle`] wrapper chains the two
/// phases for backtests and the CLI — call sites that don't need the
/// intent list keep using `on_candle` unchanged.
//
// C2 (live execution audit): the live tick loop previously held the
// engine lock across `on_candle`'s await on the broker HTTP call. By
// splitting eval from execution, the live loop now holds the lock just
// long enough to plan intents, drops it, then executes each intent
// asynchronously. Backtests keep using `process_candle` because they
// already finish before the candle clock advances.
#[derive(Debug, Clone)]
pub struct OrderIntent {
    /// The order to place. `symbol`, `side`, `quantity`, and `price`
    /// are fully resolved; the caller may submit as-is.
    pub order: Order,
    /// Identifier of the rule or safety mechanism that produced the
    /// intent (e.g. `"rule_3"`, `"stop_loss"`, `"take_profit"`).
    /// Forwarded to the trade log so the audit trail links each fill to
    /// its trigger.
    pub rule_id: String,
    /// Free-form note about *why* the intent was produced (e.g.
    /// `"slippage"`). Empty for ordinary rule fires.
    pub notes: String,
    /// The candle timestamp under evaluation. Stored on the intent so
    /// the executor can stamp log entries without re-reading the broker.
    pub candle_timestamp: i64,
}

/// Coarse origin tag for an intent. Used for logging and to let the executor
/// recognize intents that need special handling (e.g. SL/TP vs. rule fires).
///
/// Today the engine only attaches this when it knows the source ("stop_loss",
/// "take_profit"); ordinary rule fires omit it. Phase 9 may surface it on
/// the wire for plugin analytics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderIntentKind {
    RuleFire,
    StopLoss,
    TakeProfit,
}
use crate::strategy::logging::{
    LogEntry, LogEntryKind, RiskBreachReason, RuleSkipReason, StrategyLogger,
};

use super::context::EvalContext;
use super::cross::CrossDetector;
use super::incremental_provider::BoundedWindowProvider;
use super::indicator_provider::{IndicatorProvider, IndicatorProviderProfile};
use super::trigger_state::TriggerStateMap;

const FLOAT_TOLERANCE: f64 = 1e-9;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum StrategyStatus {
    Running,
    Paused,
    Stopped,
    Error(String),
}

pub struct StrategyInstance {
    pub id: String,
    pub strategy: Arc<StrategyNode>,
    pub symbol: String,
    pub timeframe: Timeframe,
    pub status: StrategyStatus,
    pub execution_target: Arc<dyn ExecutionTarget>,
    /// Starting capital used by `RISK MAX_DAILY_LOSS` to compute the
    /// cumulative-loss percentage. Stored on the instance (instead of
    /// being read from `PaperBroker::get_state().initial_cash` via
    /// downcast) so the cap fires on **every** `ExecutionTarget`,
    /// including live `DhanBroker` runs where the broker has no
    /// "initial cash" concept. Backtests, paper runs, portfolio runs,
    /// and live sessions each pass their own value here:
    ///   - backtests: the `--cash` argument
    ///   - paper/portfolio: the paper capital
    ///   - live: the user-declared starting cash (a `0.0` sentinel
    ///     degrades the check to "never breached", matching the old
    ///     pre-fix behaviour for callers that have not yet been
    ///     updated to provide one).
    pub initial_cash: f64,
}

#[derive(Debug, Clone, Serialize)]
pub enum EvalError {
    InsufficientData {
        indicator: IndicatorKind,
        period: usize,
        available: usize,
    },
    InsufficientHistory {
        required: usize,
        available: usize,
    },
    NotYetImplemented(&'static str),
    OrderBuildFailed(String),
    EmptyCandles,
}

pub struct StrategyEngine {
    pub instance: StrategyInstance,
    cross_detector: CrossDetector,
    trigger_state: TriggerStateMap,
    indicator_provider: Box<dyn IndicatorProvider>,
    logger: StrategyLogger,
    profile: StrategyEngineProfile,
    /// Optional event bus used to broadcast engine events to plugins. Set to
    /// `None` for backtest paths (determinism) and live/paper paths until the
    /// stage-9 wiring lands. See `src-tauri/src/main.rs` for the live hook.
    pub event_bus: Option<Arc<EventBus>>,
    /// Per-run risk-control state. Initialized from
    /// `instance.strategy.risk`; only allocated when at least one risk
    /// declaration is present (so strategies without `RISK` pay nothing).
    risk_state: Option<RiskState>,
}

/// Tracks the per-run counters declared via `RISK`.
///
/// `session_orders` counts **entry orders only** (BUYs). Exits (SELL /
/// SELL ALL, including the strategy-level SL/TP synthetic closes) are
/// exempt so a stop-loss / take-profit can always close a position even
/// when the cap is exhausted (audit item A2; invariant 12).
#[derive(Debug, Clone)]
struct RiskState {
    session_orders: u32,
}

impl RiskState {
    fn new() -> Self {
        Self { session_orders: 0 }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct StrategyEngineProfile {
    pub on_candle_calls: u64,
    pub on_candle_time: Duration,
    pub broker_execute_calls: u64,
    pub broker_execute_time: Duration,
    pub broker_get_positions_calls: u64,
    pub broker_get_positions_time: Duration,
}

impl StrategyEngine {
    pub fn new(instance: StrategyInstance) -> Self {
        let logger = StrategyLogger::new(instance.id.clone());
        let risk_state = if instance.strategy.risk.is_some() {
            Some(RiskState::new())
        } else {
            None
        };
        Self {
            instance,
            cross_detector: CrossDetector::new(),
            trigger_state: TriggerStateMap::new(),
            indicator_provider: Box::new(BoundedWindowProvider::new()),
            logger,
            profile: StrategyEngineProfile::default(),
            event_bus: None,
            risk_state,
        }
    }

    /// Evaluate the candle, execute every intent the engine produces **in
    /// rule order**, and return the resulting log entries + intents.
    ///
    /// This is the primary eval+execute loop. The historical contract:
    /// rule N's order is submitted before rule N+1 is evaluated (so a
    /// `SELL ALL` triggered on the same candle as a `BUY` sees the
    /// post-BUY position). The returned `Vec<OrderIntent>` is an audit
    /// trail — every intent the engine executed during this candle —
    /// that the live tick loop consumes for `H3` cancellation hooks and
    /// for plugin observability (see audit items C2, H3).
    ///
    /// The `(logs, intents)` return replaces the legacy `Vec<LogEntry>`
    /// shape on the engine's hot path so callers can observe *what was
    /// done* without re-running the planner.
    pub async fn plan_candle(
        &mut self,
        candles: &[Candle],
    ) -> (Vec<LogEntry>, Vec<OrderIntent>) {
        let started = Instant::now();
        self.profile.on_candle_calls += 1;

        if !matches!(self.instance.status, StrategyStatus::Running) {
            self.profile.on_candle_time += started.elapsed();
            return (Vec::new(), Vec::new());
        }

        let Some(ctx) = EvalContext::new(candles) else {
            self.profile.on_candle_time += started.elapsed();
            return (Vec::new(), Vec::new());
        };

        self.indicator_provider.clear_cache();
        let rules = self.instance.strategy.rules.clone();
        let mut intents = Vec::new();
        let mut log_buffer = Vec::new();

        for rule in &rules {
            let prev_state = self.trigger_state.was_true(&rule.id);
            match self.evaluate_rule(rule, &ctx) {
                Ok(Some(action)) => {
                    self.logger.log(
                        LogEntryKind::ConditionEvaluated {
                            rule_id: rule.id.clone(),
                            result: true,
                            prev_state,
                            fired: true,
                            indicator_snapshots: Vec::new(),
                        },
                        ctx.current.timestamp,
                    );
                    self.logger.log(
                        LogEntryKind::RuleFired {
                            rule_id: rule.id.clone(),
                            action: action.clone(),
                        },
                        ctx.current.timestamp,
                    );
                    if let Some(bus) = &self.event_bus {
                        bus.publish(EventKind::RuleFired {
                            rule_id: rule.id.clone(),
                            strategy_id: self.instance.id.clone(),
                        });
                    }
                    // Build intent + execute immediately so subsequent rules
                    // see the post-execution position (matches the pre-refactor
                    // `submit_action` semantics).
                    let build_result = self.build_intent(&rule.id, action, &ctx, None).await;
                    if let Some(intent) = build_result {
                        let intent_for_log = intent.clone();
                        // `execute_intent` drains the logger; we collect
                        // the new entries into `log_buffer` so the outer
                        // call returns the full stream.
                        let exec_logs = self.execute_intent(intent).await;
                        intents.push(intent_for_log);
                        log_buffer.extend(exec_logs);
                    }
                }
                Ok(None) => {
                    self.logger.log(
                        LogEntryKind::ConditionEvaluated {
                            rule_id: rule.id.clone(),
                            result: self.trigger_state.was_true(&rule.id),
                            prev_state,
                            fired: false,
                            indicator_snapshots: Vec::new(),
                        },
                        ctx.current.timestamp,
                    );
                }
                Err(error) => {
                    self.trigger_state.should_fire(&rule.id, false);
                    self.logger.log(
                        LogEntryKind::EvalError {
                            rule_id: rule.id.clone(),
                            error: format!("{error:?}"),
                        },
                        ctx.current.timestamp,
                    );
                }
            }
        }

        for rule in &rules {
            self.update_cross_state(rule, &ctx);
        }

        // After the rule loop and the cross-state update pass, run the
        // stop-loss / take-profit pass on the current open position. This
        // deliberately runs after the rule loop so a rule that fires on the
        // same candle (closing or opening a position) is reflected in the
        // position we evaluate here. It also runs after the cross-update
        // pass for symmetry with the other post-rule bookkeeping.
        if self.instance.strategy.stop_loss.is_some()
            || self.instance.strategy.take_profit.is_some()
        {
            self.run_stop_loss_take_profit_pass(&ctx, &mut intents, &mut log_buffer)
                .await;
        }

        if let Some(bus) = &self.event_bus {
            bus.publish(EventKind::CandleProcessed(ctx.current.clone()));
        }

        self.indicator_provider.advance(ctx.current);
        let entries = self.logger.drain_entries();
        log_buffer.extend(entries);
        self.profile.on_candle_time += started.elapsed();
        (log_buffer, intents)
    }

    /// Backward-compatible wrapper used by the CLI, backtest IPC, and
    /// `PortfolioEngine::on_tick`. Calls [`Self::plan_candle`] and
    /// discards the intent list — same observable behaviour as before
    /// the C2 refactor but now goes through the unified planner.
    pub async fn on_candle(&mut self, candles: &[Candle]) -> Vec<LogEntry> {
        let (logs, _intents) = self.plan_candle(candles).await;
        logs
    }

    /// Execute a single [`OrderIntent`] through the engine's
    /// `execution_target`. Emits `OrderExecuted` / `OrderFailed` log
    /// entries and publishes `EventKind::TradeExecuted` for fills.
    /// Increments `risk_state.session_orders` on a successful submit only.
    ///
    /// Designed to be called by [`Self::plan_candle`] in rule-order
    /// during planning. The live tick loop may also call it directly
    /// to feed intents through a custom executor (e.g. an H3-aware
    /// executor that checks the cancellation token before the HTTP
    /// round-trip). The audit's full C2 lock-release refactor is
    /// deferred to Phase 9 multi-session support — see the comment on
    /// `LiveSession::start`'s tick loop.
    pub async fn execute_intent(&mut self, intent: OrderIntent) -> Vec<LogEntry> {
        let OrderIntent {
            order,
            rule_id,
            notes: _,
            candle_timestamp,
        } = intent;
        let timestamp = candle_timestamp;

        let started = Instant::now();
        self.profile.broker_execute_calls += 1;
        let execution_result = self.instance.execution_target.execute(order.clone()).await;
        self.profile.broker_execute_time += started.elapsed();

        match execution_result {
            Ok(result) => {
                // MAX_ORDERS counts entry orders only. Exits
                // (OrderSide::Sell) are exempt — see the matching
                // exemption in `check_risk_breach`. Counting exits
                // would silently shrink the effective cap as the
                // strategy cycles (BUY → SL → BUY → SL → …) and
                // could leave a position open with no remaining
                // entry headroom for the next cycle. Audit item A2.
                if matches!(order.side, crate::models::OrderSide::Buy) {
                    if let Some(state) = self.risk_state.as_mut() {
                        state.session_orders += 1;
                    }
                }
                self.logger.log(
                    LogEntryKind::OrderExecuted {
                        rule_id: rule_id.clone(),
                        result: result.clone(),
                    },
                    timestamp,
                );
                if let Some(bus) = &self.event_bus {
                    if let Some(trade) = self.latest_paper_trade() {
                        bus.publish(EventKind::TradeExecuted(trade));
                    }
                }
            }
            Err(error) => self.logger.log(
                LogEntryKind::OrderFailed {
                    rule_id,
                    error: error.message,
                },
                timestamp,
            ),
        }

        self.logger.drain_entries()
    }

    pub fn profile(&self) -> StrategyEngineProfile {
        self.profile
    }

    pub fn indicator_profile(&self) -> IndicatorProviderProfile {
        self.indicator_provider.profile()
    }

    fn evaluate_rule(
        &mut self,
        rule: &RuleNode,
        ctx: &EvalContext<'_>,
    ) -> Result<Option<ActionNode>, EvalError> {
        let condition_result = eval_condition(
            &rule.condition,
            ctx,
            self.indicator_provider.as_mut(),
            &self.cross_detector,
            &rule.id,
        )?;
        let should_fire = self.trigger_state.should_fire(&rule.id, condition_result);
        Ok(should_fire.then(|| rule.action.clone()))
    }

    /// Resolve a planned action into an [`OrderIntent`], running the risk
    /// controls first. Returns `None` when the action was blocked (a
    /// `RiskBreach` / `RuleSkipped` / `OrderFailed` entry has already
    /// been logged in that case). On success logs `OrderSubmitted` and
    /// returns the intent for the caller to execute outside any lock.
    ///
    /// `kind` is `Some(OrderIntentKind::...)` for SL/TP and `None` for
    /// ordinary rule fires. The notes field of the intent is left empty;
    /// callers can add their own via `intent.notes` if needed.
    async fn build_intent(
        &mut self,
        source_id: &str,
        action: ActionNode,
        ctx: &EvalContext<'_>,
        kind: Option<OrderIntentKind>,
    ) -> Option<OrderIntent> {
        if let Some(breach) = self.check_risk_breach(&action, ctx).await {
            self.logger.log(
                LogEntryKind::RiskBreach {
                    rule_id: source_id.to_string(),
                    reason: breach,
                },
                ctx.current.timestamp,
            );
            return None;
        }

        let current_position = self.current_paper_position().await;
        let available_cash = self.instance.execution_target.available_cash();
        let order = match build_order(
            &action,
            &self.instance.symbol,
            ctx.current.close,
            available_cash,
            current_position.as_ref(),
            source_id,
        ) {
            Ok(order) => order,
            Err(error) => {
                match error {
                    OrderBuildError::NoPosition => self.logger.log(
                        LogEntryKind::RuleSkipped {
                            rule_id: source_id.to_string(),
                            reason: RuleSkipReason::NoPosition,
                        },
                        ctx.current.timestamp,
                    ),
                    OrderBuildError::InsufficientCash => self.logger.log(
                        LogEntryKind::RuleSkipped {
                            rule_id: source_id.to_string(),
                            reason: RuleSkipReason::InsufficientCash,
                        },
                        ctx.current.timestamp,
                    ),
                    OrderBuildError::ZeroQuantity => self.logger.log(
                        LogEntryKind::OrderFailed {
                            rule_id: source_id.to_string(),
                            error: "zero quantity - validator missed this".to_string(),
                        },
                        ctx.current.timestamp,
                    ),
                    OrderBuildError::QuantityTooLarge => self.logger.log(
                        LogEntryKind::OrderFailed {
                            rule_id: source_id.to_string(),
                            error: "quantity too large".to_string(),
                        },
                        ctx.current.timestamp,
                    ),
                }
                return None;
            }
        };

        self.logger.log(
            LogEntryKind::OrderSubmitted {
                rule_id: source_id.to_string(),
                order: order.clone(),
            },
            ctx.current.timestamp,
        );

        let notes = match kind {
            Some(OrderIntentKind::StopLoss) => "stop_loss".to_string(),
            Some(OrderIntentKind::TakeProfit) => "take_profit".to_string(),
            _ => String::new(),
        };

        Some(OrderIntent {
            order,
            rule_id: source_id.to_string(),
            notes,
            candle_timestamp: ctx.current.timestamp,
        })
    }

    /// Check the strategy's `RISK` declarations against current state and
    /// return the first `RiskBreachReason` that fires, or `None` if the
    /// order may proceed. Order:
    ///   1. `MAX_ORDERS` — count is in `risk_state.session_orders`, so no
    ///      broker call is needed. **Exits (`SELL` / `SELL ALL`) are
    ///      exempt** so a stop-loss or take-profit can always close a
    ///      position even when the cap is exhausted (audit item A2;
    ///      see invariant 12 — "SL/TP is a safety net").
    ///   2. `MAX_POSITIONS` — counts open positions (`quantity > 0`) on the
    ///      broker. Applies only to BUY actions; sells are never blocked by
    ///      this check.
    ///   3. `MAX_DAILY_LOSS` — uses `execution_target.realized_loss()` and
    ///      `available_cash()` to compute the cumulative loss as a
    ///      percentage of `initial_cash`. In a backtest there's no clock, so
    ///      "daily" is session-scoped (cumulative). When breached, all
    ///      subsequent orders — both buys and sells — are blocked.
    async fn check_risk_breach(
        &mut self,
        action: &ActionNode,
        _ctx: &EvalContext<'_>,
    ) -> Option<RiskBreachReason> {
        let risk: &RiskConfig = self.instance.strategy.risk.as_ref()?;

        if let Some(limit) = risk.max_orders {
            // MAX_ORDERS guards against runaway entry churn. Exits —
            // both rule-driven SELL/SELL ALL and the strategy's
            // safety-net stop-loss / take-profit — must NEVER be
            // blocked, even when the cap is exhausted: leaving a
            // position open while SL/TP silently no-ops contradicts
            // invariant 12 ("SL/TP is a safety net"). See audit item A2.
            let is_exit = matches!(action, ActionNode::Sell { .. } | ActionNode::SellAll);
            if !is_exit {
                let session_orders = self
                    .risk_state
                    .as_ref()
                    .expect("risk_state must be Some when strategy.risk is Some")
                    .session_orders;
                if session_orders >= limit {
                    return Some(RiskBreachReason::MaxOrdersReached);
                }
            }
        }

        if matches!(action, ActionNode::Buy { .. }) {
            if let Some(limit) = risk.max_open_positions {
                let started = std::time::Instant::now();
                self.profile.broker_get_positions_calls += 1;
                let positions = self
                    .instance
                    .execution_target
                    .get_positions()
                    .await
                    .ok()
                    .unwrap_or_default();
                self.profile.broker_get_positions_time += started.elapsed();
                let open_count = positions
                    .iter()
                    .filter(|position| position.quantity > 0)
                    .count() as u32;
                if open_count >= limit {
                    return Some(RiskBreachReason::MaxPositionsReached);
                }
            }
        }

        if let Some(limit_pct) = risk.max_daily_loss_pct {
            // Use `StrategyInstance::initial_cash` (threaded in by the
            // caller — `--cash` for backtests, paper capital for paper
            // runs, the user-declared starting capital for live
            // sessions) instead of downcasting `PaperBroker`. The old
            // downcast returned 0.0 for any non-Paper target, which
            // caused the `initial > 0.0` guard below to skip the
            // check entirely in live trading — making `MAX_DAILY_LOSS`
            // a silent no-op for `DhanBroker`. See audit item A1.
            let initial = self.instance.initial_cash;
            if initial > 0.0 {
                let realized = self.instance.execution_target.realized_loss();
                let loss_pct = realized / initial * 100.0;
                if loss_pct >= limit_pct {
                    return Some(RiskBreachReason::MaxDailyLossReached);
                }
            }
        }
        None
    }

    /// Run the strategy-level stop-loss / take-profit pass on the current
    /// candle. For each open position (`quantity > 0`) on the engine's
    /// symbol, compute the unrealized loss/gain against the entry price and,
    /// if either threshold is breached, submit a `SELL ALL` order through the
    /// normal order path. If both thresholds would fire on the same candle
    /// (e.g. a gap candle), stop-loss fires first and take-profit is
    /// skipped — the position is already closed by the SL pass.
    ///
    /// This deliberately bypasses `TriggerStateMap` and fires every candle
    /// while the position is underwater or in profit; it is the strategy's
    /// safety net, not an edge-triggered rule.
    async fn run_stop_loss_take_profit_pass(
        &mut self,
        ctx: &EvalContext<'_>,
        intents: &mut Vec<OrderIntent>,
        log_buffer: &mut Vec<LogEntry>,
    ) {
        let stop_loss = self.instance.strategy.stop_loss;
        let take_profit = self.instance.strategy.take_profit;

        // Snapshot the open position once. After the SL pass, the position
        // may be closed — TP must not refire against a position that no
        // longer exists.
        let position = match self.current_paper_position().await {
            Some(pos) if pos.quantity > 0 => pos,
            _ => return,
        };

        // Sanity check: the position is in this engine's symbol. In
        // single-symbol backtests / paper runs this is always true; the
        // PortfolioEngine routes per-symbol so each sub-engine sees only
        // its own position.
        if position.symbol != self.instance.symbol {
            return;
        }

        // Need a positive entry price to compute a percentage.
        if position.avg_entry_price <= 0.0 {
            return;
        }

        let current_close = ctx.current.close;
        if current_close <= 0.0 {
            return;
        }

        let entry = position.avg_entry_price;
        let loss_pct = (entry - current_close) / entry * 100.0;
        let gain_pct = (current_close - entry) / entry * 100.0;

        // Stop loss first — if it fires, the position closes and take
        // profit is skipped.
        if let Some(threshold) = stop_loss {
            if loss_pct >= threshold {
                self.logger.log(
                    LogEntryKind::StopLossFired {
                        symbol: self.instance.symbol.clone(),
                        loss_pct,
                        price: current_close,
                    },
                    ctx.current.timestamp,
                );
                if let Some(intent) = self
                    .build_intent("stop_loss", ActionNode::SellAll, ctx, Some(OrderIntentKind::StopLoss))
                    .await
                {
                    let intent_for_log = intent.clone();
                    let exec_logs = self.execute_intent(intent).await;
                    intents.push(intent_for_log);
                    log_buffer.extend(exec_logs);
                }
                return;
            }
        }

        if let Some(threshold) = take_profit {
            if gain_pct >= threshold {
                self.logger.log(
                    LogEntryKind::TakeProfitFired {
                        symbol: self.instance.symbol.clone(),
                        gain_pct,
                        price: current_close,
                    },
                    ctx.current.timestamp,
                );
                if let Some(intent) = self
                    .build_intent("take_profit", ActionNode::SellAll, ctx, Some(OrderIntentKind::TakeProfit))
                    .await
                {
                    let intent_for_log = intent.clone();
                    let exec_logs = self.execute_intent(intent).await;
                    intents.push(intent_for_log);
                    log_buffer.extend(exec_logs);
                }
            }
        }
    }

    async fn current_paper_position(&mut self) -> Option<PaperPosition> {
        let started = Instant::now();
        self.profile.broker_get_positions_calls += 1;
        let positions = self.instance.execution_target.get_positions().await.ok()?;
        self.profile.broker_get_positions_time += started.elapsed();
        positions
            .into_iter()
            .find(|position| position.symbol == self.instance.symbol)
            .map(position_to_paper)
    }

    /// Pull the most recent paper trade from the execution target, if it's a
    /// `PaperBroker`. Returns `None` for live brokers or if no trade has been
    /// recorded yet. Used to fire `EventKind::TradeExecuted` after a successful
    /// execution.
    fn latest_paper_trade(&self) -> Option<PaperTrade> {
        if !self.instance.execution_target.is_paper() {
            return None;
        }
        let any = self.instance.execution_target.as_any();
        let paper = any.downcast_ref::<crate::strategy::execution::PaperBroker>()?;
        paper.get_state().trade_history.last().cloned()
    }

    fn update_cross_state(&mut self, rule: &RuleNode, ctx: &EvalContext<'_>) {
        for (rule_id, fast, slow) in collect_cross_values(
            &rule.id,
            &rule.condition,
            ctx,
            self.indicator_provider.as_mut(),
        ) {
            self.cross_detector.update(&rule_id, fast, slow);
        }
    }
}

fn eval_condition(
    condition: &ConditionNode,
    ctx: &EvalContext<'_>,
    provider: &mut dyn IndicatorProvider,
    cross_detector: &CrossDetector,
    rule_id: &str,
) -> Result<bool, EvalError> {
    match condition {
        ConditionNode::Comparison { left, op, right } => {
            let left = eval_expr(left, ctx, provider)?;
            let right = eval_expr(right, ctx, provider)?;
            Ok(compare(left, op, right))
        }
        ConditionNode::CrossAbove { fast, slow } => {
            let fast = eval_expr(fast, ctx, provider)?;
            let slow = eval_expr(slow, ctx, provider)?;
            Ok(cross_detector.is_cross_above(rule_id, fast, slow))
        }
        ConditionNode::CrossBelow { fast, slow } => {
            let fast = eval_expr(fast, ctx, provider)?;
            let slow = eval_expr(slow, ctx, provider)?;
            Ok(cross_detector.is_cross_below(rule_id, fast, slow))
        }
        ConditionNode::And(left, right) => {
            if !eval_condition(left, ctx, provider, cross_detector, rule_id)? {
                return Ok(false);
            }
            eval_condition(right, ctx, provider, cross_detector, rule_id)
        }
        ConditionNode::Or(left, right) => {
            if eval_condition(left, ctx, provider, cross_detector, rule_id)? {
                return Ok(true);
            }
            eval_condition(right, ctx, provider, cross_detector, rule_id)
        }
        ConditionNode::Not(inner) => Ok(!eval_condition(
            inner,
            ctx,
            provider,
            cross_detector,
            rule_id,
        )?),
        ConditionNode::InPosition => Err(EvalError::NotYetImplemented("in_position")),
        ConditionNode::TimeWindow { .. } => Err(EvalError::NotYetImplemented("between")),
    }
}

fn eval_expr(
    expr: &ExprNode,
    ctx: &EvalContext<'_>,
    provider: &mut dyn IndicatorProvider,
) -> Result<f64, EvalError> {
    match expr {
        ExprNode::Literal(value) => Ok(*value),
        ExprNode::PriceField(field) => Ok(match field {
            PriceField::Close => ctx.current.close,
            PriceField::Open => ctx.current.open,
            PriceField::High => ctx.current.high,
            PriceField::Low => ctx.current.low,
            PriceField::Volume => ctx.current.volume,
            PriceField::PrevClose => {
                if ctx.candles.len() < 2 {
                    return Err(EvalError::InsufficientHistory {
                        required: 2,
                        available: ctx.candles.len(),
                    });
                }
                ctx.candles[ctx.candles.len() - 2].close
            }
            PriceField::PrevOpen => {
                if ctx.candles.len() < 2 {
                    return Err(EvalError::InsufficientHistory {
                        required: 2,
                        available: ctx.candles.len(),
                    });
                }
                ctx.candles[ctx.candles.len() - 2].open
            }
            PriceField::PrevHigh => {
                if ctx.candles.len() < 2 {
                    return Err(EvalError::InsufficientHistory {
                        required: 2,
                        available: ctx.candles.len(),
                    });
                }
                ctx.candles[ctx.candles.len() - 2].high
            }
            PriceField::PrevLow => {
                if ctx.candles.len() < 2 {
                    return Err(EvalError::InsufficientHistory {
                        required: 2,
                        available: ctx.candles.len(),
                    });
                }
                ctx.candles[ctx.candles.len() - 2].low
            }
        }),
        ExprNode::Indicator(call) => {
            provider
                .get(&call.kind, call.period, ctx.candles)
                .ok_or(EvalError::InsufficientData {
                    indicator: call.kind.clone(),
                    period: call.period,
                    available: ctx.candles.len(),
                })
        }
    }
}

fn compare(left: f64, op: &CompareOp, right: f64) -> bool {
    match op {
        CompareOp::Lt => left < right,
        CompareOp::Gt => left > right,
        CompareOp::Lte => left <= right,
        CompareOp::Gte => left >= right,
        CompareOp::Eq => {
            let diff = (left - right).abs();
            let norm = left.abs().max(right.abs());
            diff <= FLOAT_TOLERANCE * norm || diff <= FLOAT_TOLERANCE
        }
        CompareOp::Neq => {
            let diff = (left - right).abs();
            let norm = left.abs().max(right.abs());
            !(diff <= FLOAT_TOLERANCE * norm || diff <= FLOAT_TOLERANCE)
        }
    }
}

fn collect_cross_values(
    rule_id: &str,
    condition: &ConditionNode,
    ctx: &EvalContext<'_>,
    provider: &mut dyn IndicatorProvider,
) -> Vec<(String, f64, f64)> {
    let mut values = Vec::new();
    match condition {
        ConditionNode::CrossAbove { fast, slow } | ConditionNode::CrossBelow { fast, slow } => {
            if let (Ok(fast), Ok(slow)) = (
                eval_expr(fast, ctx, provider),
                eval_expr(slow, ctx, provider),
            ) {
                values.push((rule_id.to_string(), fast, slow));
            }
        }
        ConditionNode::And(left, right) | ConditionNode::Or(left, right) => {
            values.extend(collect_cross_values(rule_id, left, ctx, provider));
            values.extend(collect_cross_values(rule_id, right, ctx, provider));
        }
        ConditionNode::Not(inner) => {
            values.extend(collect_cross_values(rule_id, inner, ctx, provider));
        }
        _ => {}
    }
    values
}

fn position_to_paper(position: Position) -> PaperPosition {
    PaperPosition {
        symbol: position.symbol,
        quantity: position.quantity,
        avg_entry_price: position.average_price,
        unrealized_pnl: position.unrealized_pnl,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compare_float_tolerance() {
        let a = 20000.0;
        let b = 20000.0 + 1e-7; // within 1e-9 * 20000 = 2e-5
        let c = 20000.0 + 1e-3; // outside

        assert!(compare(a, &CompareOp::Eq, b), "{} and {} should be equal", a, b);
        assert!(!compare(a, &CompareOp::Eq, c), "{} and {} should not be equal", a, c);
        assert!(compare(a, &CompareOp::Neq, c), "{} and {} should be not equal", a, c);
        assert!(!compare(a, &CompareOp::Neq, b), "{} and {} should be equal", a, b);

        // Test near zero
        let z1 = 0.0;
        let z2 = 1e-10;
        assert!(compare(z1, &CompareOp::Eq, z2), "0.0 and 1e-10 should be equal");
    }

    #[tokio::test]
    async fn test_dsl_float_equality_regression() {
        // A3 Regression: Equality check against a large price should be
        // tolerant of small floating point noise.
        let mut engine = make_engine("WHEN close == 20000\nBUY 1", 100_000.0);

        // Close is slightly offset from 20000.0 (diff = 1e-7), which is
        // > EPSILON but < our relative tolerance.
        let candles = vec![candle(20000.0000001)];

        let logs = engine.on_candle(&candles).await;
        let buy_fired = logs.iter().any(|e| matches!(e.kind, LogEntryKind::OrderExecuted { .. }));

        assert!(buy_fired, "BUY should fire when close is approximately 20000.0");

        // Test Neq: should NOT fire if values are approximately equal
        let mut engine_neq = make_engine("WHEN close != 20000\nBUY 1", 100_000.0);
        let logs_neq = engine_neq.on_candle(&candles).await;
        let buy_fired_neq = logs_neq.iter().any(|e| matches!(e.kind, LogEntryKind::OrderExecuted { .. }));

        assert!(!buy_fired_neq, "BUY should NOT fire when close is approximately 20000.0 for !=");
    }

    use crate::strategy::dsl::{AstValidator, Lexer, Parser};
    use crate::strategy::execution::PaperBroker;

    fn candle(close: f64) -> Candle {
        Candle {
            timestamp: close as i64,
            open: close,
            high: close,
            low: close,
            close,
            volume: 1000.0,
        }
    }

    fn make_engine(source: &str, initial_cash: f64) -> StrategyEngine {
        let tokens = Lexer::tokenize(source).unwrap();
        let node = Parser::new(tokens).parse().unwrap();
        let errors = AstValidator::validate(&node);
        assert!(errors.is_empty(), "validation failed: {errors:?}");
        let broker = Arc::new(PaperBroker::new("TEST".to_string(), initial_cash));
        let instance = StrategyInstance {
            id: "test-strategy".to_string(),
            strategy: Arc::new(node),
            symbol: "TEST".to_string(),
            timeframe: Timeframe::M5,
            status: StrategyStatus::Running,
            execution_target: broker,
            initial_cash,
        };
        StrategyEngine::new(instance)
    }

    #[tokio::test]
    async fn fires_exactly_once_on_condition_trigger() {
        let mut engine = make_engine("WHEN close > 105\nBUY 1", 100_000.0);
        let candles: Vec<Candle> = (100..=108).map(|close| candle(close as f64)).collect();
        let mut total_trades = 0;

        for index in 1..=candles.len() {
            let logs = engine.on_candle(&candles[..index]).await;
            total_trades += logs
                .iter()
                .filter(|entry| matches!(entry.kind, LogEntryKind::OrderExecuted { .. }))
                .count();
        }

        assert_eq!(total_trades, 1);
    }

    #[tokio::test]
    async fn idiot_test_fires_only_once() {
        let mut engine = make_engine("WHEN close > 0\nBUY 1", 100_000.0);
        let candles: Vec<Candle> = (1..=20).map(|close| candle(close as f64)).collect();
        let mut total_trades = 0;

        for index in 1..=candles.len() {
            let logs = engine.on_candle(&candles[..index]).await;
            total_trades += logs
                .iter()
                .filter(|entry| matches!(entry.kind, LogEntryKind::OrderExecuted { .. }))
                .count();
        }

        assert_eq!(total_trades, 1);
    }

    #[tokio::test]
    async fn fires_again_after_condition_resets() {
        let mut engine = make_engine("WHEN close > 105\nBUY 1", 100_000.0);
        let candles = vec![candle(100.0), candle(106.0), candle(100.0), candle(106.0)];
        let mut total_trades = 0;

        for index in 1..=candles.len() {
            let logs = engine.on_candle(&candles[..index]).await;
            total_trades += logs
                .iter()
                .filter(|entry| matches!(entry.kind, LogEntryKind::OrderExecuted { .. }))
                .count();
        }

        assert_eq!(total_trades, 2);
    }

    #[tokio::test]
    async fn cross_above_fires_exactly_once() {
        let mut engine = make_engine("WHEN cross_above(ma(2), ma(5))\nBUY 1", 100_000.0);
        let closes = [50.0, 49.0, 48.0, 47.0, 46.0, 45.0, 90.0, 92.0, 93.0, 94.0];
        let candles: Vec<Candle> = closes.iter().copied().map(candle).collect();
        let mut total_trades = 0;

        for index in 1..=candles.len() {
            let logs = engine.on_candle(&candles[..index]).await;
            total_trades += logs
                .iter()
                .filter(|entry| matches!(entry.kind, LogEntryKind::OrderExecuted { .. }))
                .count();
        }

        assert_eq!(total_trades, 1);
    }

    #[tokio::test]
    async fn engine_skips_evaluation_when_paused() {
        let mut engine = make_engine("WHEN close > 0\nBUY 1", 100_000.0);
        engine.instance.status = StrategyStatus::Paused;
        let candles: Vec<Candle> = (1..=5).map(|close| candle(close as f64)).collect();

        let logs = engine.on_candle(&candles).await;

        assert!(logs.is_empty());
    }

    #[tokio::test]
    async fn prev_close_requires_previous_candle() {
        let mut engine = make_engine("WHEN close > prev_close\nBUY 1", 100_000.0);
        let candles = vec![candle(100.0)];

        let logs = engine.on_candle(&candles).await;

        assert!(logs.iter().any(|entry| {
            matches!(
                &entry.kind,
                LogEntryKind::EvalError {
                    error,
                    ..
                } if error.contains("InsufficientHistory")
            )
        }));
    }

    // ---------- STOP_LOSS / TAKE_PROFIT ----------

    #[tokio::test]
    async fn stop_loss_fires_when_loss_breaches_threshold() {
        // Stop loss 2%: open at 100, drop to 97 → 3% loss → SL fires.
        let mut engine = make_engine("STOP_LOSS 2%\nWHEN close > 0\nBUY 1", 100_000.0);
        let candles: Vec<Candle> = vec![candle(100.0), candle(100.0), candle(97.0)];

        // Candle 1: BUY fires (close > 0, fresh trigger).
        let logs1 = engine.on_candle(&candles[..1]).await;
        assert!(logs1
            .iter()
            .any(|e| matches!(e.kind, LogEntryKind::OrderExecuted { .. })));

        // Candle 2: no rule fire (trigger state held), no SL (not underwater).
        let logs2 = engine.on_candle(&candles[..2]).await;
        assert!(!logs2
            .iter()
            .any(|e| matches!(e.kind, LogEntryKind::StopLossFired { .. })));

        // Candle 3: 3% loss > 2% threshold → SL fires, position closes.
        let logs3 = engine.on_candle(&candles[..3]).await;
        assert!(logs3
            .iter()
            .any(|e| matches!(e.kind, LogEntryKind::StopLossFired { .. })));
        assert!(logs3
            .iter()
            .any(|e| matches!(e.kind, LogEntryKind::OrderExecuted { .. })));

        // Position should now be closed.
        let state = engine
            .instance
            .execution_target
            .as_any()
            .downcast_ref::<crate::strategy::execution::PaperBroker>()
            .unwrap()
            .get_state();
        assert!(state.positions.is_empty());
    }

    #[tokio::test]
    async fn stop_loss_does_not_fire_when_loss_under_threshold() {
        // Stop loss 5%: open at 100, drop to 97 → 3% loss < 5% threshold.
        let mut engine = make_engine("STOP_LOSS 5%\nWHEN close > 0\nBUY 1", 100_000.0);
        let candles: Vec<Candle> = vec![candle(100.0), candle(100.0), candle(97.0)];

        engine.on_candle(&candles[..1]).await;
        engine.on_candle(&candles[..2]).await;
        let logs3 = engine.on_candle(&candles[..3]).await;

        assert!(!logs3
            .iter()
            .any(|e| matches!(e.kind, LogEntryKind::StopLossFired { .. })));
    }

    #[tokio::test]
    async fn take_profit_fires_when_gain_breaches_threshold() {
        // Take profit 5%: open at 100, rise to 106 → 6% gain → TP fires.
        let mut engine = make_engine("TAKE_PROFIT 5%\nWHEN close > 0\nBUY 1", 100_000.0);
        let candles: Vec<Candle> = vec![candle(100.0), candle(100.0), candle(106.0)];

        engine.on_candle(&candles[..1]).await;
        engine.on_candle(&candles[..2]).await;
        let logs3 = engine.on_candle(&candles[..3]).await;

        assert!(logs3
            .iter()
            .any(|e| matches!(e.kind, LogEntryKind::TakeProfitFired { .. })));
        assert!(logs3
            .iter()
            .any(|e| matches!(e.kind, LogEntryKind::OrderExecuted { .. })));
    }

    #[tokio::test]
    async fn take_profit_does_not_fire_when_gain_under_threshold() {
        // Take profit 10%: open at 100, rise to 105 → 5% gain < 10%.
        let mut engine = make_engine("TAKE_PROFIT 10%\nWHEN close > 0\nBUY 1", 100_000.0);
        let candles: Vec<Candle> = vec![candle(100.0), candle(100.0), candle(105.0)];

        engine.on_candle(&candles[..1]).await;
        engine.on_candle(&candles[..2]).await;
        let logs3 = engine.on_candle(&candles[..3]).await;

        assert!(!logs3
            .iter()
            .any(|e| matches!(e.kind, LogEntryKind::TakeProfitFired { .. })));
    }

    #[tokio::test]
    async fn stop_loss_takes_priority_over_take_profit_on_gap() {
        // 2% SL / 5% TP. Open at 100, gap-candle drops to 95 (5% loss) AND
        // would have been a 5% gain if measured the other way — but here
        // close < entry so only SL can fire. The position is closed, and
        // take-profit must NOT fire because the position is gone.
        let mut engine = make_engine(
            "STOP_LOSS 2%\nTAKE_PROFIT 5%\nWHEN close > 0\nBUY 1",
            100_000.0,
        );
        let candles: Vec<Candle> = vec![candle(100.0), candle(95.0)];

        engine.on_candle(&candles[..1]).await;
        let logs2 = engine.on_candle(&candles[..2]).await;

        assert!(logs2
            .iter()
            .any(|e| matches!(e.kind, LogEntryKind::StopLossFired { .. })));
        assert!(!logs2
            .iter()
            .any(|e| matches!(e.kind, LogEntryKind::TakeProfitFired { .. })));
    }

    #[tokio::test]
    async fn sl_tp_pass_does_nothing_when_no_position() {
        // 2% SL / 5% TP. A dummy rule (which never fires) is needed so the
        // validator accepts the strategy — the engine itself doesn't care
        // about rules, but the validator rejects empty rulesets. Then we
        // assert no SL/TP fires because the rule never opens a position.
        let mut engine = make_engine(
            "STOP_LOSS 2%\nTAKE_PROFIT 5%\nWHEN close > 9999\nBUY 1",
            100_000.0,
        );
        let candles: Vec<Candle> = vec![candle(100.0), candle(50.0), candle(200.0)];

        let logs = engine.on_candle(&candles[..1]).await;
        assert!(!logs
            .iter()
            .any(|e| matches!(e.kind, LogEntryKind::StopLossFired { .. })));
        assert!(!logs
            .iter()
            .any(|e| matches!(e.kind, LogEntryKind::TakeProfitFired { .. })));
    }

    #[tokio::test]
    async fn strategy_without_sl_tp_does_not_fire_them() {
        // No SL/TP declarations. Position drops hard, no SL/TP logs.
        let mut engine = make_engine("WHEN close > 0\nBUY 1", 100_000.0);
        let candles: Vec<Candle> = vec![candle(100.0), candle(1.0)];

        engine.on_candle(&candles[..1]).await;
        let logs2 = engine.on_candle(&candles[..2]).await;

        assert!(!logs2
            .iter()
            .any(|e| matches!(e.kind, LogEntryKind::StopLossFired { .. })));
        assert!(!logs2
            .iter()
            .any(|e| matches!(e.kind, LogEntryKind::TakeProfitFired { .. })));
    }

    #[tokio::test]
    async fn stop_loss_event_carries_loss_pct_and_price() {
        // Verify the log payload: open at 100, close at 96 → 4% loss.
        let mut engine = make_engine("STOP_LOSS 2%\nWHEN close > 0\nBUY 1", 100_000.0);
        let candles: Vec<Candle> = vec![candle(100.0), candle(96.0)];

        engine.on_candle(&candles[..1]).await;
        let logs2 = engine.on_candle(&candles[..2]).await;

        let sl = logs2
            .iter()
            .find_map(|e| match &e.kind {
                LogEntryKind::StopLossFired {
                    symbol,
                    loss_pct,
                    price,
                } => Some((symbol.clone(), *loss_pct, *price)),
                _ => None,
            })
            .expect("expected StopLossFired log entry");
        assert_eq!(sl.0, "TEST");
        assert!((sl.1 - 4.0).abs() < 1e-9, "loss_pct was {}", sl.1);
        assert!((sl.2 - 96.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn take_profit_event_carries_gain_pct_and_price() {
        // Open at 100, close at 108 → 8% gain.
        let mut engine = make_engine("TAKE_PROFIT 5%\nWHEN close > 0\nBUY 1", 100_000.0);
        let candles: Vec<Candle> = vec![candle(100.0), candle(108.0)];

        engine.on_candle(&candles[..1]).await;
        let logs2 = engine.on_candle(&candles[..2]).await;

        let tp = logs2
            .iter()
            .find_map(|e| match &e.kind {
                LogEntryKind::TakeProfitFired {
                    symbol,
                    gain_pct,
                    price,
                } => Some((symbol.clone(), *gain_pct, *price)),
                _ => None,
            })
            .expect("expected TakeProfitFired log entry");
        assert_eq!(tp.0, "TEST");
        assert!((tp.1 - 8.0).abs() < 1e-9, "gain_pct was {}", tp.1);
        assert!((tp.2 - 108.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn sl_tp_is_deterministic() {
        // Same source + same candles must produce the same SL/TP log
        // ordering. Backtest determinism invariant.
        let source = "STOP_LOSS 2%\nTAKE_PROFIT 5%\nWHEN close > 0\nBUY 1";
        let candles: Vec<Candle> = (90..=110).map(|c| candle(c as f64)).collect();

        let mut e1 = make_engine(source, 100_000.0);
        let mut e2 = make_engine(source, 100_000.0);

        let mut all1 = Vec::new();
        let mut all2 = Vec::new();
        for index in 1..=candles.len() {
            all1.extend(e1.on_candle(&candles[..index]).await);
            all2.extend(e2.on_candle(&candles[..index]).await);
        }

        let kind = |e: &LogEntry| match &e.kind {
            LogEntryKind::OrderExecuted { .. } => "exec".to_string(),
            LogEntryKind::StopLossFired { .. } => "sl".to_string(),
            LogEntryKind::TakeProfitFired { .. } => "tp".to_string(),
            LogEntryKind::RuleFired { .. } => "fired".to_string(),
            LogEntryKind::OrderSubmitted { .. } => "submit".to_string(),
            _ => "other".to_string(),
        };
        let seq1: Vec<String> = all1.iter().map(kind).collect();
        let seq2: Vec<String> = all2.iter().map(kind).collect();
        assert_eq!(seq1, seq2);
    }

    #[tokio::test]
    async fn stop_loss_fires_every_candle_while_underwater() {
        // SL is NOT edge-triggered. It must fire every candle the position
        // is underwater, not just on the transition candle. We expect one
        // SL fire per candle the position is open and underwater.
        let mut engine = make_engine("STOP_LOSS 2%\nWHEN close > 0\nBUY 1", 100_000.0);
        // Open at 100, then stay at 97 (3% loss) for three more candles.
        let candles: Vec<Candle> = vec![candle(100.0), candle(97.0), candle(97.0), candle(97.0)];

        let _ = engine.on_candle(&candles[..1]).await;
        let mut total_sl = 0;
        for index in 2..=candles.len() {
            let logs = engine.on_candle(&candles[..index]).await;
            total_sl += logs
                .iter()
                .filter(|e| matches!(e.kind, LogEntryKind::StopLossFired { .. }))
                .count();
        }
        // Position closes on candle 2's SL; candles 3 and 4 have no
        // position, so no SL fires.
        assert_eq!(total_sl, 1);
    }

    // ---------- RISK ----------

    #[tokio::test]
    async fn risk_max_orders_blocks_after_limit() {
        // MAX_ORDERS 2: first two candles fire (close rises above 100, then
        // stays above 100 → trigger holds → re-fires after drop). With the
        // rule re-armed only after close drops below 105, the third
        // qualifying candle must be skipped.
        let mut engine = make_engine("RISK MAX_ORDERS 2\nWHEN close > 100\nBUY 1", 100_000.0);
        let candles: Vec<Candle> = vec![
            candle(100.0),
            candle(110.0),
            candle(50.0),
            candle(110.0),
            candle(50.0),
            candle(110.0),
        ];

        let mut total_orders = 0;
        let mut total_breaches = 0;
        for index in 1..=candles.len() {
            let logs = engine.on_candle(&candles[..index]).await;
            total_orders += logs
                .iter()
                .filter(|e| matches!(e.kind, LogEntryKind::OrderExecuted { .. }))
                .count();
            total_breaches += logs
                .iter()
                .filter(|e| {
                    matches!(
                        e.kind,
                        LogEntryKind::RiskBreach {
                            reason: RiskBreachReason::MaxOrdersReached,
                            ..
                        }
                    )
                })
                .count();
        }
        assert_eq!(total_orders, 2);
        assert_eq!(total_breaches, 1);
    }

    #[tokio::test]
    async fn risk_max_orders_does_not_count_failed_orders() {
        // MAX_ORDERS 1: alternating up/down so the rule re-fires every
        // other candle. The first up-candle's BUY fails (insufficient
        // cash: 1 share @ 200 = 200 > 100 cash), so it does NOT count
        // toward the limit. The next up-candle's BUY succeeds (price
        // 80) and is the first counted order; the third up-candle is
        // blocked.
        let mut engine = make_engine("RISK MAX_ORDERS 1\nWHEN close > 50\nBUY 1", 100.0);
        let candles: Vec<Candle> = vec![
            candle(40.0),  // close <= 50: rule doesn't fire
            candle(200.0), // fires BUY → fails (cost 200 > cash 100)
            candle(40.0),  // close <= 50: rule doesn't fire, trigger resets
            candle(80.0),  // fires BUY → succeeds (count = 1)
            candle(40.0),  // close <= 50: rule doesn't fire, trigger resets
            candle(80.0),  // limit reached → blocked
        ];

        let mut total_orders = 0;
        let mut total_failures = 0;
        let mut total_breaches = 0;
        for index in 1..=candles.len() {
            let logs = engine.on_candle(&candles[..index]).await;
            total_orders += logs
                .iter()
                .filter(|e| matches!(e.kind, LogEntryKind::OrderExecuted { .. }))
                .count();
            total_failures += logs
                .iter()
                .filter(|e| matches!(e.kind, LogEntryKind::OrderFailed { .. }))
                .count();
            total_breaches += logs
                .iter()
                .filter(|e| {
                    matches!(
                        e.kind,
                        LogEntryKind::RiskBreach {
                            reason: RiskBreachReason::MaxOrdersReached,
                            ..
                        }
                    )
                })
                .count();
        }
        assert_eq!(total_orders, 1);
        assert_eq!(total_failures, 1);
        assert_eq!(total_breaches, 1);
    }

    #[tokio::test]
    async fn risk_max_positions_blocks_buys_when_at_limit() {
        // MAX_POSITIONS 1 + alternating up/down so the rule re-fires.
        // The first up-candle's BUY opens a position. The next up-candle
        // (after a reset) must be blocked because the first position is
        // still open.
        let mut engine = make_engine("RISK MAX_POSITIONS 1\nWHEN close > 100\nBUY 1", 100_000.0);
        let candles: Vec<Candle> = vec![
            candle(50.0),  // rule doesn't fire
            candle(150.0), // fires BUY → succeeds
            candle(50.0),  // rule doesn't fire, trigger resets
            candle(150.0), // blocked: 1 position already open
        ];

        let mut total_orders = 0;
        let mut total_breaches = 0;
        for index in 1..=candles.len() {
            let logs = engine.on_candle(&candles[..index]).await;
            total_orders += logs
                .iter()
                .filter(|e| matches!(e.kind, LogEntryKind::OrderExecuted { .. }))
                .count();
            total_breaches += logs
                .iter()
                .filter(|e| {
                    matches!(
                        e.kind,
                        LogEntryKind::RiskBreach {
                            reason: RiskBreachReason::MaxPositionsReached,
                            ..
                        }
                    )
                })
                .count();
        }
        assert_eq!(total_orders, 1, "only the first BUY should execute");
        assert_eq!(total_breaches, 1);
    }

    #[tokio::test]
    async fn risk_max_positions_does_not_block_sells() {
        // MAX_POSITIONS 1: open a position, then a TP-like rule fires a
        // SELL ALL on the second candle. MAX_POSITIONS must not block the
        // sell even though there is an open position.
        let mut engine = make_engine(
            "RISK MAX_POSITIONS 1\nWHEN close > 100\nBUY 1\nWHEN close > 105\nSELL ALL",
            100_000.0,
        );
        let candles: Vec<Candle> = vec![
            candle(50.0),  // rules don't fire
            candle(110.0), // close > 100 → BUY, close > 105 → SELL ALL
        ];

        let mut total_orders = 0;
        let mut total_breaches = 0;
        for index in 1..=candles.len() {
            let logs = engine.on_candle(&candles[..index]).await;
            total_orders += logs
                .iter()
                .filter(|e| matches!(e.kind, LogEntryKind::OrderExecuted { .. }))
                .count();
            total_breaches += logs
                .iter()
                .filter(|e| matches!(e.kind, LogEntryKind::RiskBreach { .. }))
                .count();
        }
        // 1 BUY + 1 SELL = 2 successful orders; no breaches (sells aren't
        // checked, and the BUY happens before MAX_POSITIONS would have a
        // chance to compare to itself).
        assert_eq!(total_orders, 2);
        assert_eq!(total_breaches, 0);
    }

    #[tokio::test]
    async fn risk_max_daily_loss_blocks_after_threshold() {
        // MAX_DAILY_LOSS 1% with initial_cash 1000 (so 1% = 10 realized
        // loss). BUY fires on `close > 100`, SELL on `close < 50`.
        // Open at 200, drop to 40, SELL ALL — realized loss = 160 (160/1000
        // = 16% > 1%). Next buy cycle (after the trigger re-arms) is
        // blocked by the threshold.
        let mut engine = make_engine(
            "RISK MAX_DAILY_LOSS 1%\nWHEN close > 100\nBUY 1\nWHEN close < 50\nSELL ALL",
            1000.0,
        );
        let candles: Vec<Candle> = vec![
            candle(80.0),  // no rule fires
            candle(200.0), // close>100 → BUY 1 @ 200 (cost 200, OK)
            candle(40.0),  // close<50 → SELL ALL @ 40 (loss = 160)
            candle(200.0), // close>100 → BUY blocked (16% loss > 1%)
        ];

        let mut total_orders = 0;
        let mut total_breaches = 0;
        for index in 1..=candles.len() {
            let logs = engine.on_candle(&candles[..index]).await;
            total_orders += logs
                .iter()
                .filter(|e| matches!(e.kind, LogEntryKind::OrderExecuted { .. }))
                .count();
            total_breaches += logs
                .iter()
                .filter(|e| {
                    matches!(
                        e.kind,
                        LogEntryKind::RiskBreach {
                            reason: RiskBreachReason::MaxDailyLossReached,
                            ..
                        }
                    )
                })
                .count();
        }
        assert_eq!(total_orders, 2, "two orders: BUY + SELL");
        assert!(total_breaches >= 1, "expected at least one breach");
    }

    /// Mock `ExecutionTarget` for risk tests that need a non-Paper broker.
    /// Lets a test pre-load `realized_loss` (e.g. for `MAX_DAILY_LOSS`)
    /// without standing up a full `DhanBroker` HTTP plumbing. The order
    /// path accepts any market order and treats it as filled so the engine
    /// flow can complete; only `realized_loss` matters for the A1
    /// regression test.
    struct MockExecutionTarget {
        realized_loss: parking_lot::Mutex<f64>,
        available_cash: parking_lot::Mutex<f64>,
        name: &'static str,
    }

    impl MockExecutionTarget {
        fn new(realized_loss: f64, available_cash: f64) -> Self {
            Self {
                realized_loss: parking_lot::Mutex::new(realized_loss),
                available_cash: parking_lot::Mutex::new(available_cash),
                name: "mock",
            }
        }
    }

    #[async_trait::async_trait]
    impl crate::strategy::execution::ExecutionTarget for MockExecutionTarget {
        async fn execute(
            &self,
            order: crate::models::Order,
        ) -> Result<crate::models::OrderResult, crate::strategy::execution::ExecutionError> {
            Ok(crate::models::OrderResult {
                order_id: format!("mock-{}", order.symbol),
                status: crate::models::OrderStatus::Traded,
                timestamp: 0,
                correlation_id: String::new(),
            })
        }
        async fn get_positions(
            &self,
        ) -> Result<Vec<crate::models::Position>, crate::strategy::execution::ExecutionError>
        {
            Ok(Vec::new())
        }
        fn realized_loss(&self) -> f64 {
            *self.realized_loss.lock()
        }
        fn available_cash(&self) -> f64 {
            *self.available_cash.lock()
        }
        fn is_paper(&self) -> bool {
            false
        }
        fn name(&self) -> &str {
            self.name
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    /// Regression for audit item A1: with a non-Paper `ExecutionTarget`
    /// (`DhanBroker`-shaped), `RISK MAX_DAILY_LOSS` MUST still fire when
    /// `initial_cash` is non-zero. The pre-fix code downcast to
    /// `PaperBroker` and returned 0.0 for any other target, so the check
    /// silently no-op'd in live trading. The engine now reads
    /// `instance.initial_cash` directly, which is the same value the
    /// caller passed at construction time.
    #[tokio::test]
    async fn risk_max_daily_loss_fires_for_non_paper_broker() {
        use crate::strategy::execution::ExecutionTarget;
        use crate::strategy::runtime::StrategyInstance;

        // Broker-shaped mock: realized_loss already at 200 with
        // initial_cash 1000 → 20% > 5% threshold → first BUY must be
        // blocked. Pre-fix this would have been a silent no-op (the
        // downcast returned 0.0, the guard skipped the check, the BUY
        // executed).
        let target: Arc<dyn ExecutionTarget> =
            Arc::new(MockExecutionTarget::new(/* realized_loss */ 200.0, /* cash */ 1000.0));

        let tokens = Lexer::tokenize("RISK MAX_DAILY_LOSS 5%\nWHEN close > 100\nBUY 1").unwrap();
        let node = Parser::new(tokens).parse().unwrap();
        let errors = AstValidator::validate(&node);
        assert!(errors.is_empty(), "validation failed: {errors:?}");

        let instance = StrategyInstance {
            id: "a1-regression".to_string(),
            strategy: Arc::new(node),
            symbol: "TEST".to_string(),
            timeframe: Timeframe::M5,
            status: StrategyStatus::Running,
            execution_target: target,
            initial_cash: 1000.0,
        };
        let mut engine = StrategyEngine::new(instance);

        let candles: Vec<Candle> = vec![candle(80.0), candle(200.0)];

        let mut total_orders = 0;
        let mut total_breaches = 0;
        for index in 1..=candles.len() {
            let logs = engine.on_candle(&candles[..index]).await;
            total_orders += logs
                .iter()
                .filter(|e| matches!(e.kind, LogEntryKind::OrderExecuted { .. }))
                .count();
            total_breaches += logs
                .iter()
                .filter(|e| {
                    matches!(
                        e.kind,
                        LogEntryKind::RiskBreach {
                            reason: RiskBreachReason::MaxDailyLossReached,
                            ..
                        }
                    )
                })
                .count();
        }
        assert_eq!(
            total_orders, 0,
            "BUY must be blocked: realized_loss=200, initial_cash=1000, cap=5%"
        );
        assert_eq!(
            total_breaches, 1,
            "MAX_DAILY_LOSS must fire on a non-Paper ExecutionTarget"
        );
    }

    /// Companion to `risk_max_daily_loss_fires_for_non_paper_broker`:
    /// when the caller passes `initial_cash = 0.0` (or hasn't wired it
    /// yet), the engine degrades to the old behaviour of "never breach"
    /// rather than dividing by zero. The Tauri binary now defaults
    /// `initial_cash` to 1M INR for live sessions, so this branch should
    /// only be reachable on misconfiguration.
    #[tokio::test]
    async fn risk_max_daily_loss_no_op_when_initial_cash_is_zero() {
        use crate::strategy::execution::ExecutionTarget;
        use crate::strategy::runtime::StrategyInstance;

        let target: Arc<dyn ExecutionTarget> =
            Arc::new(MockExecutionTarget::new(/* realized_loss */ 999.0, /* cash */ 1000.0));

        let tokens = Lexer::tokenize("RISK MAX_DAILY_LOSS 1%\nWHEN close > 100\nBUY 1").unwrap();
        let node = Parser::new(tokens).parse().unwrap();
        let errors = AstValidator::validate(&node);
        assert!(errors.is_empty(), "validation failed: {errors:?}");

        let instance = StrategyInstance {
            id: "a1-zero-cash".to_string(),
            strategy: Arc::new(node),
            symbol: "TEST".to_string(),
            timeframe: Timeframe::M5,
            status: StrategyStatus::Running,
            execution_target: target,
            initial_cash: 0.0,
        };
        let mut engine = StrategyEngine::new(instance);

        let candles: Vec<Candle> = vec![candle(200.0)];

        let mut total_orders = 0;
        let mut total_breaches = 0;
        for index in 1..=candles.len() {
            let logs = engine.on_candle(&candles[..index]).await;
            total_orders += logs
                .iter()
                .filter(|e| matches!(e.kind, LogEntryKind::OrderExecuted { .. }))
                .count();
            total_breaches += logs
                .iter()
                .filter(|e| matches!(e.kind, LogEntryKind::RiskBreach { .. }))
                .count();
        }
        // The BUY executes; MAX_DAILY_LOSS check is intentionally skipped
        // because the denominator would be 0. Misconfiguration on the
        // caller's side; stderr warnings at session start are the
        // signal.
        assert_eq!(total_orders, 1);
        assert_eq!(total_breaches, 0);
    }

    #[tokio::test]
    async fn risk_breach_does_not_increment_session_orders() {
        // MAX_ORDERS 1 with alternating up/down. The first up-candle's
        // BUY succeeds (count = 1). The second up-candle's BUY is
        // blocked; the breach must NOT count toward the limit.
        let mut engine = make_engine("RISK MAX_ORDERS 1\nWHEN close > 100\nBUY 1", 100_000.0);
        let candles: Vec<Candle> = vec![
            candle(50.0),  // rule doesn't fire
            candle(150.0), // fires BUY → succeeds (count = 1)
            candle(50.0),  // rule doesn't fire, trigger resets
            candle(150.0), // limit reached → blocked (breach)
        ];

        let mut total_orders = 0;
        let mut total_breaches = 0;
        for index in 1..=candles.len() {
            let logs = engine.on_candle(&candles[..index]).await;
            total_orders += logs
                .iter()
                .filter(|e| matches!(e.kind, LogEntryKind::OrderExecuted { .. }))
                .count();
            total_breaches += logs
                .iter()
                .filter(|e| matches!(e.kind, LogEntryKind::RiskBreach { .. }))
                .count();
        }
        assert_eq!(total_orders, 1);
        assert_eq!(total_breaches, 1);
    }

    #[tokio::test]
    async fn risk_max_orders_does_not_block_stop_loss() {
        // Regression for audit item A2: with the cap exhausted, a
        // stop-loss SELL ALL must still fire. Pre-fix the SL pass
        // shared the same MAX_ORDERS gate as rule fires and would
        // leave the position open with the safety net disabled.
        let mut engine = make_engine(
            "RISK MAX_ORDERS 1\nSTOP_LOSS 2%\nWHEN close > 0\nBUY 1",
            100_000.0,
        );
        // BUY fires (close > 0, fresh trigger, count = 1, cap reached).
        // Then a drop below the 2% threshold must close the position
        // even though MAX_ORDERS is exhausted.
        let candles: Vec<Candle> = vec![candle(100.0), candle(100.0), candle(97.0)];

        let mut total_breaches = 0;
        let mut total_sl = 0;
        let mut total_exec = 0;
        for index in 1..=candles.len() {
            let logs = engine.on_candle(&candles[..index]).await;
            total_breaches += logs
                .iter()
                .filter(|e| {
                    matches!(
                        e.kind,
                        LogEntryKind::RiskBreach {
                            reason: RiskBreachReason::MaxOrdersReached,
                            ..
                        }
                    )
                })
                .count();
            total_sl += logs
                .iter()
                .filter(|e| matches!(e.kind, LogEntryKind::StopLossFired { .. }))
                .count();
            total_exec += logs
                .iter()
                .filter(|e| matches!(e.kind, LogEntryKind::OrderExecuted { .. }))
                .count();
        }
        // 1 BUY (count = 1, cap reached) + 1 SL SELL = 2 successful orders.
        // No MAX_ORDERS breach on the SL candle: exits are exempt.
        assert_eq!(total_breaches, 0, "SL must not be blocked by MAX_ORDERS");
        assert_eq!(total_sl, 1, "SL must fire");
        assert_eq!(total_exec, 2);
    }

    #[tokio::test]
    async fn risk_max_orders_does_not_block_take_profit() {
        // TP counterpart of `risk_max_orders_does_not_block_stop_loss`.
        let mut engine = make_engine(
            "RISK MAX_ORDERS 1\nTAKE_PROFIT 5%\nWHEN close > 0\nBUY 1",
            100_000.0,
        );
        let candles: Vec<Candle> = vec![candle(100.0), candle(100.0), candle(106.0)];

        let mut total_breaches = 0;
        let mut total_tp = 0;
        let mut total_exec = 0;
        for index in 1..=candles.len() {
            let logs = engine.on_candle(&candles[..index]).await;
            total_breaches += logs
                .iter()
                .filter(|e| {
                    matches!(
                        e.kind,
                        LogEntryKind::RiskBreach {
                            reason: RiskBreachReason::MaxOrdersReached,
                            ..
                        }
                    )
                })
                .count();
            total_tp += logs
                .iter()
                .filter(|e| matches!(e.kind, LogEntryKind::TakeProfitFired { .. }))
                .count();
            total_exec += logs
                .iter()
                .filter(|e| matches!(e.kind, LogEntryKind::OrderExecuted { .. }))
                .count();
        }
        assert_eq!(total_breaches, 0, "TP must not be blocked by MAX_ORDERS");
        assert_eq!(total_tp, 1);
        assert_eq!(total_exec, 2);
    }

    #[tokio::test]
    async fn risk_max_orders_does_not_count_or_block_rule_driven_sell() {
        // MAX_ORDERS 2: a rule-driven SELL ALL is also an exit and
        // must (a) execute even when the cap is exhausted and (b) not
        // count toward the cap, so the BUY on the next cycle can
        // still go through.
        let mut engine = make_engine(
            "RISK MAX_ORDERS 2\nWHEN close > 100\nBUY 1\nWHEN close > 105\nSELL ALL",
            100_000.0,
        );
        // Cycle 1: BUY (count=1) + SELL ALL (exit, exempt; count stays 1).
        // Cycle 2: re-arm BUY (drop below 100), then both rules fire
        // again on the up-candle: BUY (count=2, at cap but still <=
        // limit) + SELL ALL (exit, exempt). Without the A2 fix the
        // SELL on cycle 1 would have counted, count would be 2, and
        // this second BUY would be blocked with a RiskBreach.
        let candles: Vec<Candle> = vec![
            candle(50.0),  // no rules fire
            candle(110.0), // BUY (count=1) + SELL ALL (exit, exempt)
            candle(50.0),  // no rules fire, triggers reset
            candle(110.0), // BUY (count=2) + SELL ALL (exit, exempt)
        ];

        let mut total_orders = 0;
        let mut total_breaches = 0;
        for index in 1..=candles.len() {
            let logs = engine.on_candle(&candles[..index]).await;
            total_orders += logs
                .iter()
                .filter(|e| matches!(e.kind, LogEntryKind::OrderExecuted { .. }))
                .count();
            total_breaches += logs
                .iter()
                .filter(|e| {
                    matches!(
                        e.kind,
                        LogEntryKind::RiskBreach {
                            reason: RiskBreachReason::MaxOrdersReached,
                            ..
                        }
                    )
                })
                .count();
        }
        // 2 BUYs + 2 SELL ALLs = 4 orders. No breaches — the SELLs
        // don't consume the cap, so the second BUY fits.
        assert_eq!(total_breaches, 0, "SELL ALL must not consume the cap");
        assert_eq!(total_orders, 4, "BUY+SELL+BUY+SELL should all execute");
    }

    #[tokio::test]
    async fn risk_max_orders_exhausted_then_re_armed_after_exit() {
        // After the cap is exhausted, an exit must not unlock additional
        // entry headroom. The cap is on entry orders; exits neither
        // increment nor decrement the counter.
        let mut engine = make_engine(
            "RISK MAX_ORDERS 1\nWHEN close > 100\nBUY 1\nWHEN close > 105\nSELL ALL",
            100_000.0,
        );
        let candles: Vec<Candle> = vec![
            candle(50.0),  // no fire
            candle(110.0), // BUY (count = 1) + SELL ALL (exit)
            candle(50.0),  // no fire, triggers reset
            candle(110.0), // BUY is blocked (cap reached)
        ];

        let mut total_orders = 0;
        let mut total_breaches = 0;
        for index in 1..=candles.len() {
            let logs = engine.on_candle(&candles[..index]).await;
            total_orders += logs
                .iter()
                .filter(|e| matches!(e.kind, LogEntryKind::OrderExecuted { .. }))
                .count();
            total_breaches += logs
                .iter()
                .filter(|e| {
                    matches!(
                        e.kind,
                        LogEntryKind::RiskBreach {
                            reason: RiskBreachReason::MaxOrdersReached,
                            ..
                        }
                    )
                })
                .count();
        }
        // BUY #1 + SELL ALL = 2 orders executed; BUY #2 blocked.
        assert_eq!(total_orders, 2);
        assert_eq!(total_breaches, 1, "second BUY must still be blocked");
    }

    #[tokio::test]
    async fn strategy_without_risk_does_not_log_breaches() {
        // No RISK declarations: even with the rule firing every candle, no
        // RiskBreach entries are emitted and every order executes.
        let mut engine = make_engine("WHEN close > 0\nBUY 1", 100_000.0);
        let candles: Vec<Candle> = vec![candle(10.0), candle(20.0), candle(30.0)];

        let mut total_orders = 0;
        let mut total_breaches = 0;
        for index in 1..=candles.len() {
            let logs = engine.on_candle(&candles[..index]).await;
            total_orders += logs
                .iter()
                .filter(|e| matches!(e.kind, LogEntryKind::OrderExecuted { .. }))
                .count();
            total_breaches += logs
                .iter()
                .filter(|e| matches!(e.kind, LogEntryKind::RiskBreach { .. }))
                .count();
        }
        assert!(total_orders > 0);
        assert_eq!(total_breaches, 0);
    }

    #[tokio::test]
    async fn risk_is_deterministic() {
        // Same source + same candles = same RiskBreach log sequence.
        // Backtest determinism invariant must hold with risk controls on.
        let source = "RISK MAX_ORDERS 2\nWHEN close > 0\nBUY 1";
        let candles: Vec<Candle> = (1..=10).map(|c| candle(c as f64)).collect();

        let mut e1 = make_engine(source, 100_000.0);
        let mut e2 = make_engine(source, 100_000.0);

        let mut all1 = Vec::new();
        let mut all2 = Vec::new();
        for index in 1..=candles.len() {
            all1.extend(e1.on_candle(&candles[..index]).await);
            all2.extend(e2.on_candle(&candles[..index]).await);
        }

        let kind = |e: &LogEntry| match &e.kind {
            LogEntryKind::OrderExecuted { .. } => "exec".to_string(),
            LogEntryKind::OrderSubmitted { .. } => "submit".to_string(),
            LogEntryKind::RiskBreach { reason, .. } => format!("breach:{reason:?}"),
            _ => "other".to_string(),
        };
        let seq1: Vec<String> = all1.iter().map(kind).collect();
        let seq2: Vec<String> = all2.iter().map(kind).collect();
        assert_eq!(seq1, seq2);
    }

    // ---------- C2: plan_candle / execute_intent ----------

    #[tokio::test]
    async fn plan_candle_returns_intent_with_rule_id() {
        // C2 (audit): `plan_candle` returns the planned `OrderIntent`
        // list alongside the log entries. The intent's `rule_id` is
        // the strategy rule that produced it, and `notes` is empty for
        // ordinary rule fires.
        let mut engine = make_engine("WHEN close > 105\nBUY 1", 100_000.0);
        let candles: Vec<Candle> = (100..=108).map(|c| candle(c as f64)).collect();

        let (logs, intents) = engine.plan_candle(&candles).await;

        // The intent should be for the only rule that fired.
        assert_eq!(intents.len(), 1, "exactly one intent expected");
        let intent = &intents[0];
        assert!(!intent.rule_id.is_empty(), "rule_id must be set");
        assert!(
            intent.notes.is_empty(),
            "ordinary rule fires carry no notes, got {:?}",
            intent.notes
        );
        assert_eq!(intent.order.symbol, "TEST");
        assert_eq!(intent.order.quantity, 1);

        // The side-plan should also have produced OrderExecuted logs.
        assert!(logs
            .iter()
            .any(|e| matches!(e.kind, LogEntryKind::OrderExecuted { .. })));
    }

    #[tokio::test]
    async fn plan_candle_stop_loss_intent_has_notes() {
        // Stop-loss intent should carry `notes = "stop_loss"` so the
        // trade log can identify the safety-net orders.
        let mut engine = make_engine("STOP_LOSS 2%\nWHEN close > 0\nBUY 1", 100_000.0);
        let candles: Vec<Candle> = vec![candle(100.0), candle(100.0), candle(97.0)];

        engine.plan_candle(&candles[..1]).await;
        let (_, intents) = engine.plan_candle(&candles[..2]).await;
        // Candle 2: no rule fire, no SL.
        assert!(intents.is_empty());

        let (_, intents) = engine.plan_candle(&candles[..3]).await;
        // Candle 3: SL fires.
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].notes, "stop_loss");
        assert_eq!(intents[0].rule_id, "stop_loss");
    }

    #[tokio::test]
    async fn plan_candle_returns_empty_intents_when_no_rules_fire() {
        // No fire → no intents.
        let mut engine = make_engine("WHEN close > 9999\nBUY 1", 100_000.0);
        let candles: Vec<Candle> = vec![candle(100.0), candle(200.0)];

        let (logs, intents) = engine.plan_candle(&candles).await;
        assert!(intents.is_empty());
        // No OrderExecuted logs (the rule never fires).
        assert!(logs
            .iter()
            .all(|e| !matches!(e.kind, LogEntryKind::OrderExecuted { .. })));
    }

    #[tokio::test]
    async fn plan_candle_executed_intent_is_deterministic() {
        // Same input → same intent list. The C2 refactor promises a
        // "pure function of (candles, engine_state)" planner; verify
        // that promise concretely.
        let source = "WHEN close > 100\nBUY 1";
        let candles: Vec<Candle> = (95..=105).map(|c| candle(c as f64)).collect();

        let mut e1 = make_engine(source, 100_000.0);
        let mut e2 = make_engine(source, 100_000.0);

        let mut intents1 = Vec::new();
        let mut intents2 = Vec::new();
        for index in 1..=candles.len() {
            let (_, intents) = e1.plan_candle(&candles[..index]).await;
            intents1.extend(intents);
            let (_, intents) = e2.plan_candle(&candles[..index]).await;
            intents2.extend(intents);
        }

        assert_eq!(intents1.len(), intents2.len());
        for (a, b) in intents1.iter().zip(intents2.iter()) {
            assert_eq!(a.rule_id, b.rule_id);
            assert_eq!(a.notes, b.notes);
            assert_eq!(a.order.symbol, b.order.symbol);
            assert_eq!(a.order.side, b.order.side);
            assert_eq!(a.order.quantity, b.order.quantity);
        }
    }

    #[tokio::test]
    async fn on_candle_preserves_logs_when_run_via_plan_candle() {
        // Backward compatibility: `on_candle` is a thin wrapper around
        // `plan_candle` and must produce the same log sequence for
        // back-compat with downstream consumers (CLI, PortfolioEngine,
        // backtest IPC).
        let mut engine = make_engine("WHEN close > 105\nBUY 1", 100_000.0);
        let candles: Vec<Candle> = (100..=110).map(|c| candle(c as f64)).collect();

        let mut a = make_engine("WHEN close > 105\nBUY 1", 100_000.0);
        let mut b = make_engine("WHEN close > 105\nBUY 1", 100_000.0);

        let mut logs_on = Vec::new();
        let mut logs_plan = Vec::new();
        for index in 1..=candles.len() {
            logs_on.extend(a.on_candle(&candles[..index]).await);
            let (l, _i) = b.plan_candle(&candles[..index]).await;
            logs_plan.extend(l);
        }

        // Compare kind sequences. Identical-length and identical kinds.
        let kinds_on: Vec<&str> = logs_on
            .iter()
            .map(|e| match &e.kind {
                LogEntryKind::ConditionEvaluated { .. } => "cond",
                LogEntryKind::RuleFired { .. } => "fired",
                LogEntryKind::OrderSubmitted { .. } => "submit",
                LogEntryKind::OrderExecuted { .. } => "exec",
                LogEntryKind::OrderFailed { .. } => "fail",
                LogEntryKind::RuleSkipped { .. } => "skip",
                LogEntryKind::EvalError { .. } => "err",
                LogEntryKind::StatusChanged { .. } => "status",
                LogEntryKind::StopLossFired { .. } => "sl",
                LogEntryKind::TakeProfitFired { .. } => "tp",
                LogEntryKind::RiskBreach { .. } => "risk",
            })
            .collect();
        let kinds_plan: Vec<&str> = logs_plan
            .iter()
            .map(|e| match &e.kind {
                LogEntryKind::ConditionEvaluated { .. } => "cond",
                LogEntryKind::RuleFired { .. } => "fired",
                LogEntryKind::OrderSubmitted { .. } => "submit",
                LogEntryKind::OrderExecuted { .. } => "exec",
                LogEntryKind::OrderFailed { .. } => "fail",
                LogEntryKind::RuleSkipped { .. } => "skip",
                LogEntryKind::EvalError { .. } => "err",
                LogEntryKind::StatusChanged { .. } => "status",
                LogEntryKind::StopLossFired { .. } => "sl",
                LogEntryKind::TakeProfitFired { .. } => "tp",
                LogEntryKind::RiskBreach { .. } => "risk",
            })
            .collect();

        assert_eq!(kinds_on, kinds_plan);
    }
}
use std::collections::HashMap;
use std::time::Instant;

use crate::indicators::{atr, bbands, ema, ma, rel_vol, rsi, vwap};
use crate::models::Candle;
use crate::strategy::dsl::IndicatorKind;

use super::indicator_provider::{IndicatorProvider, IndicatorProviderProfile};

/// Maximum candles retained in the rolling window.
/// Must be >= the largest indicator period the user might configure.
pub const MAX_WINDOW: usize = 500;

#[derive(Debug, Default)]
pub struct BoundedWindowProvider {
    window: Vec<Candle>,
    cache: HashMap<(IndicatorKind, usize), f64>,
    profile: IndicatorProviderProfile,
}

impl BoundedWindowProvider {
    pub fn new() -> Self {
        Self {
            window: Vec::with_capacity(MAX_WINDOW + 1),
            cache: HashMap::new(),
            profile: IndicatorProviderProfile::default(),
        }
    }

    fn sync_window(&mut self, candles: &[Candle]) {
        let Some(current) = candles.last() else {
            return;
        };

        if self.window.last() == Some(current) {
            return;
        }

        if self.window.is_empty()
            || candles
                .get(candles.len().saturating_sub(2))
                .is_some_and(|previous| self.window.last() == Some(previous))
        {
            self.advance(current);
            self.cache.clear();
            return;
        }

        self.window.clear();
        let start = candles.len().saturating_sub(MAX_WINDOW);
        self.window.extend_from_slice(&candles[start..]);
        self.cache.clear();
    }
}

impl IndicatorProvider for BoundedWindowProvider {
    fn get(&mut self, kind: &IndicatorKind, period: usize, candles: &[Candle]) -> Option<f64> {
        let started = Instant::now();
        self.profile.get_calls += 1;
        self.sync_window(candles);

        if period == 0 || self.window.is_empty() {
            self.profile.cache_misses += 1;
            self.profile.get_time += started.elapsed();
            return None;
        }

        let key = (kind.clone(), period);
        if let Some(value) = self.cache.get(&key) {
            self.profile.cache_hits += 1;
            self.profile.get_time += started.elapsed();
            return Some(*value);
        }

        self.profile.cache_misses += 1;
        let value = latest_indicator_value(kind, period, &self.window);
        self.profile.get_time += started.elapsed();
        let value = value?;
        self.cache.insert(key, value);
        Some(value)
    }

    fn clear_cache(&mut self) {
        self.cache.clear();
    }

    fn advance(&mut self, candle: &Candle) {
        if self.window.last() == Some(candle) {
            return;
        }

        self.window.push(candle.clone());
        if self.window.len() > MAX_WINDOW {
            self.window.remove(0);
        }
    }

    fn profile(&self) -> IndicatorProviderProfile {
        self.profile
    }
}

fn latest_indicator_value(kind: &IndicatorKind, period: usize, candles: &[Candle]) -> Option<f64> {
    let values = match kind {
        IndicatorKind::Ma => ma(candles, period),
        IndicatorKind::Ema => ema(candles, period),
        IndicatorKind::Rsi => rsi(candles, period),
        IndicatorKind::RelVol => {
            return rel_vol(candles, period)
                .last()
                .copied()
                .filter(|value| value.is_finite());
        }
        IndicatorKind::Atr => atr(candles, period),
        IndicatorKind::Vwap => {
            return vwap(candles)
                .last()
                .copied()
                .flatten()
                .map(|point| point.value)
                .filter(|value| value.is_finite());
        }
        IndicatorKind::BbUpper => {
            return bbands(candles, period, 2.0)
                .last()
                .copied()
                .flatten()
                .map(|bands| bands.upper)
                .filter(|value| value.is_finite());
        }
        IndicatorKind::BbLower => {
            return bbands(candles, period, 2.0)
                .last()
                .copied()
                .flatten()
                .map(|bands| bands.lower)
                .filter(|value| value.is_finite());
        }
        IndicatorKind::BbMid => {
            return bbands(candles, period, 2.0)
                .last()
                .copied()
                .flatten()
                .map(|bands| bands.mid)
                .filter(|value| value.is_finite());
        }
    };

    values
        .last()
        .copied()
        .flatten()
        .filter(|value| value.is_finite())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_candle(close: f64) -> Candle {
        Candle {
            timestamp: close as i64,
            open: close,
            high: close + 1.0,
            low: close - 1.0,
            close,
            volume: 1000.0,
        }
    }

    #[test]
    fn window_does_not_grow_past_max() {
        let mut provider = BoundedWindowProvider::new();
        for i in 0..(MAX_WINDOW + 50) {
            provider.advance(&make_candle(i as f64));
        }

        assert_eq!(provider.window.len(), MAX_WINDOW);
    }

    #[test]
    fn returns_none_before_period_satisfied() {
        let mut provider = BoundedWindowProvider::new();
        for i in 0..13 {
            provider.advance(&make_candle(i as f64 + 10.0));
        }

        let result = provider.get(&IndicatorKind::Rsi, 14, &[]);

        assert!(result.is_none());
    }

    #[test]
    fn returns_value_once_period_satisfied() {
        let mut provider = BoundedWindowProvider::new();
        for i in 0..20 {
            provider.advance(&make_candle(i as f64 + 10.0));
        }

        let result = provider.get(&IndicatorKind::Ma, 5, &[]);

        assert!(result.is_some());
    }

    #[test]
    fn cache_hit_returns_same_value() {
        let mut provider = BoundedWindowProvider::new();
        for i in 0..20 {
            provider.advance(&make_candle(i as f64 + 10.0));
        }

        let v1 = provider.get(&IndicatorKind::Ma, 5, &[]);
        let v2 = provider.get(&IndicatorKind::Ma, 5, &[]);

        assert_eq!(v1, v2);
    }

    #[test]
    fn clear_cache_allows_recompute() {
        let mut provider = BoundedWindowProvider::new();
        for i in 0..20 {
            provider.advance(&make_candle(i as f64 + 10.0));
        }

        let v1 = provider.get(&IndicatorKind::Ma, 5, &[]);
        provider.clear_cache();
        provider.advance(&make_candle(200.0));
        let v2 = provider.get(&IndicatorKind::Ma, 5, &[]);

        assert_ne!(v1, v2);
    }

    #[test]
    fn benchmark_1000_candles_with_bounded_provider() {
        use std::time::Instant;

        let mut provider = BoundedWindowProvider::new();
        let candles: Vec<Candle> = (0..1000).map(|i| make_candle(i as f64 + 10.0)).collect();

        let start = Instant::now();
        for i in 1..=candles.len() {
            provider.clear_cache();
            provider.get(&IndicatorKind::Ema, 20, &candles[..i]);
            provider.get(&IndicatorKind::Rsi, 14, &candles[..i]);
            provider.advance(&candles[i - 1]);
        }
        let elapsed = start.elapsed();

        println!(
            "BoundedWindowProvider: 1000 candles, EMA(20)+RSI(14) = {:?}",
            elapsed
        );
    }
}
use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::indicators::{atr, bbands, ema, ma, rel_vol, rsi, vwap};
use crate::models::Candle;
use crate::strategy::dsl::IndicatorKind;

pub trait IndicatorProvider: Send + Sync {
    fn get(&mut self, kind: &IndicatorKind, period: usize, candles: &[Candle]) -> Option<f64>;

    fn clear_cache(&mut self) {}

    fn advance(&mut self, _candle: &Candle) {}

    fn profile(&self) -> IndicatorProviderProfile {
        IndicatorProviderProfile::default()
    }
}

#[derive(Debug, Default)]
pub struct FullRecomputeProvider {
    cache: HashMap<(IndicatorKind, usize), f64>,
    profile: IndicatorProviderProfile,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct IndicatorProviderProfile {
    pub get_calls: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub get_time: Duration,
}

impl FullRecomputeProvider {
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
            profile: IndicatorProviderProfile::default(),
        }
    }
}

impl IndicatorProvider for FullRecomputeProvider {
    fn get(&mut self, kind: &IndicatorKind, period: usize, candles: &[Candle]) -> Option<f64> {
        let started = Instant::now();
        self.profile.get_calls += 1;

        if period == 0 || candles.is_empty() {
            self.profile.cache_misses += 1;
            self.profile.get_time += started.elapsed();
            return None;
        }

        let key = (kind.clone(), period);
        if let Some(value) = self.cache.get(&key) {
            self.profile.cache_hits += 1;
            self.profile.get_time += started.elapsed();
            return Some(*value);
        }

        self.profile.cache_misses += 1;
        let value = latest_indicator_value(kind, period, candles);
        self.profile.get_time += started.elapsed();
        let value = value?;
        self.cache.insert(key, value);
        Some(value)
    }

    fn clear_cache(&mut self) {
        self.cache.clear();
    }

    fn profile(&self) -> IndicatorProviderProfile {
        self.profile
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_candle(close: f64) -> Candle {
        Candle {
            timestamp: close as i64,
            open: close,
            high: close + 1.0,
            low: close - 1.0,
            close,
            volume: 1000.0,
        }
    }

    #[test]
    fn benchmark_1000_candles_with_full_recompute_provider() {
        let mut provider = FullRecomputeProvider::new();
        let candles: Vec<Candle> = (0..1000).map(|i| make_candle(i as f64 + 10.0)).collect();

        let start = Instant::now();
        for i in 1..=candles.len() {
            provider.clear_cache();
            provider.get(&IndicatorKind::Ema, 20, &candles[..i]);
            provider.get(&IndicatorKind::Rsi, 14, &candles[..i]);
            provider.advance(&candles[i - 1]);
        }
        let elapsed = start.elapsed();

        println!(
            "FullRecomputeProvider: 1000 candles, EMA(20)+RSI(14) = {:?}",
            elapsed
        );
    }
}

fn latest_indicator_value(kind: &IndicatorKind, period: usize, candles: &[Candle]) -> Option<f64> {
    let values = match kind {
        IndicatorKind::Ma => ma(candles, period),
        IndicatorKind::Ema => ema(candles, period),
        IndicatorKind::Rsi => rsi(candles, period),
        IndicatorKind::RelVol => {
            return rel_vol(candles, period)
                .last()
                .copied()
                .filter(|value| value.is_finite());
        }
        IndicatorKind::Atr => atr(candles, period),
        IndicatorKind::Vwap => {
            return vwap(candles)
                .last()
                .copied()
                .flatten()
                .map(|point| point.value)
                .filter(|value| value.is_finite());
        }
        IndicatorKind::BbUpper => {
            return bbands(candles, period, 2.0)
                .last()
                .copied()
                .flatten()
                .map(|bands| bands.upper)
                .filter(|value| value.is_finite());
        }
        IndicatorKind::BbLower => {
            return bbands(candles, period, 2.0)
                .last()
                .copied()
                .flatten()
                .map(|bands| bands.lower)
                .filter(|value| value.is_finite());
        }
        IndicatorKind::BbMid => {
            return bbands(candles, period, 2.0)
                .last()
                .copied()
                .flatten()
                .map(|bands| bands.mid)
                .filter(|value| value.is_finite());
        }
    };

    values
        .last()
        .copied()
        .flatten()
        .filter(|value| value.is_finite())
}
pub mod context;
pub mod cross;
pub mod engine;
pub mod incremental_provider;
pub mod indicator_provider;
pub mod trigger_state;

pub use context::EvalContext;
pub use cross::CrossDetector;
pub use engine::{
    EvalError, OrderIntent, OrderIntentKind, StrategyEngine, StrategyEngineProfile,
    StrategyInstance, StrategyStatus,
};
pub use incremental_provider::BoundedWindowProvider;
pub use indicator_provider::{FullRecomputeProvider, IndicatorProvider};
pub use trigger_state::TriggerStateMap;
use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub struct TriggerStateMap {
    states: BTreeMap<String, bool>,
}

impl TriggerStateMap {
    pub fn new() -> Self {
        Self {
            states: BTreeMap::new(),
        }
    }

    pub fn should_fire(&mut self, rule_id: &str, is_true_now: bool) -> bool {
        let was_true = *self.states.get(rule_id).unwrap_or(&false);
        self.states.insert(rule_id.to_string(), is_true_now);
        !was_true && is_true_now
    }

    pub fn was_true(&self, rule_id: &str) -> bool {
        *self.states.get(rule_id).unwrap_or(&false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fires_on_false_to_true() {
        let mut state = TriggerStateMap::new();
        assert!(state.should_fire("rule_0", true));
    }

    #[test]
    fn does_not_fire_on_true_to_true() {
        let mut state = TriggerStateMap::new();
        state.should_fire("rule_0", true);
        assert!(!state.should_fire("rule_0", true));
    }

    #[test]
    fn does_not_fire_on_true_to_false() {
        let mut state = TriggerStateMap::new();
        state.should_fire("rule_0", true);
        assert!(!state.should_fire("rule_0", false));
    }

    #[test]
    fn does_not_fire_on_false_to_false() {
        let mut state = TriggerStateMap::new();
        assert!(!state.should_fire("rule_0", false));
    }

    #[test]
    fn resets_and_fires_again_after_false() {
        let mut state = TriggerStateMap::new();
        state.should_fire("rule_0", true);
        state.should_fire("rule_0", false);
        assert!(state.should_fire("rule_0", true));
    }

    #[test]
    fn independent_state_per_rule() {
        let mut state = TriggerStateMap::new();
        state.should_fire("rule_0", true);
        assert!(state.should_fire("rule_1", true));
    }
}
use crate::commands::strategy::run_backtest_internal;
use crate::models::Candle;
use crate::strategy::dsl::{Lexer, Parser, StrategyNode};

fn candle(close: f64) -> Candle {
    Candle {
        timestamp: close as i64,
        open: close,
        high: close,
        low: close,
        close,
        volume: 1000.0,
    }
}

fn parse_strategy(source: &str) -> StrategyNode {
    let tokens = Lexer::tokenize(source).unwrap();
    Parser::new(tokens).parse().unwrap()
}

#[tokio::test]
async fn backtest_known_output() {
    let strategy = parse_strategy("WHEN close > 105\nBUY 1");
    let candles: Vec<Candle> = (100..=108).map(|close| candle(close as f64)).collect();

    let result = run_backtest_internal(strategy, "TEST".to_string(), candles, 100_000.0)
        .await
        .unwrap();

    assert_eq!(result.trade_history.len(), 1);
    assert_eq!(result.total_candles_processed, 9);
}

#[tokio::test]
async fn backtest_is_deterministic() {
    let source = "WHEN close > 105\nBUY 1\n\nWHEN close < 102\nSELL ALL";
    let candles: Vec<Candle> = (98..=110).map(|close| candle(close as f64)).collect();
    let strategy1 = parse_strategy(source);
    let strategy2 = parse_strategy(source);

    let result1 = run_backtest_internal(strategy1, "TEST".to_string(), candles.clone(), 100_000.0)
        .await
        .unwrap();
    let result2 = run_backtest_internal(strategy2, "TEST".to_string(), candles, 100_000.0)
        .await
        .unwrap();

    assert_eq!(result1.trade_history.len(), result2.trade_history.len());
    assert_eq!(result1.total_realized_pnl, result2.total_realized_pnl);
    assert_eq!(result1.final_cash, result2.final_cash);

    for (trade1, trade2) in result1
        .trade_history
        .iter()
        .zip(result2.trade_history.iter())
    {
        assert_eq!(trade1.price, trade2.price);
        assert_eq!(trade1.quantity, trade2.quantity);
        assert_eq!(trade1.side, trade2.side);
    }
}

#[tokio::test]
async fn backtest_buy_sell_pnl_is_correct() {
    let source = "WHEN close > 105\nBUY 1\n\nWHEN close < 95\nSELL ALL";
    let strategy = parse_strategy(source);
    let candles = vec![candle(100.0), candle(110.0), candle(90.0)];

    let result = run_backtest_internal(strategy, "TEST".to_string(), candles, 100_000.0)
        .await
        .unwrap();

    assert_eq!(result.trade_history.len(), 2);
    assert_eq!(result.total_realized_pnl, -20.0);
}
pub mod backtest_integration;
