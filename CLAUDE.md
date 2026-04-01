# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run Commands

```bash
cargo build                    # Build the project
cargo run                      # Run main binary (real-time scanner)
cargo run --bin monitor        # Run TUI monitor (reads from SQLite DB)
cargo run --bin web            # Run web monitor (HTTP on 0.0.0.0:8080)
cargo test                     # Run all tests
cargo test --test short_put_yield  # Run a single integration test
cargo clippy                   # Lint
cargo fmt --check              # Check formatting
```

Requires a `.env` file (see `.env.example`): `DERIBIT_CLIENT_ID`, `DERIBIT_CLIENT_SECRET`, `DERIBIT_ENV` (test|prod), `ALERT_THRESHOLD`, `DB_PATH`.

Log level controlled via `RUST_LOG` env var.

## Architecture

Rust async application (tokio) for real-time BTC options arbitrage detection on Deribit exchange. Two binaries:

- **`main`** (`src/main.rs`): Connects to Deribit WebSocket API, subscribes to all BTC option tickers, runs analysis scanners every 10s, persists opportunities to SQLite.
- **`monitor`** (`src/bin/monitor.rs`): Offline TUI (ratatui) that polls SQLite for opportunities and displays them. Includes short put APY history charting via candle data.
- **`web`** (`src/bin/web.rs`): Web monitor (axum) serving a single-page dashboard at `http://0.0.0.0:8080`. Same features as TUI: filters, sorting, leverage, detail view, APY history charts. Bind address configurable via `WEB_BIND` env var.

### Data Flow

```
Deribit WS API → WsManager → EventBus (broadcast) → TickerCache / OrderBookManager
                                                   ↓
                                            Analysis Scanners (10s interval)
                                                   ↓
                                            Opportunity → mpsc → SQLite Storage
                                                   ↓
                                            Monitor TUI (polls DB)
```

### Module Layout

- **`ws/`**: WebSocket client (`WsManager`/`WsClient`), HMAC auth, rate limiter. `WsClient` is the shared handle for sending JSON-RPC requests from any task.
- **`events/`**: `EventBus` — tokio broadcast channel carrying `Event` variants (ticker updates, orderbook updates, instruments loaded, opportunity found).
- **`market/`**: `InstrumentRegistry` (parsed option metadata), `TickerCache`, `OrderBookManager`, `Subscriber` (batch subscription helper).
- **`analysis/`**: Arbitrage/signal analyzers — each implements a `scan()` method returning `Vec<Opportunity>`:
  - Arbitrage: `put_call_parity`, `box_spread`, `conversion`, `vertical_arb`, `calendar_arb`
  - Signals: `vol_surface`, `calendar_spread`, `short_put_yield`
  - Supporting: `opportunity` (core types), `portfolio` (optimizer), `short_put_history`/`short_put_history_service` (candle-based APY history)
- **`storage/`**: SQLite persistence via rusqlite (instruments, tickers, opportunities).
- **`alert/`**: Notification system (`notifier`).
- **`tui.rs`**: ratatui terminal UI with list/detail views, portfolio optimization, and APY history charts.
- **`config.rs`**: Env-based configuration via dotenvy.

### Key Patterns

- `EventBus` uses tokio broadcast; high-volume ticker events can cause subscriber lag — opportunity saving uses a dedicated mpsc channel to avoid this.
- Two separate `InstrumentRegistry` instances: one for BTC options (arb analysis), one combining BTC + BTC-USDC options (short put yield).
- Analyzers are stateless — they receive `&InstrumentRegistry` and `&TickerCache` on each scan and return opportunities.
- `Opportunity` is the universal output type with `TradeLeg` entries, serialized to JSON for SQLite storage.
