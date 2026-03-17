use anyhow::{Context, Result};
use rusqlite::Connection;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

use crate::analysis::opportunity::{Opportunity, RiskLevel, TradeLeg};
use crate::events::bus::TickerData;
use crate::market::instruments::Instrument;

pub struct Storage {
    conn: Arc<Mutex<Connection>>,
}

impl Storage {
    pub async fn new(db_path: &str) -> Result<Self> {
        let path = db_path.to_string();
        let conn = tokio::task::spawn_blocking(move || {
            Connection::open(&path).context("Failed to open SQLite database")
        })
        .await??;

        let storage = Storage {
            conn: Arc::new(Mutex::new(conn)),
        };
        storage.initialize().await?;
        Ok(storage)
    }

    async fn initialize(&self) -> Result<()> {
        let conn = self.conn.lock().await;

        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS instruments (
                instrument_name TEXT PRIMARY KEY,
                strike REAL,
                expiration_timestamp INTEGER,
                option_type TEXT,
                is_active BOOLEAN,
                updated_at INTEGER
            );

            CREATE TABLE IF NOT EXISTS tickers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                instrument_name TEXT,
                timestamp INTEGER,
                mark_price REAL,
                mark_iv REAL,
                best_bid_price REAL,
                best_ask_price REAL,
                best_bid_amount REAL,
                best_ask_amount REAL,
                open_interest REAL,
                delta REAL,
                gamma REAL,
                vega REAL,
                theta REAL
            );

            CREATE TABLE IF NOT EXISTS opportunities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy_type TEXT,
                description TEXT,
                expected_profit REAL,
                risk_level TEXT,
                instruments TEXT,
                legs TEXT,
                detected_at INTEGER,
                expired_at INTEGER,
                expiry_timestamp INTEGER,
                total_cost REAL
            );

            CREATE INDEX IF NOT EXISTS idx_tickers_instrument ON tickers(instrument_name);
            CREATE INDEX IF NOT EXISTS idx_tickers_timestamp ON tickers(timestamp);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_opportunities_key ON opportunities(strategy_type, instruments);
            CREATE INDEX IF NOT EXISTS idx_opportunities_detected ON opportunities(detected_at);
            ",
        )
        .context("Failed to create tables")?;

        // Migrations: add columns if missing
        if conn
            .prepare("SELECT expiry_timestamp FROM opportunities LIMIT 0")
            .is_err()
        {
            let _ = conn
                .execute_batch("ALTER TABLE opportunities ADD COLUMN expiry_timestamp INTEGER;");
        }
        if conn
            .prepare("SELECT total_cost FROM opportunities LIMIT 0")
            .is_err()
        {
            let _ = conn.execute_batch("ALTER TABLE opportunities ADD COLUMN total_cost REAL;");
        }
        // Migration: replace old non-unique index with unique index
        let _ = conn.execute_batch("DROP INDEX IF EXISTS idx_opportunities_type;");
        // Deduplicate existing rows before creating unique index
        let _ = conn.execute_batch(
            "DELETE FROM opportunities WHERE id NOT IN (
                SELECT MAX(id) FROM opportunities GROUP BY strategy_type, instruments
            );",
        );
        let _ = conn.execute_batch(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_opportunities_key ON opportunities(strategy_type, instruments);"
        );

        info!("Database initialized");
        Ok(())
    }

    pub async fn save_instrument(&self, instrument: &Instrument) -> Result<()> {
        let conn = self.conn.lock().await;
        let now = chrono::Utc::now().timestamp();

        conn.execute(
            "INSERT OR REPLACE INTO instruments (instrument_name, strike, expiration_timestamp, option_type, is_active, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                instrument.instrument_name,
                instrument.strike,
                instrument.expiration_timestamp,
                instrument.option_type.to_string(),
                instrument.is_active,
                now,
            ],
        )?;
        Ok(())
    }

    pub async fn save_ticker(&self, instrument_name: &str, data: &TickerData) -> Result<()> {
        let conn = self.conn.lock().await;

        conn.execute(
            "INSERT INTO tickers (instrument_name, timestamp, mark_price, mark_iv, best_bid_price, best_ask_price, best_bid_amount, best_ask_amount, open_interest, delta, gamma, vega, theta)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            rusqlite::params![
                instrument_name,
                data.timestamp,
                data.mark_price,
                data.mark_iv,
                data.best_bid_price,
                data.best_ask_price,
                data.best_bid_amount,
                data.best_ask_amount,
                data.open_interest,
                data.delta,
                data.gamma,
                data.vega,
                data.theta,
            ],
        )?;
        Ok(())
    }

    /// Load opportunities with id > after_id, returns (db_id, Opportunity) pairs
    pub async fn load_opportunities_after(&self, after_id: i64) -> Result<Vec<(i64, Opportunity)>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, strategy_type, description, expected_profit, risk_level, instruments, legs, detected_at, expiry_timestamp, total_cost
             FROM opportunities WHERE id > ?1 ORDER BY id",
        )?;
        let mut results = Vec::new();
        let rows = stmt.query_map([after_id], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, f64>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, i64>(7)?,
                row.get::<_, Option<i64>>(8)?,
                row.get::<_, Option<f64>>(9)?,
            ))
        })?;
        for row in rows {
            let (
                id,
                strategy_type,
                description,
                expected_profit,
                risk_str,
                instruments_json,
                legs_json,
                detected_at,
                expiry_timestamp,
                total_cost,
            ) = row?;
            let risk_level = match risk_str.as_str() {
                "low" => RiskLevel::Low,
                "medium" => RiskLevel::Medium,
                _ => RiskLevel::High,
            };
            let instruments: Vec<String> =
                serde_json::from_str(&instruments_json).unwrap_or_default();
            let legs: Vec<TradeLeg> = serde_json::from_str(&legs_json).unwrap_or_default();
            results.push((
                id,
                Opportunity {
                    strategy_type,
                    description,
                    legs,
                    expected_profit,
                    total_cost: total_cost.unwrap_or(0.0),
                    risk_level,
                    instruments,
                    detected_at,
                    expiry_timestamp,
                },
            ));
        }
        Ok(results)
    }

    /// Count active instruments
    pub async fn count_instruments(&self) -> Result<usize> {
        let conn = self.conn.lock().await;
        let count: i64 =
            conn.query_row("SELECT COUNT(*) FROM instruments", [], |row| row.get(0))?;
        Ok(count as usize)
    }

    pub async fn save_opportunity(&self, opp: &Opportunity) -> Result<()> {
        let conn = self.conn.lock().await;
        // Sort instruments for consistent unique key
        let mut sorted_instruments = opp.instruments.clone();
        sorted_instruments.sort();
        let instruments_json = serde_json::to_string(&sorted_instruments)?;
        let legs_json = serde_json::to_string(&opp.legs)?;

        conn.execute(
            "INSERT OR REPLACE INTO opportunities (strategy_type, description, expected_profit, risk_level, instruments, detected_at, legs, expiry_timestamp, total_cost)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            rusqlite::params![
                opp.strategy_type,
                opp.description,
                opp.expected_profit,
                opp.risk_level.to_string(),
                instruments_json,
                opp.detected_at,
                legs_json,
                opp.expiry_timestamp,
                opp.total_cost,
            ],
        )?;
        Ok(())
    }
}

impl Clone for Storage {
    fn clone(&self) -> Self {
        Storage {
            conn: self.conn.clone(),
        }
    }
}
