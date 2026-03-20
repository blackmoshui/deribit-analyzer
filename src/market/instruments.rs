use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Instrument {
    pub instrument_name: String,
    pub strike: f64,
    pub expiration_timestamp: i64,
    pub option_type: OptionType,
    pub is_active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum OptionType {
    Call,
    Put,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum OptionPriceCurrency {
    BaseAsset,
    QuoteCurrency,
}

impl std::fmt::Display for OptionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OptionType::Call => write!(f, "call"),
            OptionType::Put => write!(f, "put"),
        }
    }
}

/// Shared instrument registry
#[derive(Clone)]
pub struct InstrumentRegistry {
    instruments: Arc<RwLock<HashMap<String, Instrument>>>,
}

impl InstrumentRegistry {
    pub fn new() -> Self {
        InstrumentRegistry {
            instruments: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Load instruments from API response
    pub async fn load_from_response(&self, result: &serde_json::Value) -> Result<usize> {
        let instruments = Self::parse_response(result)?;
        Ok(self.replace_all(instruments).await)
    }

    pub fn parse_response(result: &serde_json::Value) -> Result<Vec<Instrument>> {
        let instruments_array = result
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("Expected array of instruments"))?;

        let mut instruments = Vec::new();

        for item in instruments_array {
            let name = item["instrument_name"].as_str().unwrap_or("").to_string();
            let strike = item["strike"].as_f64().unwrap_or(0.0);
            let expiration = item["expiration_timestamp"].as_i64().unwrap_or(0);
            let option_type_str = item["option_type"].as_str().unwrap_or("");
            let is_active = item["is_active"].as_bool().unwrap_or(false);

            let option_type = match option_type_str {
                "call" => OptionType::Call,
                "put" => OptionType::Put,
                _ => continue,
            };

            if !is_active {
                continue;
            }

            instruments.push(Instrument {
                instrument_name: name,
                strike,
                expiration_timestamp: expiration,
                option_type,
                is_active,
            });
        }

        Ok(instruments)
    }

    pub async fn replace_all(&self, instruments: Vec<Instrument>) -> usize {
        let mut registry = self.instruments.write().await;
        registry.clear();

        for instrument in instruments {
            registry.insert(instrument.instrument_name.clone(), instrument);
        }

        let count = registry.len();
        info!(count = count, "Loaded option instruments");
        count
    }

    pub async fn get(&self, name: &str) -> Option<Instrument> {
        self.instruments.read().await.get(name).cloned()
    }

    pub async fn get_all(&self) -> Vec<Instrument> {
        self.instruments.read().await.values().cloned().collect()
    }

    pub async fn get_all_names(&self) -> Vec<String> {
        self.instruments.read().await.keys().cloned().collect()
    }

    /// Find matching call/put pair for a given strike and expiration
    pub async fn find_pair(
        &self,
        strike: f64,
        expiration: i64,
    ) -> Option<(Instrument, Instrument)> {
        let registry = self.instruments.read().await;
        let mut call = None;
        let mut put = None;

        for inst in registry.values() {
            if (inst.strike - strike).abs() < 0.01 && inst.expiration_timestamp == expiration {
                match inst.option_type {
                    OptionType::Call => call = Some(inst.clone()),
                    OptionType::Put => put = Some(inst.clone()),
                }
            }
        }

        match (call, put) {
            (Some(c), Some(p)) => Some((c, p)),
            _ => None,
        }
    }

    /// Get unique expirations
    pub async fn get_expirations(&self) -> Vec<i64> {
        let registry = self.instruments.read().await;
        let exp_set: HashSet<i64> = registry.values().map(|i| i.expiration_timestamp).collect();
        let mut expirations: Vec<i64> = exp_set.into_iter().collect();
        expirations.sort();
        expirations
    }

    /// Get strikes for a specific expiration
    pub async fn get_strikes_for_expiration(&self, expiration: i64) -> Vec<f64> {
        let registry = self.instruments.read().await;
        let strike_set: HashSet<u64> = registry
            .values()
            .filter(|i| i.expiration_timestamp == expiration)
            .map(|i| i.strike as u64)
            .collect();
        let mut strikes: Vec<f64> = strike_set.into_iter().map(|s| s as f64).collect();
        strikes.sort_by(|a, b| a.partial_cmp(b).unwrap());
        strikes
    }
}

pub fn option_price_currency(instrument_name: &str) -> OptionPriceCurrency {
    let base = instrument_name.split('-').next().unwrap_or_default();
    if base.contains('_') {
        OptionPriceCurrency::QuoteCurrency
    } else {
        OptionPriceCurrency::BaseAsset
    }
}

pub fn option_premium_usd(
    instrument_name: &str,
    option_price: f64,
    underlying_price: f64,
) -> Option<f64> {
    if option_price <= 0.0 || underlying_price <= 0.0 {
        return None;
    }

    Some(match option_price_currency(instrument_name) {
        OptionPriceCurrency::BaseAsset => option_price * underlying_price,
        OptionPriceCurrency::QuoteCurrency => option_price,
    })
}

pub fn option_index_name(instrument_name: &str) -> Option<String> {
    let base = instrument_name.split('-').next()?;
    if base.contains('_') {
        Some(base.to_lowercase())
    } else {
        Some(format!("{}_usd", base.to_lowercase()))
    }
}
