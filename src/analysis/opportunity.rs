use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeLeg {
    pub step: usize,
    pub action: Action,
    pub instrument: String,
    pub price: f64,
    pub amount: f64,
    pub price_unit: PriceUnit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Action {
    Buy,
    Sell,
}

impl std::fmt::Display for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Action::Buy => write!(f, "BUY"),
            Action::Sell => write!(f, "SELL"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PriceUnit {
    Btc,
    Usd,
}

impl std::fmt::Display for PriceUnit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PriceUnit::Btc => write!(f, "BTC"),
            PriceUnit::Usd => write!(f, "USD"),
        }
    }
}

impl TradeLeg {
    pub fn buy(step: usize, instrument: &str, price: f64, amount: f64) -> Self {
        TradeLeg {
            step,
            action: Action::Buy,
            instrument: instrument.to_string(),
            price,
            amount,
            price_unit: PriceUnit::Btc,
        }
    }

    pub fn sell(step: usize, instrument: &str, price: f64, amount: f64) -> Self {
        TradeLeg {
            step,
            action: Action::Sell,
            instrument: instrument.to_string(),
            price,
            amount,
            price_unit: PriceUnit::Btc,
        }
    }

    pub fn with_usd(mut self) -> Self {
        self.price_unit = PriceUnit::Usd;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Opportunity {
    pub strategy_type: String,
    pub description: String,
    pub legs: Vec<TradeLeg>,
    pub expected_profit: f64,
    pub total_cost: f64,
    pub risk_level: RiskLevel,
    pub instruments: Vec<String>,
    pub detected_at: i64,
    /// Expiration timestamp in milliseconds (for annualized return calculation)
    pub expiry_timestamp: Option<i64>,
}

impl Opportunity {
    /// Calculate annualized return based on profit, cost, and time to expiry
    /// Annualized return = ROI × (365 / days_to_expiry)
    /// Uses detected_at as baseline for stable calculation.
    /// Returns None for sub-day durations (annualizing is meaningless).
    pub fn annualized_return(&self) -> Option<f64> {
        if self.total_cost <= 0.0 || self.expected_profit <= 0.0 {
            return None;
        }
        let expiry_ms = self.expiry_timestamp?;
        let detected_ms = self.detected_at * 1000; // detected_at is in seconds
        let days = (expiry_ms - detected_ms) as f64 / 86_400_000.0;
        if days < 1.0 {
            return None;
        }
        let roi = self.expected_profit / self.total_cost;
        Some(roi * 365.0 / days)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RiskLevel {
    Low,
    Medium,
    High,
}

impl std::fmt::Display for RiskLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RiskLevel::Low => write!(f, "low"),
            RiskLevel::Medium => write!(f, "medium"),
            RiskLevel::High => write!(f, "high"),
        }
    }
}
