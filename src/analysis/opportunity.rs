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
    Usdc,
}

impl std::fmt::Display for PriceUnit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PriceUnit::Btc => write!(f, "BTC"),
            PriceUnit::Usd => write!(f, "USD"),
            PriceUnit::Usdc => write!(f, "USDC"),
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

    pub fn with_usdc(mut self) -> Self {
        self.price_unit = PriceUnit::Usdc;
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
    /// Total USD notional of futures legs (identified by PriceUnit::Usd)
    pub fn futures_notional_usd(&self) -> f64 {
        self.legs
            .iter()
            .filter(|l| matches!(l.price_unit, PriceUnit::Usd))
            .map(|l| l.price * l.amount)
            .sum()
    }

    /// Net futures delta in BTC (positive = long, negative = short)
    pub fn futures_delta_btc(&self) -> f64 {
        self.legs
            .iter()
            .filter(|l| matches!(l.price_unit, PriceUnit::Usd))
            .map(|l| {
                let sign = match l.action {
                    Action::Buy => 1.0,
                    Action::Sell => -1.0,
                };
                sign * l.amount
            })
            .sum()
    }

    /// Cost adjusted for leverage (only futures margin is reduced)
    pub fn leveraged_cost(&self, leverage: f64) -> f64 {
        let base = self.total_cost.abs();
        if leverage <= 1.0 {
            return base;
        }
        let futures_usd = self.futures_notional_usd();
        let option_cost = (base - futures_usd).max(0.0);
        option_cost + futures_usd / leverage
    }

    /// Annualized return with leverage adjustment
    pub fn annualized_return_leveraged(&self, leverage: f64) -> Option<f64> {
        if self.expected_profit <= 0.0 {
            return None;
        }
        let cost = self.leveraged_cost(leverage);
        if cost < 1.0 {
            return None;
        }
        let expiry_ms = self.expiry_timestamp?;
        let detected_ms = self.detected_at * 1000;
        let days = (expiry_ms - detected_ms) as f64 / 86_400_000.0;
        if days < 1.0 {
            return None;
        }
        Some((self.expected_profit / cost) * 365.0 / days)
    }

    /// Calculate annualized return at 1x leverage
    pub fn annualized_return(&self) -> Option<f64> {
        self.annualized_return_leveraged(1.0)
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
