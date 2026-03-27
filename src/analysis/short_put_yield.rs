use tracing::info;

use crate::analysis::opportunity::{Opportunity, RiskLevel, TradeLeg};
use crate::market::instruments::{
    option_fee_usd, option_premium_usd, option_price_currency, InstrumentRegistry,
    OptionPriceCurrency, OptionType,
};
use crate::market::ticker::TickerCache;

/// Lists cash-secured short-put yields using the live bid as premium received.
///
/// Assumption:
/// - The put expires worthless (no exercise), so the seller keeps the full premium.
/// - Annualized yield = premium_received_usd / strike_usd * 365 / days_to_expiry
pub struct ShortPutYieldAnalyzer;

impl ShortPutYieldAnalyzer {
    pub fn new() -> Self {
        Self
    }

    pub async fn scan(
        &self,
        registry: &InstrumentRegistry,
        ticker_cache: &TickerCache,
    ) -> Vec<Opportunity> {
        let mut opportunities = Vec::new();
        let now = chrono::Utc::now().timestamp();

        for inst in registry.get_all().await {
            if inst.option_type != OptionType::Put {
                continue;
            }

            if inst.expiration_timestamp <= now * 1000 {
                continue;
            }

            let Some(ticker) = ticker_cache.get(&inst.instrument_name).await else {
                continue;
            };

            let Some(best_bid_price) = ticker.best_bid_price.filter(|&p| p > 0.0) else {
                continue;
            };

            if ticker.underlying_price <= 0.0 || inst.strike <= 0.0 {
                continue;
            }

            if inst.strike > ticker.underlying_price {
                continue;
            }

            let Some(premium_usd) = option_premium_usd(
                &inst.instrument_name,
                best_bid_price,
                ticker.underlying_price,
            ) else {
                continue;
            };
            if premium_usd < 1.0 {
                continue;
            }

            let Some(fee_usd) = option_fee_usd(
                &inst.instrument_name,
                best_bid_price,
                ticker.underlying_price,
                1.0,
            ) else {
                continue;
            };
            let net_premium_usd = premium_usd - fee_usd;

            let expiry_label = chrono::DateTime::from_timestamp_millis(inst.expiration_timestamp)
                .map(|dt| dt.format("%Y-%m-%d").to_string())
                .unwrap_or_else(|| "unknown".to_string());
            let annualized = (net_premium_usd / inst.strike) * 365.0
                / ((inst.expiration_timestamp - now * 1000) as f64 / 86_400_000.0);

            info!(
                instrument = %inst.instrument_name,
                strike = inst.strike,
                premium_usd = premium_usd,
                fee_usd = fee_usd,
                net_premium_usd = net_premium_usd,
                annualized = annualized,
                "Short put yield detected"
            );

            let leg = match option_price_currency(&inst.instrument_name) {
                OptionPriceCurrency::BaseAsset => {
                    TradeLeg::sell(1, &inst.instrument_name, best_bid_price, 1.0)
                }
                OptionPriceCurrency::QuoteCurrency => {
                    TradeLeg::sell(1, &inst.instrument_name, best_bid_price, 1.0).with_usdc()
                }
            };

            opportunities.push(Opportunity {
                strategy_type: "short_put_yield".to_string(),
                description: format!(
                    "{} SPUT K={:.0} <= ${:.0} | APY {:.1}%",
                    expiry_label,
                    inst.strike,
                    ticker.underlying_price,
                    annualized * 100.0,
                ),
                legs: vec![leg],
                expected_profit: net_premium_usd,
                total_cost: inst.strike,
                risk_level: RiskLevel::High,
                instruments: vec![inst.instrument_name.clone()],
                detected_at: now,
                expiry_timestamp: Some(inst.expiration_timestamp),
            });
        }

        opportunities
    }
}
