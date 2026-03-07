use tracing::info;

use crate::analysis::opportunity::{Opportunity, RiskLevel, TradeLeg};
use crate::market::instruments::InstrumentRegistry;
use crate::market::ticker::TickerCache;

/// Box Spread Arbitrage (no interest rate assumption)
///
/// Long Box: Buy C(K1) + Sell C(K2) + Buy P(K2) + Sell P(K1)
/// Guaranteed USD payoff at expiry = K2 - K1 (regardless of BTC price)
///
/// If cost(USD) < K2-K1 → profit (at r=0)
/// If cost(USD) > K2-K1 → do Short Box instead
///
/// Box spread is pure options (no futures), so no funding risk.
/// r=0 is conservative for long box (makes value look bigger → harder
/// for cost to beat) and aggressive for short box (revenue must beat
/// undiscounted value). Both are acceptable for screening.
pub struct BoxSpreadAnalyzer {
    min_profit_usd: f64,
}

impl BoxSpreadAnalyzer {
    pub fn new(min_profit_usd: f64) -> Self {
        BoxSpreadAnalyzer { min_profit_usd }
    }

    pub async fn scan(
        &self,
        registry: &InstrumentRegistry,
        ticker_cache: &TickerCache,
    ) -> Vec<Opportunity> {
        let mut opportunities = Vec::new();
        let expirations = registry.get_expirations().await;

        for expiration in &expirations {
            let strikes = registry.get_strikes_for_expiration(*expiration).await;
            if strikes.len() < 2 {
                continue;
            }

            let now = chrono::Utc::now().timestamp_millis();
            let time_to_expiry =
                (expiration - now) as f64 / (365.25 * 24.0 * 3600.0 * 1000.0);
            if time_to_expiry <= 0.0 {
                continue;
            }

            // Check all strike pairs (K1 < K2)
            for i in 0..strikes.len() {
                for j in (i + 1)..strikes.len() {
                    let k1 = strikes[i];
                    let k2 = strikes[j];

                    if let Some(opp) = self
                        .check_box(
                            registry,
                            ticker_cache,
                            k1,
                            k2,
                            *expiration,
                            time_to_expiry,
                        )
                        .await
                    {
                        opportunities.push(opp);
                    }
                }
            }
        }

        opportunities
    }

    async fn check_box(
        &self,
        registry: &InstrumentRegistry,
        ticker_cache: &TickerCache,
        k1: f64,
        k2: f64,
        expiration: i64,
        time_to_expiry: f64,
    ) -> Option<Opportunity> {
        // Need C(K1), C(K2), P(K1), P(K2)
        let (call_k1, put_k1) = registry.find_pair(k1, expiration).await?;
        let (call_k2, put_k2) = registry.find_pair(k2, expiration).await?;

        let tc_k1 = ticker_cache.get(&call_k1.instrument_name).await?;
        let tc_k2 = ticker_cache.get(&call_k2.instrument_name).await?;
        let tp_k1 = ticker_cache.get(&put_k1.instrument_name).await?;
        let tp_k2 = ticker_cache.get(&put_k2.instrument_name).await?;

        let c1_bid = tc_k1.best_bid_price.filter(|&p| p > 0.0)?;
        let c1_ask = tc_k1.best_ask_price.filter(|&p| p > 0.0)?;
        let c2_bid = tc_k2.best_bid_price.filter(|&p| p > 0.0)?;
        let c2_ask = tc_k2.best_ask_price.filter(|&p| p > 0.0)?;
        let p1_bid = tp_k1.best_bid_price.filter(|&p| p > 0.0)?;
        let p1_ask = tp_k1.best_ask_price.filter(|&p| p > 0.0)?;
        let p2_bid = tp_k2.best_bid_price.filter(|&p| p > 0.0)?;
        let p2_ask = tp_k2.best_ask_price.filter(|&p| p > 0.0)?;

        let underlying = tc_k1.underlying_price;
        if underlying <= 0.0 {
            return None;
        }

        // Theoretical box value (USD) = K2-K1 (r=0, no discounting)
        let box_value = k2 - k1;
        // Fee: 4 legs × 0.03% taker
        let fee_btc = 0.0003 * 4.0;
        let fee_usd = fee_btc * underlying;

        // Long Box cost (BTC): buy C(K1) at ask + sell C(K2) at bid + buy P(K2) at ask + sell P(K1) at bid
        let long_cost_btc = c1_ask - c2_bid + p2_ask - p1_bid;
        let long_cost_usd = long_cost_btc * underlying;
        let long_profit = box_value - long_cost_usd - fee_usd;

        if long_profit > self.min_profit_usd {
            let days = (time_to_expiry * 365.25) as i32;
            info!(
                k1 = k1, k2 = k2,
                cost = long_cost_usd,
                box_value = box_value,
                profit = long_profit,
                "Box Spread: Long box opportunity"
            );
            return Some(Opportunity {
                strategy_type: "box_spread".to_string(),
                description: format!(
                    "Long Box K1={} K2={} | Cost ${:.2} < Box value ${:.2} | Expiry {} days",
                    k1, k2, long_cost_usd, box_value, days
                ),
                legs: vec![
                    TradeLeg::buy(1, &call_k1.instrument_name, c1_ask, 1.0),
                    TradeLeg::sell(2, &call_k2.instrument_name, c2_bid, 1.0),
                    TradeLeg::buy(3, &put_k2.instrument_name, p2_ask, 1.0),
                    TradeLeg::sell(4, &put_k1.instrument_name, p1_bid, 1.0),
                ],
                expected_profit: long_profit,
                total_cost: long_cost_usd,
                risk_level: RiskLevel::Low,
                instruments: vec![
                    call_k1.instrument_name.clone(),
                    call_k2.instrument_name.clone(),
                    put_k2.instrument_name.clone(),
                    put_k1.instrument_name.clone(),
                ],
                detected_at: chrono::Utc::now().timestamp(),
                expiry_timestamp: Some(expiration),
            });
        }

        // Short Box revenue (BTC): sell C(K1) at bid + buy C(K2) at ask + sell P(K2) at bid + buy P(K1) at ask
        let short_revenue_btc = c1_bid - c2_ask + p2_bid - p1_ask;
        let short_revenue_usd = short_revenue_btc * underlying;
        // Short box: receive premium now, pay (K2-K1) at expiry
        // Profit = revenue - PV(payoff) = revenue - box_value
        let short_profit = short_revenue_usd - box_value - fee_usd;

        if short_profit > self.min_profit_usd {
            let days = (time_to_expiry * 365.25) as i32;
            info!(
                k1 = k1, k2 = k2,
                revenue = short_revenue_usd,
                box_value = box_value,
                profit = short_profit,
                "Box Spread: Short box opportunity"
            );
            return Some(Opportunity {
                strategy_type: "box_spread".to_string(),
                description: format!(
                    "Short Box K1={} K2={} | Revenue ${:.2} > Box value ${:.2} | Expiry {} days",
                    k1, k2, short_revenue_usd, box_value, days
                ),
                legs: vec![
                    TradeLeg::sell(1, &call_k1.instrument_name, c1_bid, 1.0),
                    TradeLeg::buy(2, &call_k2.instrument_name, c2_ask, 1.0),
                    TradeLeg::sell(3, &put_k2.instrument_name, p2_bid, 1.0),
                    TradeLeg::buy(4, &put_k1.instrument_name, p1_ask, 1.0),
                ],
                expected_profit: short_profit,
                total_cost: short_revenue_usd.abs(),
                risk_level: RiskLevel::Low,
                instruments: vec![
                    call_k1.instrument_name.clone(),
                    call_k2.instrument_name.clone(),
                    put_k2.instrument_name.clone(),
                    put_k1.instrument_name.clone(),
                ],
                detected_at: chrono::Utc::now().timestamp(),
                expiry_timestamp: Some(expiration),
            });
        }

        None
    }
}
