use tracing::info;

use crate::analysis::opportunity::{Opportunity, RiskLevel, TradeLeg};
use crate::market::instruments::InstrumentRegistry;
use crate::market::ticker::TickerCache;

/// Put-Call Parity Arbitrage
/// C - P = S - K × e^(-rT)
/// Deribit prices in BTC: C - P = 1 - (K/S) × e^(-rT)
pub struct PutCallParityAnalyzer {
    threshold: f64,
    risk_free_rate: f64,
}

impl PutCallParityAnalyzer {
    pub fn new(threshold: f64) -> Self {
        PutCallParityAnalyzer {
            threshold,
            risk_free_rate: 0.05,
        }
    }

    pub async fn check_pair(
        &self,
        registry: &InstrumentRegistry,
        ticker_cache: &TickerCache,
        strike: f64,
        expiration: i64,
    ) -> Option<Opportunity> {
        let (call_inst, put_inst) = registry.find_pair(strike, expiration).await?;

        let call_ticker = ticker_cache.get(&call_inst.instrument_name).await?;
        let put_ticker = ticker_cache.get(&put_inst.instrument_name).await?;

        let call_bid = call_ticker.best_bid_price.filter(|&p| p > 0.0)?;
        let call_ask = call_ticker.best_ask_price.filter(|&p| p > 0.0)?;
        let put_bid = put_ticker.best_bid_price.filter(|&p| p > 0.0)?;
        let put_ask = put_ticker.best_ask_price.filter(|&p| p > 0.0)?;

        let underlying = call_ticker.underlying_price;
        if underlying <= 0.0 {
            return None;
        }

        let now = chrono::Utc::now().timestamp_millis();
        let time_to_expiry = (expiration - now) as f64 / (365.25 * 24.0 * 3600.0 * 1000.0);
        if time_to_expiry <= 0.0 {
            return None;
        }

        let discount = (-self.risk_free_rate * time_to_expiry).exp();
        let theoretical_diff = 1.0 - (strike / underlying) * discount;
        // Fee: 2 option legs × 0.03% + 1 futures leg × 0.05%
        let fee = 0.0003 * 2.0 + 0.0005;

        // Direction 1: Buy call + Sell put (synthetic long) + Sell underlying to hedge
        // Synthetic long is cheap → buy it, sell actual underlying to lock in diff
        let market_diff_1 = call_ask - put_bid;
        let profit_1 = theoretical_diff - market_diff_1;

        if profit_1 > self.threshold + fee {
            let profit_usd = (profit_1 - fee) * underlying;
            // Total capital: option net cost + futures margin
            // Option net: market_diff_1 * S (negative = received), futures: sell at S
            let total_cost_usd = (market_diff_1.abs() + 1.0) * underlying;

            info!(
                call = %call_inst.instrument_name,
                put = %put_inst.instrument_name,
                profit_usd = profit_usd,
                "PCP: Buy call + Sell put + Sell underlying"
            );

            return Some(Opportunity {
                strategy_type: "put_call_parity".to_string(),
                description: format!(
                    "Synthetic long underpriced | K={} | {} days",
                    strike,
                    (time_to_expiry * 365.25) as i32
                ),
                legs: vec![
                    TradeLeg::buy(1, &call_inst.instrument_name, call_ask, 1.0),
                    TradeLeg::sell(2, &put_inst.instrument_name, put_bid, 1.0),
                    TradeLeg::sell(3, "BTC-PERPETUAL", underlying, 1.0).with_usd(),
                ],
                expected_profit: profit_usd,
                total_cost: total_cost_usd,
                risk_level: RiskLevel::Low,
                instruments: vec![
                    call_inst.instrument_name.clone(),
                    put_inst.instrument_name.clone(),
                    "BTC-PERPETUAL".to_string(),
                ],
                detected_at: chrono::Utc::now().timestamp(),
                expiry_timestamp: Some(expiration),
            });
        }

        // Direction 2: Sell call + Buy put (synthetic short) + Buy underlying to hedge
        // Synthetic short is expensive → sell it, buy actual underlying to lock in diff
        let market_diff_2 = call_bid - put_ask;
        let profit_2 = market_diff_2 - theoretical_diff;

        if profit_2 > self.threshold + fee {
            let profit_usd = (profit_2 - fee) * underlying;
            let total_cost_usd = (market_diff_2.abs() + 1.0) * underlying;

            info!(
                call = %call_inst.instrument_name,
                put = %put_inst.instrument_name,
                profit_usd = profit_usd,
                "PCP: Sell call + Buy put + Buy underlying"
            );

            return Some(Opportunity {
                strategy_type: "put_call_parity".to_string(),
                description: format!(
                    "Synthetic short overpriced | K={} | {} days",
                    strike,
                    (time_to_expiry * 365.25) as i32
                ),
                legs: vec![
                    TradeLeg::sell(1, &call_inst.instrument_name, call_bid, 1.0),
                    TradeLeg::buy(2, &put_inst.instrument_name, put_ask, 1.0),
                    TradeLeg::buy(3, "BTC-PERPETUAL", underlying, 1.0).with_usd(),
                ],
                expected_profit: profit_usd,
                total_cost: total_cost_usd,
                risk_level: RiskLevel::Low,
                instruments: vec![
                    call_inst.instrument_name.clone(),
                    put_inst.instrument_name.clone(),
                    "BTC-PERPETUAL".to_string(),
                ],
                detected_at: chrono::Utc::now().timestamp(),
                expiry_timestamp: Some(expiration),
            });
        }

        None
    }

    pub async fn scan_all(
        &self,
        registry: &InstrumentRegistry,
        ticker_cache: &TickerCache,
    ) -> Vec<Opportunity> {
        let mut opportunities = Vec::new();
        let expirations = registry.get_expirations().await;

        for expiration in &expirations {
            let strikes = registry.get_strikes_for_expiration(*expiration).await;
            for strike in &strikes {
                if let Some(opp) = self
                    .check_pair(registry, ticker_cache, *strike, *expiration)
                    .await
                {
                    opportunities.push(opp);
                }
            }
        }

        opportunities
    }
}
