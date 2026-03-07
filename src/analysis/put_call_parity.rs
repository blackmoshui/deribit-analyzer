use tracing::info;

use crate::analysis::opportunity::{Opportunity, RiskLevel, TradeLeg};
use crate::market::instruments::InstrumentRegistry;
use crate::market::ticker::TickerCache;

/// Put-Call Parity Arbitrage (no interest rate assumption)
///
/// Deribit BTC-settled: C - P = 1 - K/S (at r=0)
///
/// Only triggers when the options market genuinely misprices C vs P
/// relative to spot. Does NOT assume funding income from perpetual.
///
/// For true risk-free arb: use same-expiry futures as hedge instead
/// of perpetual (TODO: subscribe to BTC quarterly futures).
pub struct PutCallParityAnalyzer {
    threshold: f64,
}

impl PutCallParityAnalyzer {
    pub fn new(threshold: f64) -> Self {
        PutCallParityAnalyzer { threshold }
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

        // r=0: no funding assumption. Only genuine options mispricing triggers.
        // theoretical_diff = 1 - K/S (what C - P should equal at r=0)
        let theoretical_diff = 1.0 - strike / underlying;

        // Fee: 2 option legs × 0.03% + 1 futures leg × 0.05%
        let fee = 0.0003 * 2.0 + 0.0005;

        // Direction 1: Buy call + Sell put (synthetic long) + Sell underlying
        // Profitable when: call is cheap relative to put (synthetic long underpriced)
        let market_diff_1 = call_ask - put_bid;
        let profit_1 = theoretical_diff - market_diff_1;

        if profit_1 > self.threshold + fee {
            let profit_usd = (profit_1 - fee) * underlying;
            let total_cost_usd = (market_diff_1.abs() + 1.0) * underlying;

            info!(
                call = %call_inst.instrument_name,
                put = %put_inst.instrument_name,
                profit_btc = profit_1 - fee,
                profit_usd = profit_usd,
                theoretical = theoretical_diff,
                market = market_diff_1,
                "PCP: Call cheap vs Put → Buy C + Sell P + Sell underlying"
            );

            return Some(Opportunity {
                strategy_type: "put_call_parity".to_string(),
                description: format!(
                    "Call underpriced vs Put | K={} | {:.0} days | C-P={:.4} vs theo {:.4}",
                    strike,
                    time_to_expiry * 365.25,
                    market_diff_1,
                    theoretical_diff,
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

        // Direction 2: Sell call + Buy put (synthetic short) + Buy underlying
        // Profitable when: call is expensive relative to put (synthetic short overpriced)
        let market_diff_2 = call_bid - put_ask;
        let profit_2 = market_diff_2 - theoretical_diff;

        if profit_2 > self.threshold + fee {
            let profit_usd = (profit_2 - fee) * underlying;
            let total_cost_usd = (market_diff_2.abs() + 1.0) * underlying;

            info!(
                call = %call_inst.instrument_name,
                put = %put_inst.instrument_name,
                profit_btc = profit_2 - fee,
                profit_usd = profit_usd,
                theoretical = theoretical_diff,
                market = market_diff_2,
                "PCP: Call expensive vs Put → Sell C + Buy P + Buy underlying"
            );

            return Some(Opportunity {
                strategy_type: "put_call_parity".to_string(),
                description: format!(
                    "Call overpriced vs Put | K={} | {:.0} days | C-P={:.4} vs theo {:.4}",
                    strike,
                    time_to_expiry * 365.25,
                    market_diff_2,
                    theoretical_diff,
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
