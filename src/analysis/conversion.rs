use tracing::info;

use crate::analysis::opportunity::{Opportunity, RiskLevel, TradeLeg};
use crate::market::instruments::InstrumentRegistry;
use crate::market::ticker::TickerCache;

/// Conversion / Reversal Arbitrage (r=0, no interest rate assumption)
///
/// Conversion: Buy underlying + Buy Put(K) + Sell Call(K)
///   - At expiry, USD value = K regardless of BTC price
///   - Cost(USD) = S × (1 + P_ask - C_bid)
///   - If cost < K → profit (at r=0)
///
/// Reversal: Sell underlying + Sell Put(K) + Buy Call(K)
///   - Revenue(USD) = S × (1 - C_ask + P_bid)
///   - If revenue > K → profit (at r=0)
///
/// Note: r=0 means conversion requires K > cost (no discounting),
/// which is LESS conservative than positive r (makes it easier to
/// trigger). For production, use same-expiry futures as forward price.
pub struct ConversionAnalyzer {
    min_profit_usd: f64,
}

impl ConversionAnalyzer {
    pub fn new(min_profit_usd: f64) -> Self {
        ConversionAnalyzer { min_profit_usd }
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
            let now = chrono::Utc::now().timestamp_millis();
            let time_to_expiry =
                (expiration - now) as f64 / (365.25 * 24.0 * 3600.0 * 1000.0);
            if time_to_expiry <= 0.0 {
                continue;
            }
            for strike in &strikes {
                if let Some(opp) = self
                    .check_strike(
                        registry,
                        ticker_cache,
                        *strike,
                        *expiration,
                        time_to_expiry,
                    )
                    .await
                {
                    opportunities.push(opp);
                }
            }
        }

        opportunities
    }

    async fn check_strike(
        &self,
        registry: &InstrumentRegistry,
        ticker_cache: &TickerCache,
        strike: f64,
        expiration: i64,
        time_to_expiry: f64,
    ) -> Option<Opportunity> {
        let (call_inst, put_inst) = registry.find_pair(strike, expiration).await?;

        let call_ticker = ticker_cache.get(&call_inst.instrument_name).await?;
        let put_ticker = ticker_cache.get(&put_inst.instrument_name).await?;

        let c_bid = call_ticker.best_bid_price.filter(|&p| p > 0.0)?;
        let c_ask = call_ticker.best_ask_price.filter(|&p| p > 0.0)?;
        let p_bid = put_ticker.best_bid_price.filter(|&p| p > 0.0)?;
        let p_ask = put_ticker.best_ask_price.filter(|&p| p > 0.0)?;

        let s = call_ticker.underlying_price;
        if s <= 0.0 {
            return None;
        }

        let days = (time_to_expiry * 365.25) as i32;
        // 3 legs: underlying + put + call, each 0.03% fee (futures ~0.05%)
        let fee_usd = s * 0.0003 * 2.0 + s * 0.0005; // 2 option legs + 1 futures leg

        // === Conversion ===
        // Buy 1 BTC (or long futures), Buy Put(K), Sell Call(K)
        // Cost in BTC: 1 + P_ask - C_bid
        // Cost in USD: S × (1 + P_ask - C_bid)
        // Guaranteed payoff at expiry: K (USD), no discounting (r=0)
        let conv_cost_btc = 1.0 + p_ask - c_bid;
        let conv_cost_usd = s * conv_cost_btc;
        let conv_payoff = strike; // r=0: no present-value discount
        let conv_profit = conv_payoff - conv_cost_usd - fee_usd;

        if conv_profit > self.min_profit_usd {
            info!(
                strike = strike,
                cost = conv_cost_usd,
                payoff = conv_payoff,
                profit = conv_profit,
                "Conversion arbitrage"
            );
            return Some(Opportunity {
                strategy_type: "conversion".to_string(),
                description: format!(
                    "Conversion at K={} | Cost ${:.2} → Payoff ${:.2} | {} days",
                    strike, conv_cost_usd, conv_payoff, days
                ),
                legs: vec![
                    TradeLeg::buy(1, "BTC-PERPETUAL", s, 1.0).with_usd(),
                    TradeLeg::buy(2, &put_inst.instrument_name, p_ask, 1.0),
                    TradeLeg::sell(3, &call_inst.instrument_name, c_bid, 1.0),
                ],
                expected_profit: conv_profit,
                total_cost: conv_cost_usd,
                risk_level: RiskLevel::Low,
                instruments: vec![
                    "BTC-PERPETUAL".to_string(),
                    put_inst.instrument_name.clone(),
                    call_inst.instrument_name.clone(),
                ],
                detected_at: chrono::Utc::now().timestamp(),
                expiry_timestamp: Some(expiration),
            });
        }

        // === Reversal ===
        // Sell 1 BTC (or short futures), Sell Put(K), Buy Call(K)
        // Revenue in BTC: 1 + P_bid - C_ask
        // Revenue in USD: S × (1 + P_bid - C_ask)
        // Guaranteed liability at expiry: K (USD), no discounting (r=0)
        let rev_revenue_btc = 1.0 + p_bid - c_ask;
        let rev_revenue_usd = s * rev_revenue_btc;
        let rev_liability = strike; // r=0: no present-value discount
        let rev_profit = rev_revenue_usd - rev_liability - fee_usd;

        if rev_profit > self.min_profit_usd {
            info!(
                strike = strike,
                revenue = rev_revenue_usd,
                liability = rev_liability,
                profit = rev_profit,
                "Reversal arbitrage"
            );
            return Some(Opportunity {
                strategy_type: "reversal".to_string(),
                description: format!(
                    "Reversal at K={} | Revenue ${:.2} → Liability ${:.2} | {} days",
                    strike, rev_revenue_usd, rev_liability, days
                ),
                legs: vec![
                    TradeLeg::sell(1, "BTC-PERPETUAL", s, 1.0).with_usd(),
                    TradeLeg::sell(2, &put_inst.instrument_name, p_bid, 1.0),
                    TradeLeg::buy(3, &call_inst.instrument_name, c_ask, 1.0),
                ],
                expected_profit: rev_profit,
                total_cost: rev_revenue_usd.abs(),
                risk_level: RiskLevel::Low,
                instruments: vec![
                    "BTC-PERPETUAL".to_string(),
                    put_inst.instrument_name.clone(),
                    call_inst.instrument_name.clone(),
                ],
                detected_at: chrono::Utc::now().timestamp(),
                expiry_timestamp: Some(expiration),
            });
        }

        None
    }
}
