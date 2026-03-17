use tracing::info;

use crate::analysis::opportunity::{Opportunity, RiskLevel, TradeLeg};
use crate::market::instruments::InstrumentRegistry;
use crate::market::ticker::TickerCache;

/// Calendar Arbitrage (Hard Constraint)
///
/// For same strike, same type:
///   - American options: far-month price >= near-month price (always)
///   - European options (Deribit): same constraint holds for calls in USD terms
///
/// Deribit BTC options are European, inverse (BTC-settled).
/// In USD terms: C_USD(T2) >= C_USD(T1) for T2 > T1
/// Since price_BTC × S = price_USD:
///   price_BTC(T2) × S >= price_BTC(T1) × S
///   → price_BTC(T2) >= price_BTC(T1) (same underlying price)
///
/// If near_bid > far_ask → sell near, buy far → riskless
pub struct CalendarArbAnalyzer {
    min_profit_usd: f64,
}

impl CalendarArbAnalyzer {
    pub fn new(min_profit_usd: f64) -> Self {
        CalendarArbAnalyzer { min_profit_usd }
    }

    pub async fn scan(
        &self,
        registry: &InstrumentRegistry,
        ticker_cache: &TickerCache,
    ) -> Vec<Opportunity> {
        let mut opportunities = Vec::new();
        let all_instruments = registry.get_all().await;

        // Group by (strike_as_u64, option_type)
        let mut groups: std::collections::HashMap<
            (u64, String),
            Vec<(i64, String, f64, f64, f64)>, // (expiry, name, bid, ask, underlying)
        > = std::collections::HashMap::new();

        for inst in &all_instruments {
            if let Some(ticker) = ticker_cache.get(&inst.instrument_name).await {
                let bid = match ticker.best_bid_price {
                    Some(b) if b > 0.0 => b,
                    _ => continue,
                };
                let ask = match ticker.best_ask_price {
                    Some(a) if a > 0.0 => a,
                    _ => continue,
                };
                let underlying = ticker.underlying_price;
                if underlying <= 0.0 {
                    continue;
                }

                let key = (inst.strike as u64, inst.option_type.to_string());
                groups.entry(key).or_default().push((
                    inst.expiration_timestamp,
                    inst.instrument_name.clone(),
                    bid,
                    ask,
                    underlying,
                ));
            }
        }

        let fee_per_leg = 0.0003;

        for ((strike, opt_type), mut entries) in groups {
            if entries.len() < 2 {
                continue;
            }
            // Sort by expiration (near first)
            entries.sort_by_key(|e| e.0);

            for pair in entries.windows(2) {
                let (_exp_near, ref name_near, bid_near, _ask_near, underlying) = pair[0];
                let (_exp_far, ref name_far, _bid_far, ask_far, _) = pair[1];

                // Violation: near_bid > far_ask
                // → Sell near-month, Buy far-month
                let profit_btc = bid_near - ask_far - fee_per_leg * 2.0;

                if profit_btc > 0.0 {
                    let profit_usd = profit_btc * underlying;
                    if profit_usd > self.min_profit_usd {
                        let near_days = (pair[0].0 - chrono::Utc::now().timestamp_millis()) as f64
                            / (24.0 * 3600.0 * 1000.0);
                        let far_days = (pair[1].0 - chrono::Utc::now().timestamp_millis()) as f64
                            / (24.0 * 3600.0 * 1000.0);

                        info!(
                            strike = strike,
                            near = %name_near,
                            far = %name_far,
                            profit = profit_usd,
                            "Calendar arbitrage: near priced above far"
                        );

                        opportunities.push(Opportunity {
                            strategy_type: "calendar_arb".to_string(),
                            description: format!(
                                "Calendar arb {} K={} | Near ({:.0}d) bid {:.6} > Far ({:.0}d) ask {:.6}",
                                opt_type, strike, near_days, bid_near, far_days, ask_far
                            ),
                            legs: vec![
                                TradeLeg::sell(1, name_near, bid_near, 1.0),
                                TradeLeg::buy(2, name_far, ask_far, 1.0),
                            ],
                            expected_profit: profit_usd,
                            total_cost: ask_far * underlying,
                            risk_level: RiskLevel::Low,
                            instruments: vec![name_near.clone(), name_far.clone()],
                            detected_at: chrono::Utc::now().timestamp(),
                            expiry_timestamp: Some(pair[1].0),
                        });
                    }
                }
            }
        }

        opportunities
    }
}
