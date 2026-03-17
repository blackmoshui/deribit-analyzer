use tracing::info;

use crate::analysis::opportunity::{Opportunity, RiskLevel, TradeLeg};
use crate::market::instruments::{InstrumentRegistry, OptionType};
use crate::market::ticker::TickerCache;

/// Vertical Spread Arbitrage
///
/// Monotonicity constraint violations:
///   - Calls: C(K1) >= C(K2) when K1 < K2  (lower strike call is worth more)
///   - Puts:  P(K1) <= P(K2) when K1 < K2  (higher strike put is worth more)
///
/// If ask(cheap) < bid(expensive), there's a riskless profit.
///
/// Also checks convexity (butterfly) constraint on prices:
///   C(K2) <= λ×C(K1) + (1-λ)×C(K3) where K2 = λ×K1 + (1-λ)×K3
///   Violation → negative butterfly price → buy butterfly for free money
pub struct VerticalArbAnalyzer {
    min_profit_usd: f64,
}

impl VerticalArbAnalyzer {
    pub fn new(min_profit_usd: f64) -> Self {
        VerticalArbAnalyzer { min_profit_usd }
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

            let all_instruments = registry.get_all().await;

            // Collect call and put data for this expiration
            let mut calls: Vec<(f64, String, f64, f64)> = Vec::new(); // (strike, name, bid, ask)
            let mut puts: Vec<(f64, String, f64, f64)> = Vec::new();

            for inst in &all_instruments {
                if inst.expiration_timestamp != *expiration {
                    continue;
                }
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

                    let entry = (inst.strike, inst.instrument_name.clone(), bid, ask);
                    match inst.option_type {
                        OptionType::Call => calls.push(entry),
                        OptionType::Put => puts.push(entry),
                    }
                }
            }

            calls.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
            puts.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

            // Get underlying price from any ticker
            let underlying = if let Some(first) = calls.first() {
                if let Some(t) = ticker_cache.get(&first.1).await {
                    t.underlying_price
                } else {
                    continue;
                }
            } else {
                continue;
            };

            let fee_per_leg = 0.0003;

            // === Call monotonicity ===
            // For K1 < K2: C(K1) >= C(K2)
            // Arb: if C_ask(K1) < C_bid(K2) → buy C(K1), sell C(K2)
            for i in 0..calls.len() {
                for j in (i + 1)..calls.len() {
                    let (k1, ref name1, _bid1, ask1) = calls[i];
                    let (k2, ref name2, bid2, _ask2) = calls[j];

                    let profit_btc = bid2 - ask1 - fee_per_leg * 2.0;
                    let profit_usd = profit_btc * underlying;

                    if profit_usd > self.min_profit_usd {
                        info!(
                            k1 = k1,
                            k2 = k2,
                            profit = profit_usd,
                            "Call monotonicity violation"
                        );
                        opportunities.push(Opportunity {
                            strategy_type: "vertical_arb".to_string(),
                            description: format!(
                                "Call monotonicity: C({}) ask {:.6} < C({}) bid {:.6} | Free ${:.2}",
                                k1, ask1, k2, bid2, profit_usd
                            ),
                            legs: vec![
                                TradeLeg::buy(1, name1, ask1, 1.0),
                                TradeLeg::sell(2, name2, bid2, 1.0),
                            ],
                            expected_profit: profit_usd,
                            total_cost: ask1 * underlying,
                            risk_level: RiskLevel::Low,
                            instruments: vec![name1.clone(), name2.clone()],
                            detected_at: chrono::Utc::now().timestamp(),
                            expiry_timestamp: Some(*expiration),
                        });
                    }
                }
            }

            // === Put monotonicity ===
            // For K1 < K2: P(K1) <= P(K2)
            // Arb: if P_ask(K2) < P_bid(K1) → buy P(K2), sell P(K1)
            for i in 0..puts.len() {
                for j in (i + 1)..puts.len() {
                    let (k1, ref name1, bid1, _ask1) = puts[i];
                    let (k2, ref name2, _bid2, ask2) = puts[j];

                    let profit_btc = bid1 - ask2 - fee_per_leg * 2.0;
                    let profit_usd = profit_btc * underlying;

                    if profit_usd > self.min_profit_usd {
                        info!(
                            k1 = k1,
                            k2 = k2,
                            profit = profit_usd,
                            "Put monotonicity violation"
                        );
                        opportunities.push(Opportunity {
                            strategy_type: "vertical_arb".to_string(),
                            description: format!(
                                "Put monotonicity: P({}) bid {:.6} > P({}) ask {:.6} | Free ${:.2}",
                                k1, bid1, k2, ask2, profit_usd
                            ),
                            legs: vec![
                                TradeLeg::sell(1, name1, bid1, 1.0),
                                TradeLeg::buy(2, name2, ask2, 1.0),
                            ],
                            expected_profit: profit_usd,
                            total_cost: ask2 * underlying,
                            risk_level: RiskLevel::Low,
                            instruments: vec![name1.clone(), name2.clone()],
                            detected_at: chrono::Utc::now().timestamp(),
                            expiry_timestamp: Some(*expiration),
                        });
                    }
                }
            }

            // === Butterfly convexity (calls) ===
            // For 3 strikes K1 < K2 < K3:
            // λ = (K3-K2)/(K3-K1), C(K2) <= λ×C(K1) + (1-λ)×C(K3)
            // Buy butterfly: Buy C(K1), Sell 2×C(K2), Buy C(K3)
            // If net cost is negative → free money
            for window in calls.windows(3) {
                let (k1, ref n1, _b1, ask1) = window[0];
                let (k2, ref n2, bid2, _a2) = window[1];
                let (k3, ref n3, _b3, ask3) = window[2];

                // Buy butterfly cost: ask(K1) - 2×bid(K2) + ask(K3)
                let butterfly_cost = ask1 - 2.0 * bid2 + ask3;
                let fee = fee_per_leg * 4.0; // 4 legs (2× middle)
                let net_cost_btc = butterfly_cost + fee;

                if net_cost_btc < 0.0 {
                    let profit_usd = -net_cost_btc * underlying;
                    if profit_usd > self.min_profit_usd {
                        info!(
                            k1 = k1,
                            k2 = k2,
                            k3 = k3,
                            profit = profit_usd,
                            "Butterfly convexity violation (calls)"
                        );
                        opportunities.push(Opportunity {
                            strategy_type: "butterfly_arb".to_string(),
                            description: format!(
                                "Call butterfly {}/{}/{} has negative cost | Free ${:.2}",
                                k1, k2, k3, profit_usd
                            ),
                            legs: vec![
                                TradeLeg::buy(1, n1, ask1, 1.0),
                                TradeLeg::sell(2, n2, bid2, 2.0),
                                TradeLeg::buy(3, n3, ask3, 1.0),
                            ],
                            expected_profit: profit_usd,
                            total_cost: 0.0, // negative cost = receive premium
                            risk_level: RiskLevel::Low,
                            instruments: vec![n1.clone(), n2.clone(), n3.clone()],
                            detected_at: chrono::Utc::now().timestamp(),
                            expiry_timestamp: Some(*expiration),
                        });
                    }
                }
            }

            // === Butterfly convexity (puts) ===
            for window in puts.windows(3) {
                let (k1, ref n1, _b1, ask1) = window[0];
                let (k2, ref n2, bid2, _a2) = window[1];
                let (k3, ref n3, _b3, ask3) = window[2];

                let butterfly_cost = ask1 - 2.0 * bid2 + ask3;
                let fee = fee_per_leg * 4.0;
                let net_cost_btc = butterfly_cost + fee;

                if net_cost_btc < 0.0 {
                    let profit_usd = -net_cost_btc * underlying;
                    if profit_usd > self.min_profit_usd {
                        info!(
                            k1 = k1,
                            k2 = k2,
                            k3 = k3,
                            profit = profit_usd,
                            "Butterfly convexity violation (puts)"
                        );
                        opportunities.push(Opportunity {
                            strategy_type: "butterfly_arb".to_string(),
                            description: format!(
                                "Put butterfly {}/{}/{} has negative cost | Free ${:.2}",
                                k1, k2, k3, profit_usd
                            ),
                            legs: vec![
                                TradeLeg::buy(1, n1, ask1, 1.0),
                                TradeLeg::sell(2, n2, bid2, 2.0),
                                TradeLeg::buy(3, n3, ask3, 1.0),
                            ],
                            expected_profit: profit_usd,
                            total_cost: 0.0,
                            risk_level: RiskLevel::Low,
                            instruments: vec![n1.clone(), n2.clone(), n3.clone()],
                            detected_at: chrono::Utc::now().timestamp(),
                            expiry_timestamp: Some(*expiration),
                        });
                    }
                }
            }
        }

        opportunities
    }
}
