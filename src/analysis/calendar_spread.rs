use tracing::info;

use crate::analysis::opportunity::{Opportunity, RiskLevel, TradeLeg};
use crate::market::instruments::InstrumentRegistry;
use crate::market::ticker::TickerCache;

/// Calendar Spread (IV-based, soft signal)
/// Detects abnormal IV term structure for same-strike different-expiry options
pub struct CalendarSpreadAnalyzer {
    min_iv_diff: f64,
}

impl CalendarSpreadAnalyzer {
    pub fn new(min_iv_diff: f64) -> Self {
        CalendarSpreadAnalyzer { min_iv_diff }
    }

    pub async fn scan(
        &self,
        registry: &InstrumentRegistry,
        ticker_cache: &TickerCache,
    ) -> Vec<Opportunity> {
        let mut opportunities = Vec::new();
        let expirations = registry.get_expirations().await;

        if expirations.len() < 2 {
            return opportunities;
        }

        let all_instruments = registry.get_all().await;

        // Group by (strike, option_type)
        let mut groups: std::collections::HashMap<
            (u64, String),
            Vec<(i64, String, f64, f64, f64)>, // (exp, name, iv, bid, ask)
        > = std::collections::HashMap::new();

        for inst in &all_instruments {
            if let Some(ticker) = ticker_cache.get(&inst.instrument_name).await {
                if ticker.mark_iv > 0.0 {
                    let bid = match ticker.best_bid_price {
                        Some(b) if b > 0.0 => b,
                        _ => continue,
                    };
                    let ask = match ticker.best_ask_price {
                        Some(a) if a > 0.0 => a,
                        _ => continue,
                    };
                    let key = (inst.strike as u64, inst.option_type.to_string());
                    groups
                        .entry(key)
                        .or_default()
                        .push((
                            inst.expiration_timestamp,
                            inst.instrument_name.clone(),
                            ticker.mark_iv,
                            bid,
                            ask,
                        ));
                }
            }
        }

        for ((strike, opt_type), mut entries) in groups {
            if entries.len() < 2 {
                continue;
            }
            entries.sort_by_key(|e| e.0);

            for pair in entries.windows(2) {
                let (_exp_near, ref name_near, iv_near, bid_near, _ask_near) = pair[0];
                let (_exp_far, ref name_far, iv_far, _bid_far, ask_far) = pair[1];

                let iv_diff = iv_near - iv_far;

                if iv_diff.abs() > self.min_iv_diff {
                    let (legs, direction) = if iv_diff > 0.0 {
                        // Near IV higher → sell near (expensive), buy far (cheap)
                        (
                            vec![
                                TradeLeg::sell(1, name_near, bid_near, 1.0),
                                TradeLeg::buy(2, name_far, ask_far, 1.0),
                            ],
                            "Sell near-term (high IV), buy far-term (low IV)",
                        )
                    } else {
                        // Far IV higher → buy near (cheap), sell far (expensive)
                        (
                            vec![
                                TradeLeg::buy(1, name_near, _ask_near, 1.0),
                                TradeLeg::sell(2, name_far, _bid_far, 1.0),
                            ],
                            "Buy near-term, sell far-term (inverted term structure)",
                        )
                    };

                    info!(
                        strike = strike,
                        iv_diff = iv_diff,
                        "Calendar spread IV signal"
                    );

                    opportunities.push(Opportunity {
                        strategy_type: "calendar_spread".to_string(),
                        description: format!(
                            "{} | K={} {} | Near IV: {:.1}%, Far IV: {:.1}%, Diff: {:.1}%",
                            direction, strike, opt_type, iv_near, iv_far, iv_diff
                        ),
                        legs,
                        expected_profit: 0.0,
                        total_cost: 0.0,
                        risk_level: RiskLevel::Medium,
                        instruments: vec![name_near.clone(), name_far.clone()],
                        detected_at: chrono::Utc::now().timestamp(),
                        expiry_timestamp: Some(pair[1].0),
                    });
                }
            }
        }

        opportunities
    }
}
