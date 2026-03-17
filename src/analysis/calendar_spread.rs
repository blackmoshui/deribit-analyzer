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
        // (exp, name, iv, bid, ask, vega, underlying_price)
        let mut groups: std::collections::HashMap<
            (u64, String),
            Vec<(i64, String, f64, f64, f64, f64, f64)>,
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
                    groups.entry(key).or_default().push((
                        inst.expiration_timestamp,
                        inst.instrument_name.clone(),
                        ticker.mark_iv,
                        bid,
                        ask,
                        ticker.vega,
                        ticker.underlying_price,
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
                let (
                    _exp_near,
                    ref name_near,
                    iv_near,
                    bid_near,
                    _ask_near,
                    vega_near,
                    underlying_near,
                ) = pair[0];
                let (_exp_far, ref name_far, iv_far, _bid_far, ask_far, vega_far, underlying_far) =
                    pair[1];

                let iv_diff = iv_near - iv_far;

                if iv_diff.abs() > self.min_iv_diff {
                    // Deribit greeks.vega is in USD per 1% IV change
                    // Estimate profit if IV converges 50%
                    let convergence = 0.5;
                    let iv_move = iv_diff.abs() * convergence;
                    let est_profit_usd = (vega_near.abs() + vega_far.abs()) * iv_move;
                    let underlying = underlying_near.max(underlying_far);
                    let fee_usd = underlying * 0.0003 * 2.0;
                    let profit_usd = (est_profit_usd - fee_usd).max(0.0);

                    let (legs, direction, net_cost_btc) = if iv_diff > 0.0 {
                        // Near IV higher → sell near (expensive), buy far (cheap)
                        let cost = (ask_far - bid_near).abs();
                        (
                            vec![
                                TradeLeg::sell(1, name_near, bid_near, 1.0),
                                TradeLeg::buy(2, name_far, ask_far, 1.0),
                            ],
                            "Sell near-term (high IV), buy far-term (low IV)",
                            cost,
                        )
                    } else {
                        // Far IV higher → buy near (cheap), sell far (expensive)
                        let cost = (_ask_near - _bid_far).abs();
                        (
                            vec![
                                TradeLeg::buy(1, name_near, _ask_near, 1.0),
                                TradeLeg::sell(2, name_far, _bid_far, 1.0),
                            ],
                            "Buy near-term, sell far-term (inverted term structure)",
                            cost,
                        )
                    };

                    let total_cost_usd = net_cost_btc * underlying;
                    // Sanity cap: profit can't exceed 5x the cost
                    let profit_usd = if total_cost_usd > 1.0 {
                        profit_usd.min(total_cost_usd * 5.0)
                    } else {
                        profit_usd.min(underlying * 0.01)
                    };

                    info!(
                        strike = strike,
                        iv_diff = iv_diff,
                        est_profit_usd = profit_usd,
                        "Calendar spread IV signal"
                    );

                    opportunities.push(Opportunity {
                        strategy_type: "calendar_spread".to_string(),
                        description: format!(
                            "{} | K={} {} | Near IV: {:.1}%, Far IV: {:.1}%, Diff: {:.1}% | ~${:.0}",
                            direction, strike, opt_type, iv_near, iv_far, iv_diff, profit_usd
                        ),
                        legs,
                        expected_profit: profit_usd,
                        total_cost: total_cost_usd,
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
