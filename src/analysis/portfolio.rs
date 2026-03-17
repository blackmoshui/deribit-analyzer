use crate::analysis::opportunity::{Opportunity, RiskLevel};

/// Portfolio combination optimizer
/// Finds pairs of opportunities where futures legs offset,
/// reducing combined margin and boosting APY.
pub struct PortfolioOptimizer {
    leverage: f64,
}

struct OppInfo {
    idx: usize,
    delta_btc: f64,   // net futures delta (+long, -short)
    futures_usd: f64, // underlying price from futures leg
    option_cost: f64, // cost excluding futures margin
}

impl PortfolioOptimizer {
    pub fn new(leverage: f64) -> Self {
        Self { leverage }
    }

    pub fn set_leverage(&mut self, leverage: f64) {
        self.leverage = leverage;
    }

    /// Find top N portfolio combinations from current opportunities.
    /// Only pairs opportunities with opposing futures directions.
    pub fn find_best(&self, opportunities: &[Opportunity], top_n: usize) -> Vec<Opportunity> {
        let infos: Vec<OppInfo> = opportunities
            .iter()
            .enumerate()
            .filter_map(|(i, opp)| {
                let delta = opp.futures_delta_btc();
                if delta.abs() < 0.001 {
                    return None;
                }
                let futures_usd = opp.futures_notional_usd();
                let option_cost = (opp.total_cost.abs() - futures_usd).max(0.0);
                Some(OppInfo {
                    idx: i,
                    delta_btc: delta,
                    futures_usd,
                    option_cost,
                })
            })
            .collect();

        let longs: Vec<&OppInfo> = infos.iter().filter(|i| i.delta_btc > 0.0).collect();
        let shorts: Vec<&OppInfo> = infos.iter().filter(|i| i.delta_btc < 0.0).collect();

        let now_s = chrono::Utc::now().timestamp();
        let now_ms = now_s * 1000;

        let mut combos: Vec<(f64, Opportunity)> = Vec::new();

        for long_info in &longs {
            for short_info in &shorts {
                let opp_a = &opportunities[long_info.idx];
                let opp_b = &opportunities[short_info.idx];

                let net_delta = long_info.delta_btc + short_info.delta_btc;
                let avg_price = if long_info.futures_usd > 0.0 && short_info.futures_usd > 0.0 {
                    // Use average underlying price for residual margin
                    let price_a = long_info.futures_usd / long_info.delta_btc.abs();
                    let price_b = short_info.futures_usd / short_info.delta_btc.abs();
                    (price_a + price_b) / 2.0
                } else {
                    0.0
                };

                let combined_option_cost = long_info.option_cost + short_info.option_cost;
                let residual_futures_margin = net_delta.abs() * avg_price / self.leverage;
                let combined_cost = combined_option_cost + residual_futures_margin;

                let combined_profit = opp_a.expected_profit + opp_b.expected_profit;

                if combined_cost < 1.0 || combined_profit <= 0.0 {
                    continue;
                }

                // Use the later expiry for duration calc
                let max_expiry = match (opp_a.expiry_timestamp, opp_b.expiry_timestamp) {
                    (Some(a), Some(b)) => Some(a.max(b)),
                    (a, b) => a.or(b),
                };

                let apy = max_expiry.and_then(|exp| {
                    let days = (exp - now_ms) as f64 / 86_400_000.0;
                    if days < 1.0 {
                        return None;
                    }
                    Some((combined_profit / combined_cost) * 365.0 / days)
                });

                let apy_val = apy.unwrap_or(0.0);

                // Combine legs with renumbered steps
                let mut legs = opp_a.legs.clone();
                let offset = legs.len();
                for leg in &opp_b.legs {
                    let mut l = leg.clone();
                    l.step += offset;
                    legs.push(l);
                }

                // Deduplicate instruments
                let mut instruments = opp_a.instruments.clone();
                instruments.extend(opp_b.instruments.clone());
                instruments.sort();
                instruments.dedup();

                let delta_label = if net_delta.abs() < 0.001 {
                    "hedged".to_string()
                } else {
                    format!("{:+.2} BTC", net_delta)
                };

                let combo = Opportunity {
                    strategy_type: "portfolio".to_string(),
                    description: format!(
                        "{} + {} | {} | {}x lev",
                        opp_a.strategy_type, opp_b.strategy_type, delta_label, self.leverage as i32,
                    ),
                    legs,
                    expected_profit: combined_profit,
                    total_cost: combined_cost,
                    risk_level: if net_delta.abs() < 0.001 {
                        RiskLevel::Low
                    } else {
                        RiskLevel::Medium
                    },
                    instruments,
                    detected_at: now_s,
                    expiry_timestamp: max_expiry,
                };

                combos.push((apy_val, combo));
            }
        }

        // Sort by APY descending
        combos.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        combos.truncate(top_n);
        combos.into_iter().map(|(_, opp)| opp).collect()
    }
}
