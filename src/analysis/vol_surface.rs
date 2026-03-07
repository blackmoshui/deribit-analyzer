use tracing::info;

use crate::analysis::opportunity::{Opportunity, RiskLevel, TradeLeg};
use crate::market::instruments::{InstrumentRegistry, OptionType};
use crate::market::ticker::TickerCache;

/// Detects anomalies in the implied volatility surface
pub struct VolSurfaceAnalyzer {
    max_iv_jump: f64,
}

impl VolSurfaceAnalyzer {
    pub fn new(max_iv_jump: f64) -> Self {
        VolSurfaceAnalyzer { max_iv_jump }
    }

    pub async fn scan(
        &self,
        registry: &InstrumentRegistry,
        ticker_cache: &TickerCache,
    ) -> Vec<Opportunity> {
        let mut opportunities = Vec::new();
        let expirations = registry.get_expirations().await;

        for expiration in &expirations {
            let mut iv_points: Vec<(f64, f64, String, f64, f64)> = Vec::new();
            // (strike, iv, name, bid, ask)

            let all_instruments = registry.get_all().await;
            for inst in &all_instruments {
                if inst.expiration_timestamp == *expiration && inst.option_type == OptionType::Call {
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
                            iv_points.push((
                                inst.strike,
                                ticker.mark_iv,
                                inst.instrument_name.clone(),
                                bid,
                                ask,
                            ));
                        }
                    }
                }
            }

            iv_points.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

            // IV anomaly jumps
            for window in iv_points.windows(2) {
                let iv_diff = (window[1].1 - window[0].1).abs();
                if iv_diff > self.max_iv_jump {
                    let (high_iv_idx, low_iv_idx) = if window[1].1 > window[0].1 {
                        (1, 0)
                    } else {
                        (0, 1)
                    };

                    info!(
                        iv_diff = iv_diff,
                        "IV surface anomaly between {} and {}",
                        window[0].0, window[1].0
                    );
                    opportunities.push(Opportunity {
                        strategy_type: "vol_surface_anomaly".to_string(),
                        description: format!(
                            "IV jump {:.1}% | {} ({:.1}%) vs {} ({:.1}%) | Sell high IV, buy low IV",
                            iv_diff, window[0].0, window[0].1, window[1].0, window[1].1
                        ),
                        legs: vec![
                            TradeLeg::sell(
                                1,
                                &window[high_iv_idx].2,
                                window[high_iv_idx].3,
                                1.0,
                            ),
                            TradeLeg::buy(
                                2,
                                &window[low_iv_idx].2,
                                window[low_iv_idx].4,
                                1.0,
                            ),
                        ],
                        expected_profit: 0.0,
                        total_cost: 0.0,
                        risk_level: RiskLevel::Medium,
                        instruments: vec![window[0].2.clone(), window[1].2.clone()],
                        detected_at: chrono::Utc::now().timestamp(),
                        expiry_timestamp: Some(*expiration),
                    });
                }
            }

            // Butterfly IV deviation
            for window in iv_points.windows(3) {
                let interpolated = (window[0].1 + window[2].1) / 2.0;
                let deviation = window[1].1 - interpolated;

                if deviation.abs() > self.max_iv_jump * 0.5 {
                    let (legs, desc) = if deviation < 0.0 {
                        // Middle IV too low → buy middle, sell wings (long butterfly)
                        (
                            vec![
                                TradeLeg::sell(1, &window[0].2, window[0].3, 1.0),
                                TradeLeg::buy(2, &window[1].2, window[1].4, 2.0),
                                TradeLeg::sell(3, &window[2].2, window[2].3, 1.0),
                            ],
                            "Middle IV cheap → Long butterfly (sell wings, buy middle)",
                        )
                    } else {
                        // Middle IV too high → sell middle, buy wings (short butterfly)
                        (
                            vec![
                                TradeLeg::buy(1, &window[0].2, window[0].4, 1.0),
                                TradeLeg::sell(2, &window[1].2, window[1].3, 2.0),
                                TradeLeg::buy(3, &window[2].2, window[2].4, 1.0),
                            ],
                            "Middle IV rich → Short butterfly (buy wings, sell middle)",
                        )
                    };

                    info!(
                        strike = window[1].0,
                        deviation = deviation,
                        "Butterfly IV opportunity"
                    );
                    opportunities.push(Opportunity {
                        strategy_type: "butterfly_spread".to_string(),
                        description: format!(
                            "{} | K={} IV {:.1}% vs interp {:.1}% (dev {:.1}%)",
                            desc, window[1].0, window[1].1, interpolated, deviation
                        ),
                        legs,
                        expected_profit: 0.0,
                        total_cost: 0.0,
                        risk_level: RiskLevel::Medium,
                        instruments: vec![
                            window[0].2.clone(),
                            window[1].2.clone(),
                            window[2].2.clone(),
                        ],
                        detected_at: chrono::Utc::now().timestamp(),
                        expiry_timestamp: Some(*expiration),
                    });
                }
            }
        }

        opportunities
    }
}
