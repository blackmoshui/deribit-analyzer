use tracing::info;

use crate::analysis::opportunity::{Opportunity, RiskLevel, TradeLeg};
use crate::market::instruments::{InstrumentRegistry, OptionType};
use crate::market::ticker::TickerCache;

/// Detects anomalies in the implied volatility surface using statistical methods.
///
/// Two detection modes:
/// 1. **Butterfly (primary)**: Middle strike IV deviates from linear interpolation
///    of its neighbors — the strongest signal because it's relative to local surface.
/// 2. **Pairwise (statistical)**: Adjacent IV step is a statistical outlier compared
///    to the median step for this expiry — filters out natural skew steepness.
pub struct VolSurfaceAnalyzer {
    /// Minimum z-score for butterfly deviation to trigger (default ~2.0)
    butterfly_z_threshold: f64,
    /// Minimum z-score for pairwise IV step outlier (default ~2.5)
    pairwise_z_threshold: f64,
}

struct IvPoint {
    strike: f64,
    iv: f64,
    name: String,
    bid: f64,
    ask: f64,
    vega: f64,
    underlying: f64,
}

impl VolSurfaceAnalyzer {
    pub fn new(_max_iv_jump: f64) -> Self {
        // Ignore the old absolute threshold, use statistical z-scores
        VolSurfaceAnalyzer {
            butterfly_z_threshold: 2.0,
            pairwise_z_threshold: 2.5,
        }
    }

    pub async fn scan(
        &self,
        registry: &InstrumentRegistry,
        ticker_cache: &TickerCache,
    ) -> Vec<Opportunity> {
        let mut opportunities = Vec::new();
        let expirations = registry.get_expirations().await;

        for expiration in &expirations {
            let iv_points = Self::collect_iv_points(registry, ticker_cache, *expiration).await;

            if iv_points.len() < 3 {
                continue;
            }

            // === Butterfly detection (primary — 3-point local deviation) ===
            self.detect_butterflies(&iv_points, *expiration, &mut opportunities);

            // === Pairwise statistical outlier (secondary) ===
            self.detect_pairwise_outliers(&iv_points, *expiration, &mut opportunities);
        }

        opportunities
    }

    async fn collect_iv_points(
        registry: &InstrumentRegistry,
        ticker_cache: &TickerCache,
        expiration: i64,
    ) -> Vec<IvPoint> {
        let mut points = Vec::new();
        let all_instruments = registry.get_all().await;

        for inst in &all_instruments {
            if inst.expiration_timestamp == expiration && inst.option_type == OptionType::Call {
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
                        points.push(IvPoint {
                            strike: inst.strike,
                            iv: ticker.mark_iv,
                            name: inst.instrument_name.clone(),
                            bid,
                            ask,
                            vega: ticker.vega,
                            underlying: ticker.underlying_price,
                        });
                    }
                }
            }
        }

        points.sort_by(|a, b| a.strike.partial_cmp(&b.strike).unwrap());
        points
    }

    /// Butterfly: middle strike IV deviates from interpolation of neighbors.
    /// Uses z-score relative to all butterfly deviations in this expiry.
    fn detect_butterflies(
        &self,
        points: &[IvPoint],
        expiration: i64,
        opportunities: &mut Vec<Opportunity>,
    ) {
        if points.len() < 3 {
            return;
        }

        // Compute all butterfly deviations for this expiry
        let deviations: Vec<f64> = points
            .windows(3)
            .map(|w| {
                let interpolated = (w[0].iv + w[2].iv) / 2.0;
                w[1].iv - interpolated
            })
            .collect();

        let (mean, std) = mean_std(&deviations);
        if std < 0.5 {
            // IV surface is very smooth — no anomalies
            return;
        }

        for (i, window) in points.windows(3).enumerate() {
            let interpolated = (window[0].iv + window[2].iv) / 2.0;
            let deviation = deviations[i];
            let z_score = (deviation - mean) / std;

            if z_score.abs() < self.butterfly_z_threshold {
                continue;
            }

            // Deribit greeks.vega is in USD per 1% IV change
            let convergence = 0.5;
            let middle_vega = window[1].vega.abs();
            let wing_vega = (window[0].vega.abs() + window[2].vega.abs()) / 2.0;
            // Middle converges to interpolated; wings barely move
            let est_profit_usd =
                (middle_vega * 2.0 * deviation.abs() * convergence).max(0.0);
            // Subtract wing exposure (they move slightly opposite)
            let wing_loss_usd = wing_vega * 2.0 * deviation.abs() * convergence * 0.3;
            let net_profit_usd = (est_profit_usd - wing_loss_usd).max(0.0);
            let underlying = window[1].underlying;
            let fee_usd = underlying * 0.0003 * 4.0; // 4 option legs
            let profit_usd = (net_profit_usd - fee_usd).max(0.0);

            let (legs, desc, net_cost_btc) = if deviation < 0.0 {
                let cost = window[1].ask * 2.0 - window[0].bid - window[2].bid;
                (
                    vec![
                        TradeLeg::sell(1, &window[0].name, window[0].bid, 1.0),
                        TradeLeg::buy(2, &window[1].name, window[1].ask, 2.0),
                        TradeLeg::sell(3, &window[2].name, window[2].bid, 1.0),
                    ],
                    "IV dip",
                    cost.abs(),
                )
            } else {
                let cost = window[0].ask + window[2].ask - window[1].bid * 2.0;
                (
                    vec![
                        TradeLeg::buy(1, &window[0].name, window[0].ask, 1.0),
                        TradeLeg::sell(2, &window[1].name, window[1].bid, 2.0),
                        TradeLeg::buy(3, &window[2].name, window[2].ask, 1.0),
                    ],
                    "IV spike",
                    cost.abs(),
                )
            };

            let total_cost_usd = net_cost_btc * underlying;
            // Sanity cap: profit can't exceed 5x the cost
            let profit_usd = if total_cost_usd > 1.0 {
                profit_usd.min(total_cost_usd * 5.0)
            } else {
                profit_usd.min(underlying * 0.01) // cap at 1% of underlying
            };

            info!(
                strike = window[1].strike,
                deviation = deviation,
                z_score = z_score,
                est_profit_usd = profit_usd,
                "Butterfly IV anomaly (z={:.1})",
                z_score
            );

            opportunities.push(Opportunity {
                strategy_type: "butterfly_spread".to_string(),
                description: format!(
                    "{} K={} | IV {:.1}% vs interp {:.1}% (z={:.1}) | ~${:.0}",
                    desc, window[1].strike, window[1].iv, interpolated, z_score, profit_usd
                ),
                legs,
                expected_profit: profit_usd,
                total_cost: total_cost_usd,
                risk_level: if z_score.abs() > 3.0 {
                    RiskLevel::Low // very strong signal
                } else {
                    RiskLevel::Medium
                },
                instruments: vec![
                    window[0].name.clone(),
                    window[1].name.clone(),
                    window[2].name.clone(),
                ],
                detected_at: chrono::Utc::now().timestamp(),
                expiry_timestamp: Some(expiration),
            });
        }
    }

    /// Pairwise: adjacent IV step is a statistical outlier for this expiry.
    /// Computes median and MAD of all adjacent IV steps, flags outliers.
    fn detect_pairwise_outliers(
        &self,
        points: &[IvPoint],
        expiration: i64,
        opportunities: &mut Vec<Opportunity>,
    ) {
        if points.len() < 4 {
            // Need enough data points for meaningful statistics
            return;
        }

        // Compute all adjacent IV differences
        let steps: Vec<f64> = points.windows(2).map(|w| w[1].iv - w[0].iv).collect();

        let (mean, std) = mean_std(&steps);
        if std < 0.5 {
            return;
        }

        for (i, window) in points.windows(2).enumerate() {
            let step = steps[i];
            let z_score = (step - mean) / std;

            if z_score.abs() < self.pairwise_z_threshold {
                continue;
            }

            // Skip if this pair is already covered by a butterfly detection
            // (butterfly is the stronger signal)
            let covered_by_butterfly = if i > 0 && i + 1 < steps.len() {
                // Check if either adjacent butterfly would fire
                let left_dev = if i >= 1 {
                    let interp = (points[i - 1].iv + points[i + 1].iv) / 2.0;
                    let dev = points[i].iv - interp;
                    let devs: Vec<f64> = points.windows(3).map(|w| w[1].iv - (w[0].iv + w[2].iv) / 2.0).collect();
                    let (m, s) = mean_std(&devs);
                    if s > 0.5 { ((dev - m) / s).abs() > self.butterfly_z_threshold } else { false }
                } else {
                    false
                };
                left_dev
            } else {
                false
            };

            if covered_by_butterfly {
                continue;
            }

            let (high_idx, low_idx) = if step > 0.0 { (1, 0) } else { (0, 1) };
            let high = &window[high_idx];
            let low = &window[low_idx];

            // Deribit greeks.vega is in USD per 1% IV change
            let convergence = 0.5;
            let iv_move = step.abs() * convergence;
            let est_profit_usd = (high.vega.abs() + low.vega.abs()) * iv_move;
            let underlying = high.underlying.max(low.underlying);

            let premium_received = high.bid;
            let premium_paid = low.ask;
            let net_cost_btc = (premium_paid - premium_received).abs();
            let total_cost_usd = net_cost_btc * underlying;
            let fee_usd = underlying * 0.0003 * 2.0;
            let mut profit_usd = (est_profit_usd - fee_usd).max(0.0);
            // Sanity cap: profit can't exceed 5x the cost
            if total_cost_usd > 1.0 {
                profit_usd = profit_usd.min(total_cost_usd * 5.0);
            } else {
                profit_usd = profit_usd.min(underlying * 0.01);
            }

            info!(
                iv_step = step,
                z_score = z_score,
                mean_step = mean,
                std_step = std,
                "IV step outlier between {} and {}",
                window[0].strike,
                window[1].strike
            );

            opportunities.push(Opportunity {
                strategy_type: "vol_surface_anomaly".to_string(),
                description: format!(
                    "IV step outlier z={:.1} | {} ({:.1}%) → {} ({:.1}%) | step {:.1}% vs avg {:.1}% | ~${:.0}",
                    z_score, window[0].strike, window[0].iv, window[1].strike, window[1].iv,
                    step, mean, profit_usd
                ),
                legs: vec![
                    TradeLeg::sell(1, &high.name, high.bid, 1.0),
                    TradeLeg::buy(2, &low.name, low.ask, 1.0),
                ],
                expected_profit: profit_usd,
                total_cost: total_cost_usd,
                risk_level: RiskLevel::High, // weaker signal than butterfly
                instruments: vec![window[0].name.clone(), window[1].name.clone()],
                detected_at: chrono::Utc::now().timestamp(),
                expiry_timestamp: Some(expiration),
            });
        }
    }
}

/// Compute mean and standard deviation
fn mean_std(values: &[f64]) -> (f64, f64) {
    if values.is_empty() {
        return (0.0, 0.0);
    }
    let n = values.len() as f64;
    let mean = values.iter().sum::<f64>() / n;
    let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n;
    (mean, variance.sqrt())
}
