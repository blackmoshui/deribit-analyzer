use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::market::instruments::OptionPriceCurrency;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum HistoryResolution {
    FifteenMinutes,
    OneHour,
}

impl HistoryResolution {
    pub fn api_resolution(self) -> i64 {
        match self {
            HistoryResolution::FifteenMinutes => 15,
            HistoryResolution::OneHour => 60,
        }
    }

    pub fn bucket_ms(self) -> i64 {
        match self {
            HistoryResolution::FifteenMinutes => 900_000,
            HistoryResolution::OneHour => 3_600_000,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            HistoryResolution::FifteenMinutes => "15m",
            HistoryResolution::OneHour => "1h",
        }
    }

    pub fn lookback_ms(self) -> i64 {
        match self {
            HistoryResolution::FifteenMinutes => 24 * 60 * 60 * 1000,
            HistoryResolution::OneHour => 30 * 24 * 60 * 60 * 1000,
        }
    }

    pub fn cache_key(self) -> i64 {
        match self {
            // Versioned keys to invalidate the older trade-based cache layout.
            HistoryResolution::FifteenMinutes => 1015,
            HistoryResolution::OneHour => 1060,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ShortPutTradeSample {
    pub timestamp_ms: i64,
    pub option_price: f64,
    pub index_price: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OptionCandlePoint {
    pub tick_ms: i64,
    pub close_price: f64,
    pub volume: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexPricePoint {
    pub tick_ms: i64,
    pub price: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShortPutHistoryPoint {
    pub bucket_start_ms: i64,
    pub sample_timestamp_ms: i64,
    pub option_price: f64,
    pub underlying_price: f64,
    pub premium_usd: f64,
    pub annualized_return: f64,
    pub trade_count: usize,
}

pub fn aggregate_history_points(
    strike: f64,
    expiry_timestamp_ms: i64,
    resolution: HistoryResolution,
    trades: &[ShortPutTradeSample],
) -> Vec<ShortPutHistoryPoint> {
    aggregate_history_points_with_price_currency(
        strike,
        expiry_timestamp_ms,
        resolution,
        trades,
        OptionPriceCurrency::BaseAsset,
    )
}

pub fn aggregate_history_points_with_price_currency(
    strike: f64,
    expiry_timestamp_ms: i64,
    resolution: HistoryResolution,
    trades: &[ShortPutTradeSample],
    price_currency: OptionPriceCurrency,
) -> Vec<ShortPutHistoryPoint> {
    if strike <= 0.0 {
        return Vec::new();
    }

    let mut buckets: BTreeMap<i64, (ShortPutTradeSample, usize)> = BTreeMap::new();
    let bucket_ms = resolution.bucket_ms();

    for trade in trades {
        let bucket_start = (trade.timestamp_ms / bucket_ms) * bucket_ms;
        buckets
            .entry(bucket_start)
            .and_modify(|(last_trade, count)| {
                if trade.timestamp_ms >= last_trade.timestamp_ms {
                    *last_trade = trade.clone();
                }
                *count += 1;
            })
            .or_insert_with(|| (trade.clone(), 1));
    }

    buckets
        .into_iter()
        .filter_map(|(bucket_start_ms, (trade, trade_count))| {
            if trade.option_price <= 0.0 || trade.index_price <= 0.0 {
                return None;
            }

            if strike > trade.index_price {
                return None;
            }

            let premium_usd =
                option_premium_usd(price_currency, trade.option_price, trade.index_price);
            if premium_usd < 1.0 {
                return None;
            }

            let days_to_expiry = (expiry_timestamp_ms - trade.timestamp_ms) as f64 / 86_400_000.0;
            if days_to_expiry < 1.0 {
                return None;
            }

            Some(ShortPutHistoryPoint {
                bucket_start_ms,
                sample_timestamp_ms: trade.timestamp_ms,
                option_price: trade.option_price,
                underlying_price: trade.index_price,
                premium_usd,
                annualized_return: (premium_usd / strike) * 365.0 / days_to_expiry,
                trade_count,
            })
        })
        .collect()
}

pub fn build_history_points_from_candles(
    strike: f64,
    expiry_timestamp_ms: i64,
    option_candles: &[OptionCandlePoint],
    index_points: &[IndexPricePoint],
) -> Vec<ShortPutHistoryPoint> {
    build_history_points_from_candles_with_price_currency(
        strike,
        expiry_timestamp_ms,
        option_candles,
        index_points,
        OptionPriceCurrency::BaseAsset,
    )
}

pub fn build_history_points_from_candles_with_price_currency(
    strike: f64,
    expiry_timestamp_ms: i64,
    option_candles: &[OptionCandlePoint],
    index_points: &[IndexPricePoint],
    price_currency: OptionPriceCurrency,
) -> Vec<ShortPutHistoryPoint> {
    if strike <= 0.0 {
        return Vec::new();
    }

    let mut points = Vec::new();
    let mut index_cursor = 0usize;
    let mut current_index_price = None;

    for candle in option_candles {
        while index_cursor < index_points.len()
            && index_points[index_cursor].tick_ms <= candle.tick_ms
        {
            current_index_price = Some(index_points[index_cursor].price);
            index_cursor += 1;
        }

        let Some(underlying_price) = current_index_price else {
            continue;
        };

        if candle.close_price <= 0.0 || underlying_price <= 0.0 {
            continue;
        }

        if strike > underlying_price {
            continue;
        }

        let premium_usd = option_premium_usd(price_currency, candle.close_price, underlying_price);
        if premium_usd < 1.0 {
            continue;
        }

        let days_to_expiry = (expiry_timestamp_ms - candle.tick_ms) as f64 / 86_400_000.0;
        if days_to_expiry < 1.0 {
            continue;
        }

        points.push(ShortPutHistoryPoint {
            bucket_start_ms: candle.tick_ms,
            sample_timestamp_ms: candle.tick_ms,
            option_price: candle.close_price,
            underlying_price,
            premium_usd,
            annualized_return: (premium_usd / strike) * 365.0 / days_to_expiry,
            trade_count: usize::from(candle.volume > 0.0),
        });
    }

    points
}

fn option_premium_usd(
    price_currency: OptionPriceCurrency,
    option_price: f64,
    underlying_price: f64,
) -> f64 {
    match price_currency {
        OptionPriceCurrency::BaseAsset => option_price * underlying_price,
        OptionPriceCurrency::QuoteCurrency => option_price,
    }
}
