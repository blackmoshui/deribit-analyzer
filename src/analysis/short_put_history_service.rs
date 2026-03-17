use anyhow::{Context, Result};
use serde_json::json;

use crate::analysis::short_put_history::{
    build_history_points_from_candles, HistoryResolution, IndexPricePoint, OptionCandlePoint,
    ShortPutHistoryPoint,
};
use crate::storage::sqlite::Storage;
use crate::ws::public_client::PublicRpcClient;

#[derive(Debug, Clone)]
pub struct ShortPutHistoryRequest {
    pub cache_key: String,
    pub instrument_name: String,
    pub strike: f64,
    pub expiry_timestamp_ms: i64,
    pub resolution: HistoryResolution,
}

#[derive(Debug, Clone)]
pub struct LoadedShortPutHistory {
    pub points: Vec<ShortPutHistoryPoint>,
    pub status: String,
}

#[derive(Clone)]
pub struct ShortPutHistoryService {
    storage: Storage,
    rpc_client: PublicRpcClient,
}

impl ShortPutHistoryService {
    pub fn new(storage: Storage, ws_url: impl Into<String>) -> Self {
        Self {
            storage,
            rpc_client: PublicRpcClient::new(ws_url),
        }
    }

    pub async fn load_history(
        &self,
        request: &ShortPutHistoryRequest,
    ) -> Result<LoadedShortPutHistory> {
        let now_ms = chrono::Utc::now().timestamp_millis();
        let start_ms = now_ms - request.resolution.lookback_ms();
        let end_ms = now_ms;

        let cached = self
            .storage
            .load_short_put_history_points(
                &request.instrument_name,
                request.resolution,
                start_ms,
                end_ms,
            )
            .await?;

        let latest_completed_bucket = ((end_ms / request.resolution.bucket_ms())
            * request.resolution.bucket_ms())
            - request.resolution.bucket_ms();

        let cached_is_fresh = cache_covers_requested_window(
            cached.first().map(|point| point.bucket_start_ms),
            cached.last().map(|point| point.bucket_start_ms),
            start_ms,
            latest_completed_bucket,
            request.resolution,
        );

        if cached_is_fresh {
            return Ok(LoadedShortPutHistory {
                points: cached,
                status: format!(
                    "{} approx APY history | candle close | cached | {}",
                    request.resolution.label(),
                    lookback_label(request.resolution)
                ),
            });
        }

        let fetch_start_ms = compute_fetch_start_ms(
            cached.first().map(|point| point.bucket_start_ms),
            start_ms,
            latest_completed_bucket,
            request.resolution,
        );

        match self
            .fetch_history_points(
                &request.instrument_name,
                request.strike,
                request.expiry_timestamp_ms,
                fetch_start_ms,
                end_ms,
                request.resolution,
            )
            .await
        {
            Ok(fresh_points) => {
                self.storage
                    .delete_short_put_history_points_range(
                        &request.instrument_name,
                        request.resolution,
                        fetch_start_ms,
                        end_ms,
                    )
                    .await?;
                self.storage
                    .save_short_put_history_points(
                        &request.instrument_name,
                        request.resolution,
                        &fresh_points,
                    )
                    .await?;

                let merged = self
                    .storage
                    .load_short_put_history_points(
                        &request.instrument_name,
                        request.resolution,
                        start_ms,
                        end_ms,
                    )
                    .await?;

                Ok(LoadedShortPutHistory {
                    points: merged,
                    status: format!(
                        "{} approx APY history | candle close | refreshed | {}",
                        request.resolution.label(),
                        lookback_label(request.resolution)
                    ),
                })
            }
            Err(error) if !cached.is_empty() => Ok(LoadedShortPutHistory {
                points: cached,
                status: format!(
                    "{} approx APY history | candle close | stale cache ({})",
                    request.resolution.label(),
                    error
                ),
            }),
            Err(error) => Err(error),
        }
    }

    async fn fetch_history_points(
        &self,
        instrument_name: &str,
        strike: f64,
        expiry_timestamp_ms: i64,
        start_ms: i64,
        end_ms: i64,
        resolution: HistoryResolution,
    ) -> Result<Vec<ShortPutHistoryPoint>> {
        let option_candles = self
            .fetch_option_candles(instrument_name, start_ms, end_ms, resolution)
            .await?;
        let index_name = index_name_for_instrument(instrument_name).ok_or_else(|| {
            anyhow::anyhow!("Unable to derive index name from {}", instrument_name)
        })?;
        let index_points = self
            .fetch_index_points(&index_name, start_ms, end_ms, resolution)
            .await?;

        Ok(build_history_points_from_candles(
            strike,
            expiry_timestamp_ms,
            &option_candles,
            &index_points,
        ))
    }

    async fn fetch_option_candles(
        &self,
        instrument_name: &str,
        start_ms: i64,
        end_ms: i64,
        resolution: HistoryResolution,
    ) -> Result<Vec<OptionCandlePoint>> {
        let result = self
            .rpc_client
            .send_request(
                "public/get_tradingview_chart_data",
                json!({
                    "instrument_name": instrument_name,
                    "start_timestamp": start_ms,
                    "end_timestamp": end_ms,
                    "resolution": resolution.api_resolution(),
                }),
            )
            .await?;

        parse_option_candles(&result)
            .with_context(|| format!("Failed to parse candle data for {}", instrument_name))
    }

    async fn fetch_index_points(
        &self,
        index_name: &str,
        start_ms: i64,
        end_ms: i64,
        resolution: HistoryResolution,
    ) -> Result<Vec<IndexPricePoint>> {
        let result = self
            .rpc_client
            .send_request(
                "public/get_index_chart_data",
                json!({
                    "index_name": index_name,
                    "range": index_chart_range_for_resolution(resolution),
                }),
            )
            .await?;

        let points = parse_index_points(&result)
            .with_context(|| format!("Failed to parse index chart data for {}", index_name))?;

        Ok(points
            .into_iter()
            .filter(|point| point.tick_ms >= start_ms && point.tick_ms <= end_ms)
            .collect())
    }
}

pub fn cache_covers_requested_window(
    first_bucket_start_ms: Option<i64>,
    last_bucket_start_ms: Option<i64>,
    requested_start_ms: i64,
    latest_completed_bucket_ms: i64,
    resolution: HistoryResolution,
) -> bool {
    let Some(first_bucket_start_ms) = first_bucket_start_ms else {
        return false;
    };
    let Some(last_bucket_start_ms) = last_bucket_start_ms else {
        return false;
    };

    let bucket_ms = resolution.bucket_ms();
    let requested_start_bucket = (requested_start_ms / bucket_ms) * bucket_ms;

    first_bucket_start_ms <= requested_start_bucket
        && last_bucket_start_ms >= latest_completed_bucket_ms
}

pub fn compute_fetch_start_ms(
    first_bucket_start_ms: Option<i64>,
    requested_start_ms: i64,
    latest_completed_bucket_ms: i64,
    resolution: HistoryResolution,
) -> i64 {
    let bucket_ms = resolution.bucket_ms();
    let requested_start_bucket = (requested_start_ms / bucket_ms) * bucket_ms;

    match first_bucket_start_ms {
        Some(first_bucket_start_ms)
            if first_bucket_start_ms <= requested_start_bucket
                && first_bucket_start_ms <= latest_completed_bucket_ms =>
        {
            latest_completed_bucket_ms.saturating_sub(bucket_ms)
        }
        _ => requested_start_ms.max(0),
    }
}

pub fn index_chart_range_for_resolution(resolution: HistoryResolution) -> &'static str {
    match resolution {
        HistoryResolution::FifteenMinutes => "1d",
        HistoryResolution::OneHour => "1m",
    }
}

fn index_name_for_instrument(instrument_name: &str) -> Option<String> {
    let base = instrument_name.split('-').next()?;
    Some(format!("{}_usd", base.to_lowercase()))
}

fn parse_option_candles(result: &serde_json::Value) -> Result<Vec<OptionCandlePoint>> {
    let ticks = result["ticks"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("Missing ticks array"))?;
    let closes = result["close"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("Missing close array"))?;
    let volumes = result["volume"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("Missing volume array"))?;

    if ticks.len() != closes.len() || ticks.len() != volumes.len() {
        anyhow::bail!("Mismatched candle array lengths");
    }

    Ok(ticks
        .iter()
        .zip(closes.iter())
        .zip(volumes.iter())
        .filter_map(|((tick, close), volume)| {
            Some(OptionCandlePoint {
                tick_ms: tick.as_i64()?,
                close_price: close.as_f64()?,
                volume: volume.as_f64()?,
            })
        })
        .collect())
}

fn parse_index_points(result: &serde_json::Value) -> Result<Vec<IndexPricePoint>> {
    let points = result
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("Missing index point array"))?;

    Ok(points
        .iter()
        .filter_map(|point| {
            let values = point.as_array()?;
            Some(IndexPricePoint {
                tick_ms: values.first()?.as_i64()?,
                price: values.get(1)?.as_f64()?,
            })
        })
        .collect())
}

fn lookback_label(resolution: HistoryResolution) -> &'static str {
    match resolution {
        HistoryResolution::FifteenMinutes => "last 24h",
        HistoryResolution::OneHour => "last 30d",
    }
}
