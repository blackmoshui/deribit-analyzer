use anyhow::Result;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{Html, IntoResponse},
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};

use deribit::analysis::short_put_history::HistoryResolution;
use deribit::analysis::short_put_history_service::{ShortPutHistoryRequest, ShortPutHistoryService};
use deribit::config::Config;
use deribit::storage::sqlite::Storage;

#[derive(Clone)]
struct AppState {
    storage: Storage,
    history_service: ShortPutHistoryService,
}

#[derive(Serialize)]
struct ApiOpportunity {
    id: i64,
    strategy_type: String,
    description: String,
    expected_profit: f64,
    total_cost: f64,
    risk_level: String,
    instruments: Vec<String>,
    legs: Vec<ApiLeg>,
    detected_at: i64,
    expiry_timestamp: Option<i64>,
    annualized_return: Option<f64>,
    days_to_expiry: Option<f64>,
}

#[derive(Serialize)]
struct ApiLeg {
    step: usize,
    action: String,
    instrument: String,
    price: f64,
    amount: f64,
    price_unit: String,
}

#[derive(Serialize)]
struct StatsResponse {
    instrument_count: usize,
    opportunity_count: usize,
}

#[derive(Deserialize)]
struct HistoryQuery {
    instrument: String,
    strike: f64,
    expiry: i64, // expiry timestamp in ms
    resolution: Option<String>, // "15m" or "1h"
}

#[derive(Serialize)]
struct HistoryPoint {
    timestamp: i64,
    apy: f64,
    premium_usd: f64,
    underlying_price: f64,
    trade_count: usize,
}

#[derive(Serialize)]
struct HistoryResponse {
    points: Vec<HistoryPoint>,
    resolution: String,
    status: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("web=info".parse().unwrap()),
        )
        .init();

    let config = Config::from_env()?;
    let storage = Storage::new(&config.db_path).await?;
    let history_service = ShortPutHistoryService::new(storage.clone(), config.ws_url);

    let state = AppState {
        storage,
        history_service,
    };

    let bind = std::env::var("WEB_BIND").unwrap_or_else(|_| "0.0.0.0:8080".to_string());

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/api/opportunities", get(opportunities_handler))
        .route("/api/stats", get(stats_handler))
        .route("/api/history", get(history_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!("Web monitor listening on http://{}", bind);
    axum::serve(listener, app).await?;

    Ok(())
}

async fn index_handler() -> impl IntoResponse {
    Html(include_str!("../../static/index.html"))
}

#[derive(Deserialize)]
struct OppsQuery {
    /// Max age in seconds (default: 120, 0 = no filter)
    max_age: Option<i64>,
}

async fn opportunities_handler(
    State(state): State<AppState>,
    Query(params): Query<OppsQuery>,
) -> Result<Json<Vec<ApiOpportunity>>, StatusCode> {
    let entries = state
        .storage
        .load_opportunities_after(0)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let now = chrono::Utc::now().timestamp();
    let max_age = params.max_age.unwrap_or(120);
    let results: Vec<ApiOpportunity> = entries
        .into_iter()
        .filter(|(_, opp)| max_age == 0 || now - opp.detected_at < max_age)
        .map(|(id, opp)| {
            let annualized_return = opp.annualized_return();
            let days_to_expiry = opp.expiry_timestamp.map(|exp| {
                let detected_ms = opp.detected_at * 1000;
                (exp - detected_ms) as f64 / 86_400_000.0
            });
            let legs = opp
                .legs
                .iter()
                .map(|l| ApiLeg {
                    step: l.step,
                    action: l.action.to_string(),
                    instrument: l.instrument.clone(),
                    price: l.price,
                    amount: l.amount,
                    price_unit: l.price_unit.to_string(),
                })
                .collect();
            ApiOpportunity {
                id,
                strategy_type: opp.strategy_type,
                description: opp.description,
                expected_profit: opp.expected_profit,
                total_cost: opp.total_cost,
                risk_level: opp.risk_level.to_string(),
                instruments: opp.instruments,
                legs,
                detected_at: opp.detected_at,
                expiry_timestamp: opp.expiry_timestamp,
                annualized_return,
                days_to_expiry,
            }
        })
        .collect();

    Ok(Json(results))
}

async fn stats_handler(
    State(state): State<AppState>,
) -> Result<Json<StatsResponse>, StatusCode> {
    let instrument_count = state
        .storage
        .count_instruments()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let now = chrono::Utc::now().timestamp();
    let entries = state
        .storage
        .load_opportunities_after(0)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let opportunity_count = entries
        .iter()
        .filter(|(_, opp)| now - opp.detected_at < 120)
        .count();

    Ok(Json(StatsResponse {
        instrument_count,
        opportunity_count,
    }))
}

async fn history_handler(
    State(state): State<AppState>,
    Query(params): Query<HistoryQuery>,
) -> Result<Json<HistoryResponse>, StatusCode> {
    let resolution = match params.resolution.as_deref() {
        Some("1h") => HistoryResolution::OneHour,
        _ => HistoryResolution::FifteenMinutes,
    };

    let cache_key = format!("{}:{}:{}", params.instrument, params.expiry, resolution.label());
    let request = ShortPutHistoryRequest {
        cache_key,
        instrument_name: params.instrument,
        strike: params.strike,
        expiry_timestamp_ms: params.expiry,
        resolution,
    };

    match state.history_service.load_history(&request).await {
        Ok(history) => {
            let api_points: Vec<HistoryPoint> = history
                .points
                .into_iter()
                .map(|p| HistoryPoint {
                    timestamp: p.bucket_start_ms,
                    apy: p.annualized_return,
                    premium_usd: p.premium_usd,
                    underlying_price: p.underlying_price,
                    trade_count: p.trade_count,
                })
                .collect();

            Ok(Json(HistoryResponse {
                points: api_points,
                resolution: resolution.label().to_string(),
                status: history.status,
            }))
        }
        Err(e) => {
            tracing::warn!("History load failed for {}: {}", request.instrument_name, e);
            // Return empty with error status instead of 500
            Ok(Json(HistoryResponse {
                points: vec![],
                resolution: resolution.label().to_string(),
                status: format!("error: {}", e),
            }))
        }
    }
}
