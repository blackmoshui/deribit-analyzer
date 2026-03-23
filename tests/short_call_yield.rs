use deribit::analysis::opportunity::PriceUnit;
use deribit::analysis::short_call_yield::ShortCallYieldAnalyzer;
use deribit::events::bus::TickerData;
use deribit::market::instruments::InstrumentRegistry;
use deribit::market::ticker::TickerCache;

fn approx_eq(left: f64, right: f64, tolerance: f64) {
    assert!(
        (left - right).abs() <= tolerance,
        "left={left}, right={right}, tolerance={tolerance}"
    );
}

#[tokio::test]
async fn generates_short_call_yield_opportunity_with_annualized_return() {
    let registry = InstrumentRegistry::new();
    let ticker_cache = TickerCache::new();
    let analyzer = ShortCallYieldAnalyzer::new();

    let now_ms = chrono::Utc::now().timestamp_millis();
    let expiry_ms = now_ms + 30 * 86_400_000;

    let instruments = serde_json::json!([
        {
            "instrument_name": "BTC-30APR26-100000-C",
            "strike": 100000.0,
            "expiration_timestamp": expiry_ms,
            "option_type": "call",
            "is_active": true
        }
    ]);
    registry.load_from_response(&instruments).await.unwrap();

    ticker_cache
        .update(
            "BTC-30APR26-100000-C",
            TickerData {
                mark_price: 0.05,
                mark_iv: 55.0,
                best_bid_price: Some(0.05),
                best_ask_price: Some(0.051),
                best_bid_amount: 10.0,
                best_ask_amount: 8.0,
                open_interest: 42.0,
                underlying_price: 80000.0,
                delta: 0.25,
                gamma: 0.0,
                vega: 0.0,
                theta: 0.0,
                timestamp: now_ms,
            },
        )
        .await;

    let opportunities = analyzer.scan(&registry, &ticker_cache).await;

    assert_eq!(opportunities.len(), 1);
    let opp = &opportunities[0];

    assert_eq!(opp.strategy_type, "short_call_yield");
    assert_eq!(opp.instruments, vec!["BTC-30APR26-100000-C"]);
    // premium = 0.05 BTC * 80000 = $4000
    approx_eq(opp.expected_profit, 4000.0, 1e-6);
    approx_eq(opp.total_cost, 100000.0, 1e-6);
    assert!(opp.description.starts_with("2026-"));
    assert!(opp.description.contains("SCALL"));

    let annualized_return = opp.annualized_return().unwrap();
    approx_eq(
        annualized_return,
        (4000.0 / 100000.0) * (365.0 / 30.0),
        0.02,
    );
}

#[tokio::test]
async fn hides_calls_with_strike_below_current_btc_price() {
    let registry = InstrumentRegistry::new();
    let ticker_cache = TickerCache::new();
    let analyzer = ShortCallYieldAnalyzer::new();

    let now_ms = chrono::Utc::now().timestamp_millis();
    let expiry_ms = now_ms + 30 * 86_400_000;

    // ITM call: strike 60000 < underlying 80000
    let instruments = serde_json::json!([
        {
            "instrument_name": "BTC-30APR26-60000-C",
            "strike": 60000.0,
            "expiration_timestamp": expiry_ms,
            "option_type": "call",
            "is_active": true
        }
    ]);
    registry.load_from_response(&instruments).await.unwrap();

    ticker_cache
        .update(
            "BTC-30APR26-60000-C",
            TickerData {
                mark_price: 0.05,
                mark_iv: 55.0,
                best_bid_price: Some(0.05),
                best_ask_price: Some(0.051),
                best_bid_amount: 10.0,
                best_ask_amount: 8.0,
                open_interest: 42.0,
                underlying_price: 80000.0,
                delta: 0.75,
                gamma: 0.0,
                vega: 0.0,
                theta: 0.0,
                timestamp: now_ms,
            },
        )
        .await;

    let opportunities = analyzer.scan(&registry, &ticker_cache).await;

    assert!(opportunities.is_empty());
}

#[tokio::test]
async fn generates_short_call_yield_for_btc_usdc_options_without_btc_conversion() {
    let registry = InstrumentRegistry::new();
    let ticker_cache = TickerCache::new();
    let analyzer = ShortCallYieldAnalyzer::new();

    let now_ms = chrono::Utc::now().timestamp_millis();
    let expiry_ms = now_ms + 30 * 86_400_000;

    let instruments = serde_json::json!([
        {
            "instrument_name": "BTC_USDC-30APR26-100000-C",
            "strike": 100000.0,
            "expiration_timestamp": expiry_ms,
            "option_type": "call",
            "is_active": true
        }
    ]);
    registry.load_from_response(&instruments).await.unwrap();

    ticker_cache
        .update(
            "BTC_USDC-30APR26-100000-C",
            TickerData {
                mark_price: 2_500.0,
                mark_iv: 55.0,
                best_bid_price: Some(2_500.0),
                best_ask_price: Some(2_525.0),
                best_bid_amount: 10.0,
                best_ask_amount: 8.0,
                open_interest: 42.0,
                underlying_price: 80_000.0,
                delta: 0.25,
                gamma: 0.0,
                vega: 0.0,
                theta: 0.0,
                timestamp: now_ms,
            },
        )
        .await;

    let opportunities = analyzer.scan(&registry, &ticker_cache).await;

    assert_eq!(opportunities.len(), 1);
    let opp = &opportunities[0];

    assert_eq!(opp.instruments, vec!["BTC_USDC-30APR26-100000-C"]);
    approx_eq(opp.expected_profit, 2_500.0, 1e-6);
    approx_eq(opp.total_cost, 100_000.0, 1e-6);
    assert!(matches!(opp.legs[0].price_unit, PriceUnit::Usdc));
    let annualized_return = opp.annualized_return().unwrap();
    approx_eq(
        annualized_return,
        (2_500.0 / 100_000.0) * (365.0 / 30.0),
        0.02,
    );
}
