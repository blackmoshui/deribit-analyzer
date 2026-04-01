use deribit::analysis::short_put_history::{
    aggregate_history_points, build_history_points_from_candles,
    build_history_points_from_candles_with_price_currency, HistoryResolution, IndexPricePoint,
    OptionCandlePoint, ShortPutTradeSample,
};
use deribit::analysis::short_put_history_service::{
    cache_covers_requested_window, compute_fetch_start_ms, index_chart_range_for_resolution,
};
use deribit::market::instruments::OptionPriceCurrency;
use deribit::storage::sqlite::Storage;

fn approx_eq(left: f64, right: f64, tolerance: f64) {
    assert!(
        (left - right).abs() <= tolerance,
        "left={left}, right={right}, tolerance={tolerance}"
    );
}

#[test]
fn aggregates_last_trade_per_bucket_for_short_put_history() {
    let base_ms = 1_700_000_100_000;
    let expiry_ms = base_ms + 30 * 86_400_000;
    let trades = vec![
        ShortPutTradeSample {
            timestamp_ms: base_ms,
            option_price: 0.040,
            index_price: 80_000.0,
        },
        ShortPutTradeSample {
            timestamp_ms: base_ms + 20_000,
            option_price: 0.050,
            index_price: 82_000.0,
        },
        ShortPutTradeSample {
            timestamp_ms: base_ms + 61_000,
            option_price: 0.045,
            index_price: 81_000.0,
        },
    ];

    let points = aggregate_history_points(
        60_000.0,
        expiry_ms,
        HistoryResolution::OneHour,
        &trades,
    );

    assert_eq!(points.len(), 1);
    // 1h bucket: floor(base_ms / 3_600_000) * 3_600_000
    let expected_bucket = (base_ms / 3_600_000) * 3_600_000;
    assert_eq!(points[0].bucket_start_ms, expected_bucket);
    approx_eq(points[0].option_price, 0.045, 1e-9);
    approx_eq(points[0].underlying_price, 81_000.0, 1e-9);
    approx_eq(points[0].premium_usd, 3_645.0, 1e-6);
    approx_eq(
        points[0].annualized_return,
        (3_645.0 / 60_000.0) * (365.0 / ((expiry_ms - (base_ms + 61_000)) as f64 / 86_400_000.0)),
        0.02,
    );
    assert_eq!(points[0].trade_count, 3);
}

#[test]
fn skips_buckets_that_violate_live_short_put_filters() {
    let base_ms = 1_700_000_100_000;
    let expiry_ms = base_ms + 20 * 86_400_000;
    let trades = vec![
        ShortPutTradeSample {
            timestamp_ms: base_ms,
            option_price: 0.060,
            index_price: 58_000.0,
        },
        ShortPutTradeSample {
            timestamp_ms: base_ms + 300_000,
            option_price: 0.055,
            index_price: 79_000.0,
        },
    ];

    let points = aggregate_history_points(60_000.0, expiry_ms, HistoryResolution::OneHour, &trades);

    assert_eq!(points.len(), 1);
    assert_eq!(points[0].bucket_start_ms, 1_699_999_200_000);
    approx_eq(points[0].premium_usd, 4_345.0, 1e-6);
}

#[test]
fn history_resolutions_use_expected_lookback_windows() {
    assert_eq!(
        HistoryResolution::OneHour.lookback_ms(),
        7 * 24 * 60 * 60 * 1000
    );
    assert_eq!(
        HistoryResolution::FourHours.lookback_ms(),
        90 * 24 * 60 * 60 * 1000
    );
}

#[test]
fn index_chart_range_matches_history_resolution() {
    assert_eq!(
        index_chart_range_for_resolution(HistoryResolution::OneHour),
        "1m"
    );
    assert_eq!(
        index_chart_range_for_resolution(HistoryResolution::FourHours),
        "all"
    );
}

#[test]
fn builds_history_points_from_candles_and_index_series() {
    let base_ms = 1_700_000_100_000;
    let expiry_ms = base_ms + 30 * 86_400_000;
    let option_candles = vec![
        OptionCandlePoint {
            tick_ms: base_ms,
            close_price: 0.090,
            volume: 1.0,
        },
        OptionCandlePoint {
            tick_ms: base_ms + 900_000,
            close_price: 0.091,
            volume: 0.0,
        },
    ];
    let index_points = vec![
        IndexPricePoint {
            tick_ms: base_ms - 60_000,
            price: 74_000.0,
        },
        IndexPricePoint {
            tick_ms: base_ms + 300_000,
            price: 75_000.0,
        },
    ];

    let points =
        build_history_points_from_candles(66_000.0, expiry_ms, &option_candles, &index_points);

    assert_eq!(points.len(), 2);
    assert_eq!(points[0].bucket_start_ms, base_ms);
    approx_eq(points[0].premium_usd, 6_660.0, 1e-6);
    assert_eq!(points[0].trade_count, 1);

    assert_eq!(points[1].bucket_start_ms, base_ms + 900_000);
    approx_eq(points[1].underlying_price, 75_000.0, 1e-6);
    approx_eq(points[1].premium_usd, 6_825.0, 1e-6);
    assert_eq!(points[1].trade_count, 0);
}

#[test]
fn builds_history_points_for_btc_usdc_candles_without_btc_repricing() {
    let base_ms = 1_700_000_100_000;
    let expiry_ms = base_ms + 30 * 86_400_000;
    let option_candles = vec![OptionCandlePoint {
        tick_ms: base_ms,
        close_price: 2_400.0,
        volume: 1.0,
    }];
    let index_points = vec![IndexPricePoint {
        tick_ms: base_ms - 60_000,
        price: 80_000.0,
    }];

    let points = build_history_points_from_candles_with_price_currency(
        60_000.0,
        expiry_ms,
        &option_candles,
        &index_points,
        OptionPriceCurrency::QuoteCurrency,
    );

    assert_eq!(points.len(), 1);
    approx_eq(points[0].premium_usd, 2_400.0, 1e-6);
    approx_eq(
        points[0].annualized_return,
        (2_400.0 / 60_000.0) * (365.0 / 30.0),
        0.02,
    );
}

#[test]
fn short_cache_window_is_not_treated_as_full_90d_cover() {
    let start_ms = 1_700_000_000_000;
    let end_ms = start_ms + HistoryResolution::FourHours.lookback_ms();
    let latest_completed_bucket = ((end_ms / HistoryResolution::FourHours.bucket_ms())
        * HistoryResolution::FourHours.bucket_ms())
        - HistoryResolution::FourHours.bucket_ms();

    assert!(!cache_covers_requested_window(
        Some(end_ms - 24 * 60 * 60 * 1000),
        Some(latest_completed_bucket),
        start_ms,
        latest_completed_bucket,
        HistoryResolution::FourHours
    ));
}

#[tokio::test]
async fn stores_and_loads_short_put_history_points_from_cache() {
    let storage = Storage::new(":memory:").await.unwrap();
    let points = vec![
        deribit::analysis::short_put_history::ShortPutHistoryPoint {
            bucket_start_ms: 1_700_000_000_000,
            sample_timestamp_ms: 1_700_000_020_000,
            option_price: 0.050,
            underlying_price: 82_000.0,
            premium_usd: 4_100.0,
            annualized_return: 0.81,
            trade_count: 2,
        },
        deribit::analysis::short_put_history::ShortPutHistoryPoint {
            bucket_start_ms: 1_700_000_060_000,
            sample_timestamp_ms: 1_700_000_061_000,
            option_price: 0.045,
            underlying_price: 81_000.0,
            premium_usd: 3_645.0,
            annualized_return: 0.72,
            trade_count: 1,
        },
    ];

    storage
        .save_short_put_history_points(
            "BTC-30APR26-60000-P",
            HistoryResolution::OneHour,
            &points,
        )
        .await
        .unwrap();

    let loaded = storage
        .load_short_put_history_points(
            "BTC-30APR26-60000-P",
            HistoryResolution::OneHour,
            1_700_000_000_000,
            1_700_000_120_000,
        )
        .await
        .unwrap();

    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded[0].bucket_start_ms, points[0].bucket_start_ms);
    approx_eq(
        loaded[1].annualized_return,
        points[1].annualized_return,
        1e-9,
    );
}

#[tokio::test]
async fn replacing_a_history_window_removes_stale_cached_rows() {
    let storage = Storage::new(":memory:").await.unwrap();
    let old_points = vec![
        deribit::analysis::short_put_history::ShortPutHistoryPoint {
            bucket_start_ms: 1_700_000_000_000,
            sample_timestamp_ms: 1_700_000_020_000,
            option_price: 0.050,
            underlying_price: 82_000.0,
            premium_usd: 4_100.0,
            annualized_return: 0.81,
            trade_count: 2,
        },
        deribit::analysis::short_put_history::ShortPutHistoryPoint {
            bucket_start_ms: 1_700_003_600_000,
            sample_timestamp_ms: 1_700_003_620_000,
            option_price: 0.045,
            underlying_price: 81_000.0,
            premium_usd: 3_645.0,
            annualized_return: 0.72,
            trade_count: 1,
        },
    ];

    storage
        .save_short_put_history_points(
            "BTC-25DEC26-70000-P",
            HistoryResolution::OneHour,
            &old_points,
        )
        .await
        .unwrap();

    storage
        .delete_short_put_history_points_range(
            "BTC-25DEC26-70000-P",
            HistoryResolution::OneHour,
            1_700_000_000_000,
            1_700_010_000_000,
        )
        .await
        .unwrap();

    let new_points = vec![deribit::analysis::short_put_history::ShortPutHistoryPoint {
        bucket_start_ms: 1_700_003_600_000,
        sample_timestamp_ms: 1_700_003_620_000,
        option_price: 0.044,
        underlying_price: 80_500.0,
        premium_usd: 3_542.0,
        annualized_return: 0.70,
        trade_count: 1,
    }];

    storage
        .save_short_put_history_points(
            "BTC-25DEC26-70000-P",
            HistoryResolution::OneHour,
            &new_points,
        )
        .await
        .unwrap();

    let loaded = storage
        .load_short_put_history_points(
            "BTC-25DEC26-70000-P",
            HistoryResolution::OneHour,
            1_700_000_000_000,
            1_700_010_000_000,
        )
        .await
        .unwrap();

    assert_eq!(loaded.len(), 1);
    approx_eq(loaded[0].option_price, 0.044, 1e-9);
}

#[test]
fn incomplete_cache_restarts_fetch_from_requested_start() {
    let requested_start_ms = 1_700_000_000_000;
    let latest_completed_bucket_ms = requested_start_ms + HistoryResolution::FourHours.lookback_ms();

    assert_eq!(
        compute_fetch_start_ms(
            Some(requested_start_ms + 24 * 60 * 60 * 1000),
            requested_start_ms,
            latest_completed_bucket_ms,
            HistoryResolution::FourHours,
        ),
        requested_start_ms
    );
}
