use anyhow::Result;
use serde_json::json;
use tracing::info;

use crate::ws::client::WsClient;

/// Manages channel subscriptions
pub struct Subscriber;

impl Subscriber {
    /// Subscribe to ticker channels for given instruments
    pub async fn subscribe_tickers(client: &WsClient, instrument_names: &[String]) -> Result<()> {
        // Subscribe in batches to respect rate limits
        let batch_size = 20;
        for chunk in instrument_names.chunks(batch_size) {
            let channels: Vec<String> = chunk
                .iter()
                .map(|name| format!("ticker.{}.100ms", name))
                .collect();

            let _result = client
                .send_request("public/subscribe", json!({"channels": channels}))
                .await?;

            info!(count = chunk.len(), "Subscribed to ticker channels");
        }
        Ok(())
    }

    /// Subscribe to orderbook channels for given instruments
    pub async fn subscribe_orderbooks(
        client: &WsClient,
        instrument_names: &[String],
    ) -> Result<()> {
        let batch_size = 20;
        for chunk in instrument_names.chunks(batch_size) {
            let channels: Vec<String> = chunk
                .iter()
                .map(|name| format!("book.{}.100ms", name))
                .collect();

            let _result = client
                .send_request("public/subscribe", json!({"channels": channels}))
                .await?;

            info!(count = chunk.len(), "Subscribed to orderbook channels");
        }
        Ok(())
    }

    /// Subscribe to trade channels
    pub async fn subscribe_trades(client: &WsClient, instrument_names: &[String]) -> Result<()> {
        let batch_size = 20;
        for chunk in instrument_names.chunks(batch_size) {
            let channels: Vec<String> = chunk
                .iter()
                .map(|name| format!("trades.{}.100ms", name))
                .collect();

            let _result = client
                .send_request("public/subscribe", json!({"channels": channels}))
                .await?;

            info!(count = chunk.len(), "Subscribed to trade channels");
        }
        Ok(())
    }
}
