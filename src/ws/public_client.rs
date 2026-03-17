use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::ws::rate_limiter::RateLimiter;

#[derive(Clone)]
pub struct PublicRpcClient {
    ws_url: String,
    rate_limiter: Arc<Mutex<RateLimiter>>,
    request_id: Arc<Mutex<u64>>,
}

impl PublicRpcClient {
    pub fn new(ws_url: impl Into<String>) -> Self {
        Self {
            ws_url: ws_url.into(),
            rate_limiter: Arc::new(Mutex::new(RateLimiter::new())),
            request_id: Arc::new(Mutex::new(1)),
        }
    }

    pub async fn send_request(&self, method: &str, params: Value) -> Result<Value> {
        self.rate_limiter.lock().await.acquire(method).await;

        let id = {
            let mut request_id = self.request_id.lock().await;
            let current = *request_id;
            *request_id += 1;
            current
        };

        let request = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });

        let (ws_stream, _) = connect_async(&self.ws_url)
            .await
            .with_context(|| format!("Failed to connect to {}", self.ws_url))?;
        let (mut write, mut read) = ws_stream.split();

        write
            .send(Message::Text(request.to_string()))
            .await
            .context("Failed to send public RPC request")?;

        while let Some(message) = read.next().await {
            match message.context("WebSocket read failed")? {
                Message::Text(text) => {
                    let response: Value =
                        serde_json::from_str(&text).context("Invalid JSON-RPC response")?;
                    if response.get("id").and_then(Value::as_u64) != Some(id) {
                        continue;
                    }

                    if let Some(error) = response.get("error") {
                        let code = error["code"].as_i64().unwrap_or_default();
                        let msg = error["message"].as_str().unwrap_or("Unknown error");
                        anyhow::bail!("RPC error {}: {}", code, msg);
                    }

                    return Ok(response["result"].clone());
                }
                Message::Ping(payload) => {
                    write
                        .send(Message::Pong(payload))
                        .await
                        .context("Failed to reply to ping")?;
                }
                Message::Close(_) => break,
                _ => {}
            }
        }

        anyhow::bail!("Connection closed before response for {}", method)
    }
}
