use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, Notify};
use tokio::time::{interval, sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::events::bus::{Event, EventBus};
use crate::ws::auth::AuthState;
use crate::ws::rate_limiter::RateLimiter;

type PendingRequests = Arc<Mutex<HashMap<u64, tokio::sync::oneshot::Sender<Value>>>>;

/// Shared state that allows sending requests from any task
#[derive(Clone)]
pub struct WsClient {
    sender_tx: Arc<Mutex<Option<mpsc::Sender<Message>>>>,
    pending_requests: PendingRequests,
    request_id: Arc<Mutex<u64>>,
    rate_limiter: Arc<Mutex<RateLimiter>>,
}

impl WsClient {
    /// Send a JSON-RPC request and wait for the response
    pub async fn send_request(&self, method: &str, params: Value) -> Result<Value> {
        self.rate_limiter.lock().await.acquire(method).await;

        let id = {
            let mut id = self.request_id.lock().await;
            let current = *id;
            *id += 1;
            current
        };

        let request = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params
        });

        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        self.pending_requests.lock().await.insert(id, response_tx);

        {
            let sender_guard = self.sender_tx.lock().await;
            let sender = sender_guard
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("WebSocket not connected"))?;
            sender
                .send(Message::Text(request.to_string()))
                .await
                .context("Failed to send message")?;
        }

        debug!(method = method, id = id, "Sent request");

        let response = tokio::time::timeout(Duration::from_secs(30), response_rx)
            .await
            .context("Request timed out")?
            .context("Response channel closed")?;

        if let Some(error) = response.get("error") {
            let code = error["code"].as_i64().unwrap_or(0);
            let msg = error["message"].as_str().unwrap_or("Unknown error");
            anyhow::bail!("RPC error {}: {}", code, msg);
        }

        Ok(response["result"].clone())
    }
}

/// WebSocket connection manager - owns the connection lifecycle
pub struct WsManager {
    config: Config,
    event_bus: EventBus,
    client: WsClient,
    auth_state: Arc<Mutex<AuthState>>,
    connected_notify: Arc<Notify>,
}

impl WsManager {
    pub fn new(config: Config, event_bus: EventBus) -> Self {
        let client = WsClient {
            sender_tx: Arc::new(Mutex::new(None)),
            pending_requests: Arc::new(Mutex::new(HashMap::new())),
            request_id: Arc::new(Mutex::new(1)),
            rate_limiter: Arc::new(Mutex::new(RateLimiter::new())),
        };

        WsManager {
            config,
            event_bus,
            client,
            auth_state: Arc::new(Mutex::new(AuthState::new())),
            connected_notify: Arc::new(Notify::new()),
        }
    }

    /// Get a cloneable client handle for sending requests
    pub fn client(&self) -> WsClient {
        self.client.clone()
    }

    /// Wait until a connection is established
    pub async fn wait_connected(&self) {
        self.connected_notify.notified().await;
    }

    /// Run the WebSocket connection loop with auto-reconnect
    pub async fn run(&self) -> Result<()> {
        let mut retry_delay = Duration::from_secs(1);
        let max_retry = Duration::from_secs(60);

        loop {
            info!(url = %self.config.ws_url, "Connecting to Deribit");

            match self.run_connection().await {
                Ok(()) => {
                    info!("Connection closed normally");
                    retry_delay = Duration::from_secs(1);
                }
                Err(e) => {
                    error!(error = %e, "Connection error");
                }
            }

            // Clear state on disconnect
            *self.client.sender_tx.lock().await = None;
            self.client.pending_requests.lock().await.clear();

            warn!(delay_secs = retry_delay.as_secs(), "Reconnecting...");
            sleep(retry_delay).await;
            retry_delay = (retry_delay * 2).min(max_retry);
        }
    }

    async fn run_connection(&self) -> Result<()> {
        let (ws_stream, _) = connect_async(&self.config.ws_url)
            .await
            .context("WebSocket connect failed")?;
        info!("WebSocket connected");

        let (mut ws_sink, mut ws_stream) = ws_stream.split();

        // Channel for outgoing messages
        let (sender_tx, mut sender_rx) = mpsc::channel::<Message>(256);
        *self.client.sender_tx.lock().await = Some(sender_tx.clone());

        let pending_requests = self.client.pending_requests.clone();
        let event_bus = self.event_bus.clone();
        let auth_state = self.auth_state.clone();
        let config = self.config.clone();
        let rate_limiter = self.client.rate_limiter.clone();
        let request_id = self.client.request_id.clone();
        let sender_for_refresh = sender_tx.clone();

        // Start writer: forwards channel messages to WebSocket sink
        let writer_handle = tokio::spawn(async move {
            while let Some(msg) = sender_rx.recv().await {
                if let Err(e) = ws_sink.send(msg).await {
                    error!(error = %e, "Failed to send WebSocket message");
                    break;
                }
            }
        });

        // Start reader: reads from WebSocket stream and dispatches
        let reader_handle = tokio::spawn(async move {
            let mut refresh_interval = interval(Duration::from_secs(30));
            refresh_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    msg = ws_stream.next() => {
                        match msg {
                            Some(Ok(Message::Text(text))) => {
                                // Respond to heartbeat test_request
                                if text.contains("\"test_request\"") {
                                    if let Ok(hb) = serde_json::from_str::<serde_json::Value>(&text) {
                                        if hb.get("method").and_then(|m| m.as_str()) == Some("heartbeat")
                                            && hb["params"]["type"].as_str() == Some("test_request")
                                        {
                                            let hb_id = {
                                                let mut id_lock = request_id.lock().await;
                                                let id = *id_lock;
                                                *id_lock += 1;
                                                id
                                            };
                                            let resp = serde_json::json!({
                                                "jsonrpc": "2.0",
                                                "id": hb_id,
                                                "method": "public/test",
                                                "params": {}
                                            });
                                            let _ = sender_tx.send(Message::Text(resp.to_string())).await;
                                            debug!("Responded to heartbeat test_request");
                                            continue;
                                        }
                                    }
                                }
                                handle_message(
                                    &text,
                                    &pending_requests,
                                    &event_bus,
                                    &auth_state,
                                ).await;
                            }
                            Some(Ok(Message::Ping(data))) => {
                                let _ = sender_tx.send(Message::Pong(data)).await;
                            }
                            Some(Ok(Message::Close(_))) => {
                                info!("Received close frame");
                                break;
                            }
                            Some(Err(e)) => {
                                error!(error = %e, "WebSocket error");
                                break;
                            }
                            None => {
                                info!("WebSocket stream ended");
                                break;
                            }
                            _ => {}
                        }
                    }
                    _ = refresh_interval.tick() => {
                        let needs_refresh = auth_state.lock().await.needs_refresh();
                        if needs_refresh {
                            let refresh_token = auth_state.lock().await
                                .get_refresh_token()
                                .map(|s| s.to_string());

                            let id = {
                                let mut id_lock = request_id.lock().await;
                                let id = *id_lock;
                                *id_lock += 1;
                                id
                            };

                            let req = if let Some(token) = refresh_token {
                                AuthState::build_refresh_request(&token, id)
                            } else {
                                AuthState::build_auth_request(
                                    &config.client_id,
                                    &config.client_secret,
                                    id,
                                )
                            };

                            rate_limiter.lock().await.acquire("public/auth").await;
                            let _ = sender_for_refresh
                                .send(Message::Text(req.to_string()))
                                .await;
                            info!("Sent token refresh request");
                        }
                    }
                }
            }
        });

        // Now authenticate (reader & writer are already running)
        self.authenticate().await?;

        // Setup heartbeat
        let _result = self
            .client
            .send_request(
                "public/set_heartbeat",
                json!({"interval": self.config.heartbeat_interval}),
            )
            .await?;
        info!("Heartbeat configured");

        // Notify waiters that we're connected
        self.connected_notify.notify_waiters();

        // Wait for reader to finish (connection closed)
        let _ = reader_handle.await;
        writer_handle.abort();
        Ok(())
    }

    async fn authenticate(&self) -> Result<()> {
        let id = {
            let mut id = self.client.request_id.lock().await;
            let current = *id;
            *id += 1;
            current
        };

        let auth_req =
            AuthState::build_auth_request(&self.config.client_id, &self.config.client_secret, id);

        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        self.client
            .pending_requests
            .lock()
            .await
            .insert(id, response_tx);

        {
            let sender_guard = self.client.sender_tx.lock().await;
            let sender = sender_guard
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Not connected"))?;
            sender
                .send(Message::Text(auth_req.to_string()))
                .await
                .context("Failed to send auth request")?;
        }

        let response = tokio::time::timeout(Duration::from_secs(10), response_rx)
            .await
            .context("Auth timed out")?
            .context("Auth channel closed")?;

        if let Some(error) = response.get("error") {
            anyhow::bail!("Auth failed: {}", error);
        }

        self.auth_state
            .lock()
            .await
            .process_auth_response(&response["result"])?;

        Ok(())
    }
}

async fn handle_message(
    text: &str,
    pending_requests: &PendingRequests,
    event_bus: &EventBus,
    auth_state: &Arc<Mutex<AuthState>>,
) {
    let msg: Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(e) => {
            warn!(error = %e, "Failed to parse message");
            return;
        }
    };

    // RPC response (has "id")
    if let Some(id) = msg.get("id").and_then(|id| id.as_u64()) {
        // Check if this is an auth response
        if msg.get("result").is_some() && msg["result"].get("access_token").is_some() {
            if let Err(e) = auth_state
                .lock()
                .await
                .process_auth_response(&msg["result"])
            {
                warn!(error = %e, "Failed to process auth response");
            }
        }

        let mut pending = pending_requests.lock().await;
        if let Some(tx) = pending.remove(&id) {
            let _ = tx.send(msg);
        }
        return;
    }

    // Subscription notification
    if msg.get("method").and_then(|m| m.as_str()) == Some("subscription") {
        let params = &msg["params"];
        let channel = params["channel"].as_str().unwrap_or("");
        let data = &params["data"];

        if channel.starts_with("ticker.") {
            handle_ticker_update(data, event_bus);
        } else if channel.starts_with("book.") {
            handle_orderbook_update(data, event_bus);
        } else if channel.starts_with("trades.") {
            debug!(channel = channel, "Trade update received");
        }
    }
}

fn handle_ticker_update(data: &Value, event_bus: &EventBus) {
    let instrument_name = data["instrument_name"].as_str().unwrap_or("").to_string();

    let ticker_data = crate::events::bus::TickerData {
        mark_price: data["mark_price"].as_f64().unwrap_or(0.0),
        mark_iv: data["mark_iv"].as_f64().unwrap_or(0.0),
        best_bid_price: data["best_bid_price"].as_f64(),
        best_ask_price: data["best_ask_price"].as_f64(),
        best_bid_amount: data["best_bid_amount"].as_f64().unwrap_or(0.0),
        best_ask_amount: data["best_ask_amount"].as_f64().unwrap_or(0.0),
        open_interest: data["open_interest"].as_f64().unwrap_or(0.0),
        underlying_price: data["underlying_price"].as_f64().unwrap_or(0.0),
        delta: data["greeks"]["delta"].as_f64().unwrap_or(0.0),
        gamma: data["greeks"]["gamma"].as_f64().unwrap_or(0.0),
        vega: data["greeks"]["vega"].as_f64().unwrap_or(0.0),
        theta: data["greeks"]["theta"].as_f64().unwrap_or(0.0),
        timestamp: data["timestamp"].as_i64().unwrap_or(0),
    };

    event_bus.publish(Event::TickerUpdate {
        instrument_name,
        data: ticker_data,
    });
}

fn handle_orderbook_update(data: &Value, event_bus: &EventBus) {
    let instrument_name = data["instrument_name"].as_str().unwrap_or("").to_string();

    let parse_levels = |key: &str| -> Vec<(f64, f64)> {
        data[key]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|level| {
                        let price = level[1].as_f64()?;
                        let amount = level[2].as_f64()?;
                        Some((price, amount))
                    })
                    .collect()
            })
            .unwrap_or_default()
    };

    let bids = parse_levels("bids");
    let asks = parse_levels("asks");

    event_bus.publish(Event::OrderbookUpdate {
        instrument_name,
        bids,
        asks,
    });
}
