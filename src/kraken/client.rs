use crate::kraken::websocket_auth;
use crate::error_handling::AppError;
use crate::mongo::client::MongoClient;
use futures_util::{SinkExt, StreamExt};
use kraken_rest_client::OrderSide as KrakenOrderSide;
use serde::{Deserialize, Serialize};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, WebSocketStream};
use bytes::Bytes;
use log::{debug, error, info, warn};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use serde_json::Value;
use crate::kraken::orderbook::{OrderBookAnalyzer, Signal};
use std::collections::HashMap;

const KRAKEN_PUBLIC_WS_URL: &str = "wss://ws.kraken.com/v2";
const KRAKEN_PRIVATE_WS_URL: &str = "wss://ws-auth.kraken.com/v2";
const MAX_RECONNECT_ATTEMPTS: u32 = 5;
const INITIAL_BACKOFF_MS: u64 = 1000;
const PING_INTERVAL_SECS: u64 = 30;

// Public Types
#[derive(Debug)]
pub struct Candle {
    pub timestamp: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

impl Candle {
    /// Calculates the volume in terms of the base currency value
    /// by multiplying the trading volume with the closing price
    pub fn volume_in_base_currency(&self) -> f64 {
        self.close * self.volume
    }

    /// Prints trading statistics for this candle
    pub fn print_stats(&self) {
        info!(
            "Trading stats - Close: {}, Volume: {}, Total Value: {}",
            self.close,
            self.volume,
            self.volume_in_base_currency()
        );
    }
}

pub struct OrderResponse {
    pub id: String,
    pub status: String,
    pub price: f64,
    pub volume: f64,
    pub pair: String,
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum OrderType {
    Market,
    Limit,
    StopLoss,
    StopLossLimit,
    TakeProfit,
    TakeProfitLimit,
    TrailingStop,
    TrailingStopLimit,
    Iceberg,
    SettlePosition,
}

#[derive(Debug)]
pub enum OrderSide {
    Buy,
    Sell,
}

// Private Types
#[derive(Serialize, Deserialize, Debug)]
struct OHLCSubscription {
    method: String,
    params: OHLCParams,
}

#[derive(Serialize, Deserialize, Debug)]
struct OHLCParams {
    channel: String,
    symbol: Vec<String>,
    interval: i32,
    snapshot: bool,
}

#[derive(Serialize, Deserialize, Debug)]
struct OHLCResponse {
    channel: String,
    #[serde(rename = "type")]
    response_type: String,
    data: Vec<OHLCData>,
}

#[derive(Serialize, Deserialize, Debug)]
struct OHLCData {
    symbol: String,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    interval_begin: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct WebSocketMessage {
    event: String,
    #[serde(flatten)]
    data: serde_json::Value,
}

#[derive(Serialize, Deserialize, Debug)]
struct TickerResponse {
    channel: String,
    #[serde(rename = "type")]
    response_type: String,
    data: Vec<TickerData>,
}

#[derive(Serialize, Deserialize, Debug)]
struct TickerData {
    symbol: String,
    last: f64,
}

// Helper function to create ping message
fn create_ping() -> Message {
    Message::Ping(Bytes::from(vec![]))
}

// Implementations
impl fmt::Debug for OrderResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OrderResponse")
            .field("id", &self.id)
            .field("status", &self.status)
            .field("price", &self.price)
            .field("volume", &self.volume)
            .field("pair", &self.pair)
            .finish()
    }
}

impl OrderType {
    fn as_str(&self) -> &'static str {
        match self {
            OrderType::Market => "market",
            OrderType::Limit => "limit",
            OrderType::StopLoss => "stop-loss",
            OrderType::StopLossLimit => "stop-loss-limit",
            OrderType::TakeProfit => "take-profit",
            OrderType::TakeProfitLimit => "take-profit-limit",
            OrderType::TrailingStop => "trailing-stop",
            OrderType::TrailingStopLimit => "trailing-stop-limit",
            OrderType::Iceberg => "iceberg",
            OrderType::SettlePosition => "settle-position",
        }
    }
}

// Main Client Implementation
pub struct KrakenClient {
    private_ws:
        Arc<Mutex<WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>>>,
    public_ws:
        Arc<Mutex<WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>>>,
    is_connected: Arc<AtomicBool>,
    api_key: String,
    api_secret: String,
    order_book_analyzers: HashMap<String, Arc<Mutex<OrderBookAnalyzer>>>,
    volume_threshold: f64,
}

impl KrakenClient {
    // Simplified ping task
    async fn start_ping_task(
        private_ws: Arc<Mutex<WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>>>,
        public_ws: Arc<Mutex<WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>>>,
        is_connected: Arc<AtomicBool>,
        api_key: String,
        api_secret: String,
    ) {
        let mut interval = tokio::time::interval(Duration::from_secs(PING_INTERVAL_SECS));

        tokio::spawn(async move {
            while is_connected.load(Ordering::SeqCst) {
                interval.tick().await;

                // Ping both connections
                for (ws, conn_type) in [(private_ws.clone(), "private"), (public_ws.clone(), "public")] {
                    let mut ws_lock = ws.lock().await;
                    match ws_lock.send(create_ping()).await {
                        Ok(_) => debug!("Successfully pinged {} connection", conn_type),
                        Err(e) => {
                            error!("Failed to ping {} connection: {:?}", conn_type, e);
                            drop(ws_lock);
                            if let Err(e) = Self::attempt_reconnection(&private_ws, &public_ws, &is_connected, &api_key, &api_secret).await {
                                error!("Failed to reconnect after {} ping failure: {:?}", conn_type, e);
                                is_connected.store(false, Ordering::SeqCst);
                                break;
                            }
                        }
                    }
                }
            }
            info!("Ping task stopped");
        });
    }

    async fn attempt_reconnection(
        private_ws: &Arc<Mutex<WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>>>,
        public_ws: &Arc<Mutex<WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>>>,
        is_connected: &Arc<AtomicBool>,
        api_key: &str,
        api_secret: &str,
    ) -> Result<(), AppError> {
        let mut attempts = 0;
        while attempts < MAX_RECONNECT_ATTEMPTS {
            let backoff = INITIAL_BACKOFF_MS * 2u64.pow(attempts);
            info!(
                "Attempting reconnection in {}ms (attempt {}/{})",
                backoff,
                attempts + 1,
                MAX_RECONNECT_ATTEMPTS
            );
            tokio::time::sleep(Duration::from_millis(backoff)).await;

            match Self::reconnect_websockets(private_ws, public_ws, api_key, api_secret).await {
                Ok(_) => {
                    info!("Successfully reconnected on attempt {}", attempts + 1);
                    is_connected.store(true, Ordering::SeqCst);
                    return Ok(());
                }
                Err(e) => {
                    error!("Reconnection attempt {} failed: {:?}", attempts + 1, e);
                    attempts += 1;
                }
            }
        }
        Err(AppError::ConnectionError)
    }

    async fn reconnect_websockets(
        private_ws: &Arc<Mutex<WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>>>,
        public_ws: &Arc<Mutex<WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>>>,
        api_key: &str,
        api_secret: &str,
    ) -> Result<(), AppError> {
        info!("Establishing new WebSocket connections...");

        let (new_private_ws, _) = connect_async(KRAKEN_PRIVATE_WS_URL).await.map_err(|e| {
            error!("Failed to establish private WebSocket connection: {:?}", e);
            AppError::ConnectionError
        })?;

        let (new_public_ws, _) = connect_async(KRAKEN_PUBLIC_WS_URL).await.map_err(|e| {
            error!("Failed to establish public WebSocket connection: {:?}", e);
            AppError::ConnectionError
        })?;

        // Replace the old connections with new ones
        let mut private_lock = private_ws.lock().await;
        let mut public_lock = public_ws.lock().await;
        *private_lock = new_private_ws;
        *public_lock = new_public_ws;
        drop(private_lock);
        drop(public_lock);

        // Re-authenticate the private connection
        let token = websocket_auth::get_ws_token(api_key, api_secret).await?;
        let auth_message = serde_json::json!({
            "method": "subscribe",
            "params": {
                "channel": "balances",
                "token": token,
            }
        });

        info!("Re-authenticating private WebSocket connection...");
        let mut private_ws_lock = private_ws.lock().await;
        private_ws_lock
            .send(Message::Text(auth_message.to_string().into()))
            .await
            .map_err(|e| {
                error!("Failed to send authentication message: {:?}", e);
                AppError::ConnectionError
            })?;

        // Wait for auth confirmation
        while let Some(msg) = private_ws_lock.next().await {
            let msg = msg.map_err(|e| {
                error!("WebSocket error during re-authentication: {:?}", e);
                AppError::ConnectionError
            })?;

            if let Message::Text(text) = msg {
                if text.contains("\"channel\":\"balances\"")
                    && text.contains("\"type\":\"snapshot\"")
                {
                    info!("Successfully re-authenticated");
                    break;
                }
            }
        }

        info!("WebSocket connections re-established and re-authenticated successfully");
        Ok(())
    }

    pub async fn new() -> Result<Self, AppError> {
        let api_key = std::env::var("KRAKEN_API_KEY").map_err(|_| AppError::ConnectionError)?;
        let api_secret =
            std::env::var("KRAKEN_API_SECRET").map_err(|_| AppError::ConnectionError)?;

        // Store credentials for later use
        let stored_api_key = api_key.clone();
        let stored_api_secret = api_secret.clone();

        // Validate credentials first
        websocket_auth::validate_api_credentials(&api_key, &api_secret)?;
        websocket_auth::verify_api_permissions(&api_key, &api_secret).await?;

        // Connect to both endpoints
        info!("Initializing Kraken WebSocket connections...");
        let (private_ws, _) = connect_async(KRAKEN_PRIVATE_WS_URL).await.map_err(|e| {
            error!("Failed to connect to private WebSocket: {:?}", e);
            AppError::ConnectionError
        })?;

        let (public_ws, _) = connect_async(KRAKEN_PUBLIC_WS_URL).await.map_err(|e| {
            error!("Failed to connect to public WebSocket: {:?}", e);
            AppError::ConnectionError
        })?;

        let private_ws = Arc::new(Mutex::new(private_ws));
        let public_ws = Arc::new(Mutex::new(public_ws));
        let is_connected = Arc::new(AtomicBool::new(true));

        let client = Self {
            private_ws,
            public_ws,
            is_connected,
            api_key: stored_api_key,
            api_secret: stored_api_secret,
            order_book_analyzers: HashMap::new(),
            volume_threshold: 1.0,
        };

        // Authenticate the private connection
        client.authenticate(&api_key, &api_secret).await?;

        info!("WebSocket connections established and authenticated");
        Self::start_ping_task(
            client.private_ws.clone(),
            client.public_ws.clone(),
            client.is_connected.clone(),
            client.api_key.clone(),
            client.api_secret.clone(),
        )
        .await;

        Ok(client)
    }

    async fn authenticate(&self, api_key: &str, api_secret: &str) -> Result<(), AppError> {
        let token = websocket_auth::get_ws_token(api_key, api_secret).await?;

        let auth_message = serde_json::json!({
            "method": "subscribe",
            "params": {
                "channel": "balances",
                "token": token,
            }
        });

        info!("Sending authentication request...");
        self.send_with_retry(&auth_message.to_string()).await?;

        // Wait for auth response
        let mut ws = self.private_ws.lock().await;
        while let Some(msg) = ws.next().await {
            let msg = msg.map_err(|e| {
                error!("WebSocket error during authentication: {:?}", e);
                AppError::ConnectionError
            })?;

            if let Message::Text(text) = msg {
                // debug!("Received auth response: {}", text);

                // Try parsing the response and log any errors
                match serde_json::from_str::<serde_json::Value>(&text) {
                    Ok(response) => {
                        // debug!("Successfully parsed response: {:?}", response);
                        if text.contains("\"channel\":\"balances\"") {
                            if response["type"] == "snapshot" {
                                info!("Successfully authenticated");
                                return Ok(());
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            "Failed to parse auth response: {:?}\nResponse text: {}",
                            e, text
                        );
                        return Err(AppError::ParseError);
                    }
                }
            }
        }

        error!("Authentication loop ended without success");
        Err(AppError::ConnectionError)
    }

    pub async fn subscribe_to_ticker(&self, pair: &str) -> Result<(), AppError> {
        let subscription = serde_json::json!({
            "method": "subscribe",
            "params": {
                "channel": "ticker",
                "symbol": [pair],
            }
        });
        self.send_with_retry(&subscription.to_string()).await?;

        Ok(())
    }

    pub async fn subscribe_to_ohlc(&self, pair: &str, interval: i32) -> Result<(), AppError> {
        let subscription = OHLCSubscription {
            method: "subscribe".to_string(),
            params: OHLCParams {
                channel: "ohlc".to_string(),
                symbol: vec![pair.to_string()],
                interval,
                snapshot: true,
            },
        };
        let json = serde_json::to_string(&subscription).map_err(|_| AppError::ParseError)?;
        self.send_with_retry(&json).await?;

        Ok(())
    }

    async fn handle_message(&self, msg: Message) -> Result<Option<String>, AppError> {
        match msg {
            Message::Text(text) => Ok(Some(text.to_string())),
            Message::Ping(data) => {
                info!("Received ping, sending pong...");
                let mut ws = self.private_ws.lock().await;
                ws.send(Message::Pong(data)).await.map_err(|e| {
                    error!("Failed to send pong: {:?}", e);
                    AppError::ConnectionError
                })?;
                Ok(None)
            }
            Message::Pong(_) => {
                debug!("Received pong");
                Ok(None)
            }
            Message::Close(frame) => {
                error!("Received close frame: {:?}", frame);
                Err(AppError::ConnectionError)
            }
            _ => Ok(None),
        }
    }

    pub async fn get_ohlc_data(&self, pair: &str, interval: i32) -> Result<Vec<Candle>, AppError> {
        debug!(
            "Subscribing to OHLC data for {} with interval {}",
            pair, interval
        );
        self.subscribe_to_ohlc(pair, interval).await?;
        let mut ws = self.public_ws.lock().await;

        debug!("Waiting for OHLC response...");
        while let Some(msg) = ws.next().await {
            let msg = msg.map_err(|e| {
                error!("WebSocket error: {:?}", e);
                AppError::ConnectionError
            })?;

            if let Some(text) = self.handle_message(msg).await? {
                debug!("Received WebSocket message: {}", text);

                // Skip subscription acknowledgment messages
                if text.contains("\"method\":\"subscribe\"") {
                    debug!("Received subscription acknowledgment, waiting for data...");
                    continue;
                }

                // Try parsing as status message
                if text.contains("\"channel\":\"status\"") {
                    debug!("Received status message, continuing to wait for OHLC data");
                    continue;
                }

                // Try parsing as OHLC response
                match serde_json::from_str::<OHLCResponse>(&text) {
                    Ok(response) => {
                        debug!("Parsing {} OHLC entries", response.data.len());
                        let candles: Result<Vec<Candle>, AppError> = response
                            .data
                            .into_iter()
                            .map(|ohlc| {
                                let timestamp =
                                    chrono::DateTime::parse_from_rfc3339(&ohlc.interval_begin)
                                        .map_err(|e| {
                                            error!(
                                                "Failed to parse timestamp '{}': {:?}",
                                                ohlc.interval_begin, e
                                            );
                                            AppError::ParseError
                                        })?;

                                Ok(Candle {
                                    timestamp: timestamp.timestamp(),
                                    open: ohlc.open,
                                    high: ohlc.high,
                                    low: ohlc.low,
                                    close: ohlc.close,
                                    volume: ohlc.volume,
                                })
                            })
                            .collect();

                        let candles = candles?;  // First unwrap the Result
                        for candle in &candles {
                            candle.print_stats();
                        }
                        return Ok(candles);
                    }
                    Err(e) => {
                        error!(
                            "Failed to parse as OHLC response: {:?}\nMessage: {}",
                            e, text
                        );
                        continue;
                    }
                }
            }
        }

        error!("WebSocket connection closed without receiving OHLC data");
        Err(AppError::ConnectionError)
    }

    pub async fn add_order(
        &self,
        pair: &str,
        side: OrderSide,
        order_type: OrderType,
        volume: f64,
        price_params: OrderPriceParams,
        _user_id: i64,
    ) -> Result<OrderResponse, AppError> {
        // Get current market price for context
        match self.get_current_price(pair).await {
            Ok(current_price) => info!(
                "Market price at order time - {}: {}",
                pair, current_price
            ),
            Err(e) => warn!("Could not fetch current price: {:?}", e),
        }

        info!("=== Starting new order placement ===");
        
        let kraken_side: KrakenOrderSide = match side {
            OrderSide::Buy => KrakenOrderSide::Buy,
            OrderSide::Sell => KrakenOrderSide::Sell,
        };

        info!(
            "Order details:\n\
             - Pair: {}\n\
             - Side: {}\n\
             - Type: {}\n\
             - Volume: {}\n\
             - Price params: {:?}",
            pair, kraken_side.to_string(), order_type.as_str(), volume, price_params
        );

        let mut order_data = serde_json::json!({
            "method": "createOrder",
            "params": {
                "symbol": pair,
                "side": kraken_side.to_string(),
                "orderType": order_type.as_str(),
                "quantity": volume.to_string(),
            }
        });

        // Add price parameters based on order type
        match price_params {
            OrderPriceParams::Market => (), // No additional parameters needed
            OrderPriceParams::Limit { price } => {
                order_data["params"]["price"] = serde_json::json!(price.to_string());
            }
            OrderPriceParams::StopLoss { stop_price } => {
                order_data["params"]["stopPrice"] = serde_json::json!(stop_price.to_string());
            }
            OrderPriceParams::StopLossLimit { stop_price, limit_price } => {
                order_data["params"]["stopPrice"] = serde_json::json!(stop_price.to_string());
                order_data["params"]["price"] = serde_json::json!(limit_price.to_string());
            }
            OrderPriceParams::TakeProfit { take_profit_price } => {
                order_data["params"]["takeProfitPrice"] = serde_json::json!(take_profit_price.to_string());
            }
            OrderPriceParams::TakeProfitLimit { take_profit_price, limit_price } => {
                order_data["params"]["takeProfitPrice"] = serde_json::json!(take_profit_price.to_string());
                order_data["params"]["price"] = serde_json::json!(limit_price.to_string());
            }
            OrderPriceParams::TrailingStop { trailing_distance } => {
                order_data["params"]["trailingDistance"] = serde_json::json!(trailing_distance.to_string());
            }
            OrderPriceParams::TrailingStopLimit { trailing_distance, limit_offset } => {
                order_data["params"]["trailingDistance"] = serde_json::json!(trailing_distance.to_string());
                order_data["params"]["limitOffset"] = serde_json::json!(limit_offset.to_string());
            }
            OrderPriceParams::Iceberg { display_volume, limit_price } => {
                order_data["params"]["price"] = serde_json::json!(limit_price.to_string());
                order_data["params"]["displayQuantity"] = serde_json::json!(display_volume.to_string());
            }
        }

        let order_str = order_data.to_string();
        info!("Sending order request to Kraken: {}", serde_json::to_string_pretty(&order_data)?);

        match self.send_with_retry(&order_str).await {
            Ok(_) => info!("Order request sent successfully, waiting for response..."),
            Err(e) => {
                error!("Failed to send order request: {:?}", e);
                return Err(e);
            }
        }

        let mut retries = 3;
        while retries > 0 {
            debug!("Waiting for order response (retries left: {})", retries);
            let mut ws = self.private_ws.lock().await;

            while let Some(msg) = ws.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        debug!("Received message: {}", text);

                        if text.contains("\"method\":\"subscribe\"") {
                            debug!("Skipping subscription message while waiting for order response");
                            continue;
                        }
                        if text.contains("\"channel\":\"status\"") {
                            debug!("Skipping status message while waiting for order response");
                            continue;
                        }

                        match serde_json::from_str::<WebSocketMessage>(&text) {
                            Ok(response) => {
                                if let Some(error) = response.data.get("error") {
                                    error!("Order error response: {}", error);
                                    return Err(AppError::ParseError);
                                }

                                if response.event == "createOrder"
                                    || response.event == "orderStatus"
                                {
                                    info!("=== Order placement completed successfully ===");
                                    info!("Order details: {:?}", response.data);
                                    let order = OrderResponse {
                                        id: response.data["orderId"]
                                            .as_str()
                                            .unwrap_or("unknown")
                                            .to_string(),
                                        status: response.data["status"]
                                            .as_str()
                                            .unwrap_or("pending")
                                            .to_string(),
                                        price: response.data["price"]
                                            .as_str()
                                            .and_then(|p| p.parse().ok())
                                            .unwrap_or(0.0),
                                        volume,
                                        pair: pair.to_string(),
                                    };
                                    info!("Order created: {:?}", order);
                                    return Ok(order);
                                }
                                debug!("Received non-order event: {}", response.event);
                            }
                            Err(e) => {
                                error!("Failed to parse response: {:?}\nRaw message: {}", e, text);
                                continue;
                            }
                        }
                    }
                    Err(e) => {
                        error!("WebSocket error: {:?}", e);
                        drop(ws);
                        info!("Attempting reconnection...");
                        if let Err(e) = self.reconnect().await {
                            error!("Reconnection attempt {} failed: {:?}", 4 - retries, e);
                            retries -= 1;
                            if retries == 0 {
                                error!("All reconnection attempts failed");
                                return Err(AppError::ConnectionError);
                            }
                            break;
                        }
                        info!("Successfully reconnected, retrying order");
                        break;
                    }
                    _ => {
                        debug!("Received non-text message");
                        continue;
                    }
                }
            }
        }

        error!("Failed to receive order confirmation after all retries");
        Err(AppError::ConnectionError)
    }

    pub async fn get_current_price(&self, pair: &str) -> Result<f64, AppError> {
        info!("=== Fetching current price for {} ===", pair);
        self.subscribe_to_ticker(pair).await?;
        let mut ws = self.public_ws.lock().await;

        while let Some(msg) = ws.next().await {
            let msg = msg.map_err(|e| {
                error!("WebSocket error: {:?}", e);
                AppError::ConnectionError
            })?;

            if let Message::Text(text) = msg {
                debug!("Received ticker message: {}", text);

                // Skip subscription acknowledgment
                if text.contains("\"method\":\"subscribe\"") {
                    debug!("Received subscription acknowledgment, waiting for ticker data...");
                    continue;
                }

                // Skip status messages
                if text.contains("\"channel\":\"status\"")
                    || text.contains("\"channel\":\"heartbeat\"")
                {
                    debug!("Received status/heartbeat message, continuing to wait for ticker data");
                    continue;
                }

                // Try parsing as ticker response
                match serde_json::from_str::<TickerResponse>(&text) {
                    Ok(response) => {
                        if let Some(ticker) = response.data.first() {
                            info!("Current price for {}: {}", pair, ticker.last);
                            return Ok(ticker.last);
                        }
                    }
                    Err(e) => {
                        error!(
                            "Failed to parse ticker response: {:?}\nMessage: {}",
                            e, text
                        );
                        continue;
                    }
                }
            }
        }

        error!("WebSocket connection closed without receiving ticker data");
        Err(AppError::ConnectionError)
    }

    async fn reconnect(&self) -> Result<(), AppError> {
        info!("Starting WebSocket reconnection process...");
        let start = std::time::Instant::now();

        let (private_ws, _) = connect_async(KRAKEN_PRIVATE_WS_URL).await.map_err(|e| {
            error!("Failed to establish new WebSocket connection: {:?}", e);
            AppError::ConnectionError
        })?;

        let (public_ws, _) = connect_async(KRAKEN_PUBLIC_WS_URL).await.map_err(|e| {
            error!("Failed to establish new WebSocket connection: {:?}", e);
            AppError::ConnectionError
        })?;

        let mut private_lock = self.private_ws.lock().await;
        let mut public_lock = self.public_ws.lock().await;
        *private_lock = private_ws;
        *public_lock = public_ws;
        self.is_connected.store(true, Ordering::SeqCst);

        // Restart ping task
        Self::start_ping_task(
            self.private_ws.clone(),
            self.public_ws.clone(),
            self.is_connected.clone(),
            self.api_key.clone(),
            self.api_secret.clone(),
        )
        .await;

        info!("WebSocket reconnected successfully");
        info!("Total reconnection time: {:?}", start.elapsed());
        Ok(())
    }

    async fn send_with_retry(&self, message: &str) -> Result<(), AppError> {
        let mut retries = 3;
        while retries > 0 {
            info!("Attempting to send message (retries left: {})", retries);
            let start = std::time::Instant::now();
            let mut ws = self.private_ws.lock().await;
            debug!("Acquired WebSocket lock after {:?}", start.elapsed());

            match ws.send(Message::Text(message.to_string().into())).await {
                Ok(_) => {
                    info!("Message sent successfully");
                    return Ok(());
                }
                Err(e) => {
                    error!("Failed to send message: {:?}", e);
                    error!("WebSocket state before drop: {:?}", ws);
                    drop(ws);
                    info!("Attempting reconnection after send failure");
                    if let Err(e) = self.reconnect().await {
                        error!("Reconnection attempt {} failed: {:?}", 4 - retries, e);
                        retries -= 1;
                        if retries == 0 {
                            error!("All send retries exhausted");
                            return Err(AppError::ConnectionError);
                        }
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    }
                }
            }
        }
        Err(AppError::ConnectionError)
    }

    /// Withdraws SOL from Kraken to the configured wallet address if there are sufficient profits
    /// Returns the withdrawal response which includes the reference ID
    pub async fn withdraw_sol(&self, amount: f64, user_id: i64) -> Result<Value, AppError> {
        info!("=== Initiating SOL withdrawal process ===");
        
        let mongo_client = MongoClient::new().await?;
        
        // First check profits from MongoDB
        let profits = mongo_client.get_total_profits(user_id).await?;
        
        info!("Current total profits: {} SOL", profits);
        
        // Ensure amount is positive and profits exist
        if amount <= 0.0 {
            error!("Invalid withdrawal amount: {} SOL", amount);
            return Err(AppError::InvalidInput);
        }

        // Check if we have any profits at all
        if profits <= 0.0 {
            error!("No profits available for withdrawal. Current profits: {} SOL", profits);
            return Err(AppError::InsufficientFunds);
        }
        
        // Check if we have enough profits for the requested amount
        if profits < amount {
            error!(
                "Insufficient profits for withdrawal. Available profits: {} SOL, Requested: {} SOL",
                profits, amount
            );
            return Err(AppError::InsufficientFunds);
        }
        
        // Verify we have sufficient balance on Kraken
        let balance = self.get_sol_balance().await?;
        
        if balance < amount {
            error!(
                "Insufficient SOL balance on Kraken. Available: {} SOL, Requested: {} SOL",
                balance, amount
            );
            return Err(AppError::InsufficientFunds);
        }

        info!("Sufficient balance and profits verified:");
        info!("  - Available balance: {} SOL", balance);
        info!("  - Total profits: {} SOL", profits);
        info!("  - Withdrawal amount: {} SOL", amount);
        
        // Proceed with withdrawal
        match crate::kraken::withdrawl::withdraw_assets(amount).await {
            Ok(response) => {
                info!("SOL withdrawal successful. Response: {:?}", response);
                
                // Extract and log the reference ID if available
                if let Some(refid) = response.get("refid") {
                    info!("Withdrawal reference ID: {}", refid);
                    
                    // Record the withdrawal in MongoDB
                    if let Err(e) = mongo_client.record_withdrawal(
                        user_id,
                        amount,
                        refid.as_str().unwrap_or("unknown"),
                        profits - amount
                    ).await {
                        error!("Failed to record withdrawal in MongoDB: {:?}", e);
                        // Note: We don't return an error here since the withdrawal was successful
                    }
                }
                
                Ok(response)
            },
            Err(e) => {
                error!("SOL withdrawal failed: {:?}", e);
                Err(e)
            }
        }
    }

    /// Gets the current SOL balance from the account
    async fn get_sol_balance(&self) -> Result<f64, AppError> {
        let mut ws = self.private_ws.lock().await;
        
        // Subscribe to balance updates
        let balance_request = serde_json::json!({
            "method": "subscribe",
            "params": {
                "channel": "balances"
            }
        });

        ws.send(Message::Text(balance_request.to_string().into()))
            .await
            .map_err(|e| {
                error!("Failed to request balance: {:?}", e);
                AppError::ConnectionError
            })?;

        // Wait for and process the balance response
        while let Some(msg) = ws.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    if text.contains("\"channel\":\"balances\"") {
                        let parsed: Value = serde_json::from_str(&text).map_err(|e| {
                            error!("Failed to parse balance response: {:?}", e);
                            AppError::ParseError
                        })?;

                        // Extract SOL balance from the response
                        if let Some(balances) = parsed.get("data") {
                            if let Some(sol_balance) = balances.get("SOL") {
                                if let Some(balance_str) = sol_balance.as_str() {
                                    match balance_str.parse::<f64>() {
                                        Ok(balance) => {
                                            debug!("Current SOL balance: {}", balance);
                                            return Ok(balance);
                                        }
                                        Err(e) => {
                                            error!("Failed to parse SOL balance '{}': {:?}", balance_str, e);
                                            return Err(AppError::ParseError);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Ok(Message::Close(frame)) => {
                    error!("WebSocket closed while fetching balance: {:?}", frame);
                    return Err(AppError::ConnectionError);
                }
                Err(e) => {
                    error!("WebSocket error while fetching balance: {:?}", e);
                    return Err(AppError::ConnectionError);
                }
                _ => continue,
            }
        }

        error!("Failed to receive balance information");
        Err(AppError::ConnectionError)
    }

    pub async fn subscribe_to_order_book(&mut self, symbol: &str, depth: i32) -> Result<(), AppError> {
        let subscription = serde_json::json!({
            "method": "subscribe",
            "params": {
                "channel": "level3",
                "symbol": [symbol],
                "depth": depth,
                "snapshot": true,
                "token": websocket_auth::get_ws_token(&self.api_key, &self.api_secret).await?,
            }
        });

        self.send_with_retry(&subscription.to_string()).await?;
        
        // Initialize analyzer for this symbol
        self.order_book_analyzers.insert(
            symbol.to_string(),
            Arc::new(Mutex::new(OrderBookAnalyzer::new(symbol, self.volume_threshold)))
        );

        Ok(())
    }

    pub async fn process_order_book_update(&self, message: &str) -> Result<Option<Signal>, AppError> {
        let update: Value = serde_json::from_str(message).map_err(|_| AppError::ParseError)?;
        
        if let Some(symbol) = update["data"][0]["symbol"].as_str() {
            if let Some(analyzer) = self.order_book_analyzers.get(symbol) {
                let analyzer = analyzer.lock().await;
                // Process bid updates
                if let Some(bids) = update["data"][0]["bids"].as_array() {
                    for bid in bids {
                        if let Ok(event) = serde_json::from_value(bid.clone()) {
                            if let Some(signal) = analyzer.process_order_event("bids", &event) {
                                return Ok(Some(signal));
                            }
                        }
                    }
                }

                // Process ask updates
                if let Some(asks) = update["data"][0]["asks"].as_array() {
                    for ask in asks {
                        if let Ok(event) = serde_json::from_value(ask.clone()) {
                            if let Some(signal) = analyzer.process_order_event("asks", &event) {
                                return Ok(Some(signal));
                            }
                        }
                    }
                }
            }
        }

        Ok(None)
    }

    pub async fn get_next_order_book_update(&self) -> Result<Option<Signal>, AppError> {
        let mut ws = self.public_ws.lock().await;
        if let Some(msg) = ws.next().await {
            let msg = msg.map_err(|e| {
                error!("WebSocket error: {:?}", e);
                AppError::ConnectionError
            })?;
            if let Message::Text(text) = msg {
                return self.process_order_book_update(&text).await;
            }
        }
        Ok(None)
    }
}

// Add this new enum to handle different price parameters
#[derive(Debug)]
#[allow(dead_code)]
pub enum OrderPriceParams {
    Market,
    Limit {
        price: f64,
    },
    StopLoss {
        stop_price: f64,
    },
    StopLossLimit {
        stop_price: f64,
        limit_price: f64,
    },
    TakeProfit {
        take_profit_price: f64,
    },
    TakeProfitLimit {
        take_profit_price: f64,
        limit_price: f64,
    },
    TrailingStop {
        trailing_distance: f64,
    },
    TrailingStopLimit {
        trailing_distance: f64,
        limit_offset: f64,
    },
    Iceberg {
        display_volume: f64,
        limit_price: f64,
    },
}
