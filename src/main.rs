// main.rs
mod kraken;
mod error_handling;
pub mod mongo;

use error_handling::AppError;
use chrono::{DateTime, Local, NaiveDate, NaiveDateTime, Utc};
use kraken::client::{Candle, OrderSide, OrderType, OrderPriceParams};
use crate::kraken::client::{KrakenClient, OrderResponse};
use std::{collections::HashMap, sync::Arc};
use tokio::time::{self, Duration};
use tokio::sync::Mutex;
use log::{debug, error, info};
use kraken::orderbook::Signal;

// Add these constants at the top with other constants
const WITHDRAWAL_CHECK_INTERVAL: u64 = 3600; // Check for withdrawal every hour
const MIN_PROFIT_THRESHOLD: f64 = 1.0; // Minimum SOL profit to trigger withdrawal

#[tokio::main]
async fn main() -> Result<(), AppError> {
    // Initialize logging
    env_logger::init();
    
    // Initialize environment variables
    dotenv::dotenv().ok();
    
    info!("Starting trading bot...");

    // Define trading parameters
    let trading_pairs = vec!["BTC/USD", "SOL/USD", "LOCKIN/USD"];
    let max_trades_per_day = 2;
    let mut trades_today: HashMap<String, usize> = HashMap::new();
    let mut last_trade_day: Option<NaiveDate> = None;

    // Track ongoing trades for each pair
    let trade_tracker: Arc<Mutex<HashMap<String, Vec<OrderResponse>>>> = Arc::new(Mutex::new(HashMap::new()));

    println!("Trading bot started for pairs: {:?}", trading_pairs);

    let client = KrakenClient::new().await?;
    let client_arc = Arc::new(Mutex::new(client));
    
    // Create a clone for the withdrawal task
    let withdrawal_client = client_arc.clone();
    
    // Spawn withdrawal check task
    let _withdrawal_task = tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(WITHDRAWAL_CHECK_INTERVAL));
        loop {
            interval.tick().await;
            if let Err(e) = check_and_withdraw_profits(&withdrawal_client.lock().await, 1).await {
                error!("Withdrawal check failed: {:?}", e);
            }
        }
    });

    // Subscribe to all pairs at startup
    for pair in &trading_pairs {
        client_arc.lock().await.subscribe_to_ticker(pair).await?;
    }

    // Start the trading loop
    let mut interval = time::interval(Duration::from_secs(300)); // 5 minutes
    loop {
        interval.tick().await;

        // Get the current time and date
        let now: DateTime<Utc> = Utc::now();
        let local_now: DateTime<Local> = now.into();

        // Check if a new day has started
        let current_date = local_now.naive_local().date();
        if last_trade_day.is_none() || current_date > last_trade_day.unwrap() {
            last_trade_day = Some(current_date);
            trades_today.clear();
            println!("New trading day started: {}", current_date);
        }

        // Iterate over all trading pairs
        for pair in &trading_pairs {
            let trades_count = trades_today.entry(pair.to_string()).or_insert(0);

            // Stop trading if the maximum trades have been reached for this pair
            if *trades_count >= max_trades_per_day {
                println!("Maximum trades reached for {} today. Skipping...", pair);
                continue;
            }

            // Fetch the first 5-minute candle after daily open
            let daily_open_time = NaiveDateTime::new(
                current_date, 
                chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap()
            );
            let first_candle = match get_first_candle(pair, daily_open_time).await {
                Ok(c) => c,
                Err(e) => {
                    println!("Error fetching first candle for {}: {}", pair, e);
                    continue;
                }
            };
            let open_price = first_candle.open;
            let high_price = first_candle.high;
            let low_price = first_candle.low;

            println!(
                "{} - First 5-minute candle - Open: {}, High: {}, Low: {}",
                pair, open_price, high_price, low_price
            );

            // Check if the price has moved above/below the opening range
            let current_price = match get_current_price(&*client_arc.lock().await, pair).await {
                Ok(price) => price,
                Err(e) => {
                    println!("Error fetching current price for {}: {}", pair, e);
                    continue;
                }
            };

            if current_price > high_price {
                // Place a long limit order just below the current price
                let volume = get_trade_volume(pair); // Adjust volume based on your risk
                let limit_price = current_price - 0.01 * current_price; // 1% below current price
                let stop_loss = low_price;

                match place_limit_order(pair, "buy", volume, limit_price, stop_loss).await {
                    Ok(order) => {
                        // println!("Placed a long order for {}: {:?}", pair, order);
                        *trades_count += 1;

                        // Track the trade
                        let mut tracker = trade_tracker.lock().await;
                        tracker.entry(pair.to_string()).or_default().push(order);
                    }
                    Err(e) => println!("Failed to place long order for {}: {}", pair, e),
                }
            } else if current_price < low_price {
                // Place a short limit order just above the current price
                let volume = get_trade_volume(pair); // Adjust volume based on your risk
                let limit_price = current_price + 0.01 * current_price; // 1% above current price
                let stop_loss = high_price;

                match place_limit_order(pair, "sell", volume, limit_price, stop_loss).await {
                    Ok(order) => {
                        // println!("Placed a short order for {}: {:?}", pair, order);
                        *trades_count += 1;

                        // Track the trade
                        let mut tracker = trade_tracker.lock().await;
                        tracker.entry(pair.to_string()).or_default().push(order);
                    }
                    Err(e) => println!("Failed to place short order for {}: {}", pair, e),
                }
            } else {
                println!("No significant movement detected for {}. Skipping this interval.", pair);
            }

            // Subscribe to order book data
            client_arc.lock().await.subscribe_to_order_book(pair, 100).await?;

            // Process order book updates
            if let Some(signal) = client_arc.lock().await.get_next_order_book_update().await? {
                match signal {
                    Signal::Long { strength, price } => {
                        if strength > 2.0 {  // Significant buying pressure
                            // Place a buy order
                            let volume = get_trade_volume(pair);
                            let limit_price = price * 0.999; // Slightly below VWAP
                            place_limit_order(pair, "buy", volume, limit_price, price * 0.995).await?;
                        }
                    },
                    Signal::Short { strength, price } => {
                        if strength > 2.0 {  // Significant selling pressure
                            // Place a sell order
                            let volume = get_trade_volume(pair);
                            let limit_price = price * 1.001; // Slightly above VWAP
                            place_limit_order(pair, "sell", volume, limit_price, price * 1.005).await?;
                        }
                    }
                }
            }
        }
    }
}

// Helper function to get the first 5-minute candle after the daily open
async fn get_first_candle(pair: &str, daily_open_time: NaiveDateTime) -> Result<Candle, AppError> {
    debug!("Fetching first candle for {} after {}", pair, daily_open_time);
    let client = KrakenClient::new().await?;
    let candles = client.get_ohlc_data(pair, 5).await?;
    
    debug!("Received {} candles for {}", candles.len(), pair);
    for candle in candles {
        if candle.timestamp >= daily_open_time.and_utc().timestamp() {
            debug!("Found first candle for {} at timestamp {}", pair, candle.timestamp);
            return Ok(candle);
        }
    }
    
    error!("No suitable candle found for {} after {}", pair, daily_open_time);
    Err(AppError::InternalServerError)
}

// Helper function to place a limit order
async fn place_limit_order(
    pair: &str,
    side: &str,
    volume: f64,
    limit_price: f64,
    stop_loss: f64,
) -> Result<OrderResponse, AppError> {
    let client = KrakenClient::new().await?;
    let order_side = match side {
        "buy" => OrderSide::Buy,
        "sell" => OrderSide::Sell,
        _ => return Err(AppError::InvalidInput),
    };

    let price_params = OrderPriceParams::StopLossLimit {
        stop_price: stop_loss,
        limit_price,
    };

    let order = client
        .add_order(pair, order_side, OrderType::Limit, volume, price_params, 1)
        .await?;
    Ok(order)
}

// Helper function to get trade volume based on the pair
fn get_trade_volume(pair: &str) -> f64 {
    match pair {
        "BTC/USD" => 0.01,  // Adjust for BTC
        "SOL/USD" => 1.0,   // Adjust for SOL
        "LOCKIN/USD" => 100.0, // Adjust for LOCKIN
        _ => 0.0, // Default volume
    }
}

// Update the get_current_price function
async fn get_current_price(client: &KrakenClient, pair: &str) -> Result<f64, AppError> {
    client.get_current_price(pair).await
}

// Add this function after the other helper functions
async fn check_and_withdraw_profits(client: &tokio::sync::MutexGuard<'_, KrakenClient>, user_id: i64) -> Result<(), AppError> {
    info!("Checking profits for potential withdrawal...");
    
    // Try to withdraw all profits above the minimum threshold
    match client.withdraw_sol(MIN_PROFIT_THRESHOLD, user_id).await {
        Ok(response) => {
            info!("Successfully withdrew profits: {:?}", response);
            Ok(())
        }
        Err(AppError::InsufficientFunds) => {
            debug!("Insufficient profits for withdrawal (below threshold)");
            Ok(()) // Not an error condition
        }
        Err(e) => {
            error!("Error during withdrawal check: {:?}", e);
            Err(e)
        }
    }
}
