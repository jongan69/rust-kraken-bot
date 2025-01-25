use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use log::{debug, info};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBookEntry {
    pub order_id: String,
    pub limit_price: f64,
    pub order_qty: f64,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug)]
pub struct OrderBookAnalyzer {
    symbol: String,
    bids: Arc<Mutex<HashMap<String, OrderBookEntry>>>,
    asks: Arc<Mutex<HashMap<String, OrderBookEntry>>>,
    volume_threshold: f64,
}

impl OrderBookAnalyzer {
    pub fn new(symbol: &str, volume_threshold: f64) -> Self {
        Self {
            symbol: symbol.to_string(),
            bids: Arc::new(Mutex::new(HashMap::new())),
            asks: Arc::new(Mutex::new(HashMap::new())),
            volume_threshold,
        }
    }

    pub fn process_order_event(&self, side: &str, event: &OrderEvent) -> Option<Signal> {
        debug!("Processing {} order event for symbol {}", side, self.symbol);
        let orders = if side == "bids" { &self.bids } else { &self.asks };
        let mut orders = orders.lock().unwrap();
        
        match event.event.as_str() {
            "add" => {
                orders.insert(event.order_id.clone(), OrderBookEntry {
                    order_id: event.order_id.clone(),
                    limit_price: event.limit_price,
                    order_qty: event.order_qty,
                    timestamp: event.timestamp,
                });
            },
            "modify" => {
                if let Some(order) = orders.get_mut(&event.order_id) {
                    order.order_qty = event.order_qty;
                }
            },
            "delete" => {
                orders.remove(&event.order_id);
            },
            _ => return None,
        }

        self.analyze_volume_patterns()
    }

    fn analyze_volume_patterns(&self) -> Option<Signal> {
        // Calculate total volume at each price level
        let mut bid_volumes: HashMap<i64, f64> = HashMap::new();
        let mut ask_volumes: HashMap<i64, f64> = HashMap::new();

        for order in self.bids.lock().unwrap().values() {
            let price_key = (order.limit_price * 1000.0) as i64;  // Convert to fixed decimal
            *bid_volumes.entry(price_key).or_default() += order.order_qty;
        }

        for order in self.asks.lock().unwrap().values() {
            let price_key = (order.limit_price * 1000.0) as i64;  // Convert to fixed decimal
            *ask_volumes.entry(price_key).or_default() += order.order_qty;
        }

        // Look for volume imbalances
        let total_bid_volume: f64 = bid_volumes.values().sum();
        let total_ask_volume: f64 = ask_volumes.values().sum();
        
        // Calculate volume weighted average prices (VWAP)
        let bid_vwap = self.calculate_vwap(&bid_volumes);
        let ask_vwap = self.calculate_vwap(&ask_volumes);

        // Check for significant volume imbalances
        if total_bid_volume > self.volume_threshold * total_ask_volume {
            info!(
                "Detected strong buying pressure - Bid/Ask ratio: {:.2}",
                total_bid_volume / total_ask_volume
            );
            return Some(Signal::Long {
                strength: total_bid_volume / total_ask_volume,
                price: bid_vwap,
            });
        } else if total_ask_volume > self.volume_threshold * total_bid_volume {
            info!(
                "Detected strong selling pressure - Ask/Bid ratio: {:.2}",
                total_ask_volume / total_bid_volume
            );
            return Some(Signal::Short {
                strength: total_ask_volume / total_bid_volume,
                price: ask_vwap,
            });
        }

        None
    }

    fn calculate_vwap(&self, volumes: &HashMap<i64, f64>) -> f64 {
        let mut volume_price_sum = 0.0;
        let mut total_volume = 0.0;

        for (price_key, volume) in volumes {
            let price = *price_key as f64 / 1000.0;  // Convert back to f64
            volume_price_sum += price * volume;
            total_volume += volume;
        }

        if total_volume > 0.0 {
            volume_price_sum / total_volume
        } else {
            0.0
        }
    }
}

#[derive(Debug)]
pub enum Signal {
    Long {
        strength: f64,
        price: f64,
    },
    Short {
        strength: f64,
        price: f64,
    },
}

#[derive(Debug, Deserialize)]
pub struct OrderEvent {
    pub event: String,
    pub order_id: String,
    pub limit_price: f64,
    pub order_qty: f64,
    pub timestamp: DateTime<Utc>,
} 