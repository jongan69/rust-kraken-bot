// mongo.rs
use mongodb::{
    bson::{doc, DateTime as BsonDateTime},
    Client, Collection, Database,
};
use serde::{Deserialize, Serialize};
use crate::error_handling::AppError;
use mongodb::bson::oid::ObjectId;
use chrono::Utc;
use log::{error, info};
use futures_util::TryStreamExt;
use futures_util::StreamExt;


#[derive(Debug, Serialize, Deserialize)]
pub struct Transaction {
    pub user_id: i32,
    pub amount: f64,
    pub processed: bool,
    pub status: String, // New field for transaction status
    pub address: String,
    pub timestamp: BsonDateTime,
    // pub kraken_result: serde_json::Value,
    // pub kraken_error: serde_json::Value,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct User {
    #[serde(rename = "_id")]
    pub id: ObjectId,
    pub user_id: i64,
    pub username: Option<String>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub api_key: Option<String>,
    pub total_deposit: f64,
    pub lockin_total: f64,
    pub autobuy_amount: Option<f64>,
    pub solana_public_key: Option<String>,
    pub solana_private_key: Option<String>,
    pub bitcoin_public_key: Option<String>,
    pub bitcoin_private_key: Option<String>,
    pub bitcoin_mnemonic: Option<String>,
    pub ethereum_public_key: Option<String>,
    pub ethereum_private_key: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Trade {
    #[serde(rename = "_id")]
    pub id: ObjectId,
    pub user_id: i64,
    pub order_id: String,
    pub pair: String,
    pub side: String,
    pub price: f64,
    pub volume: f64,
    pub timestamp: BsonDateTime,
    pub status: String,
    pub fee: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Withdrawal {
    user_id: i64,
    amount: f64,
    reference_id: String,
    timestamp: chrono::DateTime<Utc>,
    remaining_profits: f64,
}

pub async fn get_database() -> Result<Database, AppError> {
    let url = std::env::var("MONGO_URL")?;
    let client = Client::with_uri_str(&url).await?;
    Ok(client.database("trading_bot"))
}

pub async fn get_trades_collection() -> Result<Collection<Trade>, AppError> {
    let db = get_database().await?;
    Ok(db.collection("trades"))
}

impl Trade {
    pub async fn get_user_trades(user_id: i64) -> Result<Vec<Trade>, AppError> {
        let collection = get_trades_collection().await?;
        let filter = doc! { "user_id": user_id };
        let mut cursor = collection.find(filter).await?;
        
        let mut trades = Vec::new();
        while let Some(trade) = cursor.try_next().await? {
            trades.push(trade);
        }
        
        Ok(trades)
    }

    pub async fn calculate_profit(user_id: i64, pair: &str) -> Result<f64, AppError> {
        let trades = Self::get_user_trades(user_id).await?;
        let pair_trades: Vec<_> = trades
            .into_iter()
            .filter(|t| t.pair == pair)
            .collect();

        let mut total_profit = 0.0;
        let mut buy_volume = 0.0;
        let mut buy_cost = 0.0;

        for trade in pair_trades {
            match trade.side.as_str() {
                "buy" => {
                    buy_volume += trade.volume;
                    buy_cost += trade.volume * trade.price;
                }
                "sell" => {
                    let sell_value = trade.volume * trade.price;
                    if buy_volume > 0.0 {
                        let avg_buy_price = buy_cost / buy_volume;
                        total_profit += sell_value - (trade.volume * avg_buy_price);
                        buy_volume -= trade.volume;
                        buy_cost = buy_volume * avg_buy_price;
                    }
                }
                _ => {}
            }
        }

        Ok(total_profit)
    }
}

pub struct MongoClient {
    db: Database,
}

impl MongoClient {
    pub async fn new() -> Result<Self, AppError> {
        let db = get_database().await?;
        Ok(Self { db })
    }

    /// Gets the total profits for a user from all their trades
    pub async fn get_total_profits(&self, user_id: i64) -> Result<f64, AppError> {
        info!("Fetching total profits from MongoDB for user {}", user_id);
        let trades: Collection<Trade> = self.db.collection("trades");
        
        let pipeline = vec![
            doc! {
                "$match": {
                    "user_id": user_id
                }
            },
            doc! {
                "$group": {
                    "_id": null,
                    "total_profits": {
                        "$sum": "$profit"
                    }
                }
            }
        ];

        let mut cursor = trades.aggregate(pipeline).await.map_err(|e| {
            error!("Failed to aggregate trades: {:?}", e);
            AppError::MongoError(e.to_string())
        })?;

        if let Some(result) = cursor.next().await {
            let result = result.map_err(|e| {
                error!("Failed to get next cursor result: {:?}", e);
                AppError::DatabaseError
            })?;
            
            let total_profits = result.get_f64("total_profits").unwrap_or(0.0);
            info!("Successfully retrieved profits from MongoDB: {}", total_profits);
            Ok(total_profits)
        } else {
            Ok(0.0) // No trades found
        }
    }

    /// Records a withdrawal in the database
    pub async fn record_withdrawal(
        &self,
        user_id: i64,
        amount: f64,
        reference_id: &str,
        remaining_profits: f64,
    ) -> Result<(), AppError> {
        info!("Recording withdrawal to MongoDB - Amount: {} SOL, RefID: {}", amount, reference_id);
        let withdrawals: Collection<Withdrawal> = self.db.collection("withdrawals");
        
        let withdrawal = Withdrawal {
            user_id,
            amount,
            reference_id: reference_id.to_string(),
            timestamp: Utc::now(),
            remaining_profits,
        };

        withdrawals.insert_one(withdrawal).await.map_err(|e| {
            error!("Failed to record withdrawal: {:?}", e);
            AppError::MongoError(e.to_string())
        })?;

        info!("Successfully recorded withdrawal in MongoDB");
        Ok(())
    }
}