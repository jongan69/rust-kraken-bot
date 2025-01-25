use dotenv::dotenv;
use serde_json::Value;
use crate::error_handling::AppError;
use kraken_rest_client::Client;
use log::{info, error};
use crate::kraken::utils::get_nonce;

pub async fn withdraw_assets(
    amount: f64,
) -> Result<Value, AppError> {
    info!("=== Starting SOL withdrawal process ===");
    dotenv().ok(); // Load environment variables from the ".env" file

    // Read Kraken API key and secret stored in environment variables
    let api_key = std::env::var("KRAKEN_API_KEY")?;
    let api_secret = std::env::var("KRAKEN_API_SECRET")?;
    let address_key = std::env::var("KRAKEN_SOL_ADDRESS_KEY")?;
    let address = std::env::var("KRAKEN_SOL_ADDRESS")?;

    info!("Initiating withdrawal of {} SOL", amount);
    
    // Create the client
    let client = Client::new(api_key, api_secret);

    // Construct the request payload
    let payload = serde_json::json!({
        "nonce": get_nonce(),
        "asset": "SOL",  // SOL asset ticker on Kraken
        "key": address_key, // Name of SOL wallet in Kraken
        "address": address, // SOL wallet address
        "amount": amount // Amount of SOL to withdraw
    });

    info!("Sending withdrawal request to Kraken");
    
    // Send the withdrawal request
    let response: Value = client
        .send_private_json("/0/private/Withdraw", payload)
        .await
        .map_err(|_e| AppError::ExchangeError)?;

    match response.get("error") {
        Some(errors) if !errors.as_array().unwrap_or(&vec![]).is_empty() => {
            error!("Withdrawal error: {:?}", errors);
            Err(AppError::ExchangeError)
        },
        _ => {
            info!("Withdrawal request successful: {:?}", response);
            Ok(response)
        }
    }
}