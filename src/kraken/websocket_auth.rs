use crate::error_handling::AppError;
use kraken_rest_client::Client;
use base64::Engine;
use serde_json::{json, Value};
use log::{error, info, debug};
use crate::kraken::utils::get_nonce;

pub async fn verify_api_permissions(api_key: &str, api_secret: &str) -> Result<(), AppError> {
    let client = Client::new(api_key, api_secret);
    let payload = json!({
        "nonce": get_nonce()
    });

    let response: Value = client
        .send_private_json("/0/private/GetWebSocketsToken", payload)
        .await
        .map_err(|_| AppError::ConnectionError)?;

    if response["error"].as_array().map_or(false, |e| !e.is_empty()) {
        error!("API permission error: {:?}", response);
        error!("API key used: {}...", &api_key[..10]);
        return Err(AppError::ConnectionError);
    }

    info!("API permissions verified successfully");
    Ok(())
}

pub fn validate_api_credentials(api_key: &str, api_secret: &str) -> Result<(), AppError> {
    // Check API key format
    if api_key.trim() != api_key {
        error!("API key contains whitespace");
        return Err(AppError::ConnectionError);
    }
    
    // Check API secret format
    if api_secret.trim() != api_secret {
        error!("API secret contains whitespace");
        return Err(AppError::ConnectionError);
    }

    // Verify base64 encoding of secret
    if let Err(e) = base64::engine::general_purpose::STANDARD.decode(api_secret) {
        error!("API secret is not valid base64: {:?}", e);
        return Err(AppError::ConnectionError);
    }

    Ok(())
}

pub async fn get_ws_token(api_key: &str, api_secret: &str) -> Result<String, AppError> {
    let client = Client::new(api_key, api_secret);
    let payload = json!({
        "nonce": get_nonce()
    });

    let response: serde_json::Value = match client
        .send_private_json("/0/private/GetWebSocketsToken", payload)
        .await {
            Ok(resp) => resp,
            Err(e) => {
                error!("Failed to get WebSocket token: {:?}", e);
                return Err(AppError::ConnectionError);
            }
        };

    debug!("WebSocket token response: {:?}", response);

    match response["token"].as_str() {
        Some(token) => {
            debug!("Successfully extracted token: {}", token);
            Ok(token.to_string())
        },
        None => {
            error!("Failed to extract token from response: {:?}", response);
            Err(AppError::ParseError)
        }
    }
}