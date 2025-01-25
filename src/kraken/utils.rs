use std::time::{SystemTime, UNIX_EPOCH};

// Function to get the current nonce
pub fn get_nonce() -> String {
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    let in_ms = since_the_epoch.as_millis();
    in_ms.to_string()
}