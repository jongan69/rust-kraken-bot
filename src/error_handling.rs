use std::fmt;

#[derive(Debug)]
pub enum AppError {
    ConnectionError,
    ParseError,
    InvalidInput,
    InternalServerError,
    InsufficientFunds,
    ExchangeError,
    MongoError(String),
    DatabaseError,
    // Add more error types as needed
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AppError::ConnectionError => write!(f, "Connection error"),
            AppError::ParseError => write!(f, "Parse error"),
            AppError::InvalidInput => write!(f, "Invalid input"),
            AppError::InternalServerError => write!(f, "Internal server error"),
            AppError::InsufficientFunds => write!(f, "Insufficient funds"),
            AppError::ExchangeError => write!(f, "Exchange error"),
            AppError::MongoError(e) => write!(f, "MongoDB error: {}", e),
            AppError::DatabaseError => write!(f, "Database error"),
        }
    }
}

impl From<serde_json::Error> for AppError {
    fn from(_error: serde_json::Error) -> Self {
        AppError::ParseError
    }
}

impl From<std::env::VarError> for AppError {
    fn from(_: std::env::VarError) -> Self {
        AppError::ConnectionError
    }
}

impl From<mongodb::error::Error> for AppError {
    fn from(error: mongodb::error::Error) -> Self {
        AppError::MongoError(error.to_string())
    }
} 