use std::{io, result};

use thiserror::Error;

/// Result type for operations that could result in an `ArtilleryError`
pub type Result<T> = result::Result<T, ArtilleryError>;

#[derive(Error, Debug)]
pub enum ArtilleryError {
    // General Error Types
    #[error("Artillery :: I/O error occurred: {}", _0)]
    Io(#[from] io::Error),
    #[error("Artillery :: Cluster Message Decode Error: {}", _0)]
    ClusterMessageDecode(String),
    #[error("Artillery :: Message Send Error: {}", _0)]
    Send(String),
    #[error("Artillery :: Message Receive Error: {}", _0)]
    Receive(String),
    #[error("Artillery :: Unexpected Error: {}", _0)]
    Unexpected(String),
    #[error("Artillery :: Decoding Error: {}", _0)]
    Decoding(String),
    #[error("Artillery :: Numeric Cast Error: {}", _0)]
    NumericCast(String),
}

impl From<serde_json::error::Error> for ArtilleryError {
    fn from(e: serde_json::error::Error) -> Self {
        ArtilleryError::ClusterMessageDecode(e.to_string())
    }
}

impl<T> From<flume::SendError<T>> for ArtilleryError {
    fn from(e: flume::SendError<T>) -> Self {
        ArtilleryError::Send(e.to_string())
    }
}

impl From<std::num::TryFromIntError> for ArtilleryError {
    fn from(e: std::num::TryFromIntError) -> Self {
        ArtilleryError::NumericCast(e.to_string())
    }
}
