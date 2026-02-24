use thiserror::Error;

/// Unified error type for the state-sync engine.
///
/// We use `thiserror` so each variant carries a meaningful message at the
/// call site rather than requiring callers to wrap errors manually.
#[derive(Error, Debug)]
pub enum EngineError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Gossip peer error: {0}")]
    Peer(String),

    #[error("RPC method not found: {0}")]
    MethodNotFound(String),

    #[error("Invalid RPC params: {0}")]
    InvalidParams(String),

    #[error("Internal engine error: {0}")]
    Internal(String),
}

// Manual impl so we can convert bincode::Error (not a std::error::Error impl
// in all versions) without pulling in the full From chain.
impl From<Box<bincode::ErrorKind>> for EngineError {
    fn from(e: Box<bincode::ErrorKind>) -> Self {
        EngineError::Serialization(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, EngineError>;
