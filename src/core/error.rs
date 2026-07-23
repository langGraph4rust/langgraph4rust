use std::error::Error;
use std::fmt;
use std::fmt::Display;

#[derive(Debug)]
pub enum LangGraphError {
    NodeError(String),
    StateError(String),
    GraphError(String),
    NotFound(String),
    Timeout(String),
    RetryExhausted(String),
}

impl Display for LangGraphError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LangGraphError::NodeError(msg) => write!(f, "Node error: {}", msg),
            LangGraphError::StateError(msg) => write!(f, "State error: {}", msg),
            LangGraphError::GraphError(msg) => write!(f, "Graph error: {}", msg),
            LangGraphError::NotFound(msg) => write!(f, "Not found: {}", msg),
            LangGraphError::Timeout(msg) => write!(f, "Timeout: {}", msg),
            LangGraphError::RetryExhausted(msg) => write!(f, "Retry exhausted: {}", msg),
        }
    }
}

impl Error for LangGraphError {}

impl From<String> for LangGraphError {
    fn from(msg: String) -> Self {
        LangGraphError::NodeError(msg)
    }
}

impl From<&str> for LangGraphError {
    fn from(msg: &str) -> Self {
        LangGraphError::NodeError(msg.to_string())
    }
}