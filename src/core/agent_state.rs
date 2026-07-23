use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{from_value, to_value, Value};
use crate::core::error::LangGraphError;

#[async_trait::async_trait]
pub trait AgentState {
    async fn get<T: DeserializeOwned + Send + Sync>(&self, key: &str) -> Result<Option<T>, LangGraphError>;
    async fn set<T: Serialize + Send + Sync>(&self, key: &str, value: T) -> Result<bool, LangGraphError>;
}

pub struct DefaultMemoryState {
    memory: Arc<RwLock<HashMap<String, Value>>>,
}

impl DefaultMemoryState {
    pub fn new() -> Self {
        DefaultMemoryState {
            memory: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for DefaultMemoryState {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl AgentState for DefaultMemoryState {
    async fn get<T: DeserializeOwned + Send + Sync>(&self, key: &str) -> Result<Option<T>, LangGraphError> {
        let memory = self.memory.read().await;
        
        match memory.get(key) {
            Some(value) => {
                let result = from_value(value.clone())
                    .map_err(|e| LangGraphError::StateError(format!("Deserialization error: {}", e)))?;
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    async fn set<T: Serialize + Send + Sync>(&self, key: &str, value: T) -> Result<bool, LangGraphError> {
        let json_value = to_value(value)
            .map_err(|e| LangGraphError::StateError(format!("Serialization error: {}", e)))?;
        
        let mut memory = self.memory.write().await;
        memory.insert(key.to_string(), json_value);
        Ok(true)
    }
}