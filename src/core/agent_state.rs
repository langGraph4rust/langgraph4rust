use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{from_value, to_value, Value};

#[async_trait::async_trait]
pub trait AgentState {
    async fn get<T: DeserializeOwned + Send + Sync>(&self, key: &str) -> Result<Option<T>, Error>;
    async fn set<T: Serialize + Send + Sync>(&self, key: &str, value: T) -> Result<bool, Error>;
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
    async fn get<T: DeserializeOwned + Send + Sync>(&self, key: &str) -> Result<Option<T>, Error> {
        let memory = self.memory.read().await;
        
        match memory.get(key) {
            Some(value) => {
                let result = from_value(value.clone())
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    async fn set<T: Serialize + Send + Sync>(&self, key: &str, value: T) -> Result<bool, Error> {
        let json_value = to_value(value)
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
        
        let mut memory = self.memory.write().await;
        memory.insert(key.to_string(), json_value);
        Ok(true)
    }
}