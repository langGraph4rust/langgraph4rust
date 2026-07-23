use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{from_value, to_value, Value};

pub trait AgentState {
    fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, Error>;
    fn set<T: Serialize>(&mut self, key: &str, value: T) -> Result<bool, Error>;
}


pub struct DefaultMemoryState{
    memory: HashMap<String, Value>,
}

impl AgentState for DefaultMemoryState {
   fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, Error> {
        match self.memory.get(key) {
            Some(value) => {
                let result = from_value(value.clone())
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

  fn set<T: Serialize>(&mut self, key: &str, value: T) -> Result<bool, Error> {
        let json_value = to_value(value)
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
        self.memory.insert(key.to_string(), json_value);
        Ok(true)
    }
}