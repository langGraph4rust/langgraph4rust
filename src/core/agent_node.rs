use std::collections::HashMap;
use std::error::Error;
use serde_json::Value;
use crate::core::agent_state::AgentState;
use crate::core::error::LangGraphError;

pub trait AgentNode<S: AgentState> {
    fn apply(&self, state: &mut S) -> Result<HashMap<String, Value>, LangGraphError>;
    
    fn fallback(&self, state: &mut S, error: &(dyn Error)) -> Result<HashMap<String, Value>, LangGraphError> {
        Err(LangGraphError::NodeError(error.to_string().into()))
    }
}