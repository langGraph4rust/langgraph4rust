use std::collections::HashMap;
use serde_json::Value;
use crate::core::agent_state::AgentState;
use crate::core::error::LangGraphError;

pub trait AgentNode<S: AgentState> {
    fn apply(&self, state: &mut S) -> Result<HashMap<String, Value>, LangGraphError>;
    
    fn fallback(&self, _state: &mut S, error: &LangGraphError) -> Result<HashMap<String, Value>, LangGraphError> {
        Err(LangGraphError::NodeError(format!("Node failed and no fallback implemented: {}", error)))
    }
}