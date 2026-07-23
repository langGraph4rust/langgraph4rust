use std::collections::HashMap;
use std::io::Error;
use serde_json::Value;
use crate::core::agent_state::AgentState;

pub trait AgentNode<S: AgentState> {
    fn apply(&self, state: &mut S) -> Result<HashMap<String, Value>, Error>;
}
