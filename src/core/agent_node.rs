use std::sync::Arc;
use crate::core::agent_state::AgentState;
use crate::core::error::LangGraphError;


pub trait AgentNode<S: AgentState> {
    fn apply(&self, state: Arc<S>) -> Result<(), LangGraphError>;
    
}