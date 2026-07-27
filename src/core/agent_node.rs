use std::error::Error;
use std::sync::Arc;
use tokio::sync::Mutex as TokioMutex;
use crate::core::agent_state::AgentState;
use crate::core::error::LangGraphError;
use async_trait::async_trait;


pub trait AgentNode<S: AgentState> {
    fn apply(&self, state: Arc<S>) -> Result<(), LangGraphError>;
    
    fn fallback(&self, state: Arc<S>, error: &(dyn Error)) -> Result<(), LangGraphError>;
}