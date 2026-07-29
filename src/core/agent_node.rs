use std::sync::Arc;
use crate::core::agent_state::AgentState;
use crate::core::error::LangGraphError;
use async_trait::async_trait;

#[async_trait]
pub trait AgentNode<S: AgentState + Send + Sync> {
    async fn apply(&self, state: Arc<S>) -> Result<(), LangGraphError>;
}