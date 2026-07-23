use std::io::Error;
use crate::core::agent_state::AgentState;

pub trait AgentNode<S: AgentState> {
    fn apply(&self, state: &mut S) -> Result<(), Error>;
}
