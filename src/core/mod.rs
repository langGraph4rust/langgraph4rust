pub(crate) mod agent_node;
pub(crate) mod agent_state;
pub(crate) mod error;
pub(crate) mod graph_validator;
pub mod state_graph;
pub(crate) mod state_graph_builder;
pub mod state_graph_stream;

pub use error::LangGraphError;

/// Type alias for conditional edge router functions.
pub(crate) type RouterFn<S> = Box<dyn Fn(&S) -> String + Send + Sync>;
