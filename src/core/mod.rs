//! Core engine internals of the `langgraph4rust` workflow engine.
//!
//! This module groups the building blocks that power the public API. Most of
//! these items are re-exported at the crate root, so users normally interact
//! with them through `langgraph4rust::*` rather than this module directly.
//!
//! # Submodules
//!
//! | Module | Responsibility |
//! |--------|----------------|
//! | [`agent_node`](crate::AgentNode) | The [`AgentNode`](crate::AgentNode) trait — a unit of work |
//! | [`agent_state`](crate::AgentState) | The [`AgentState`](crate::AgentState) trait and [`DefaultMemoryState`](crate::DefaultMemoryState) |
//! | [`state_graph_builder`](crate::StateGraphBuilder) | Declarative, validated graph construction |
//! | [`state_graph`](crate::StateGraph) | Compiled, immutable graph and batch execution ([`invoke`](crate::StateGraph::invoke)) |
//! | [`state_graph_stream`](crate::StreamEvent) | Push-based streaming execution ([`stream`](crate::StateGraph::stream)) |
//! | `graph_validator` | Compile-time structural validation (internal) |
//! | [`error`](crate::LangGraphError) | The [`LangGraphError`] type |
//!
//! # Execution flow
//!
//! ```text
//! StateGraphBuilder ──compile()──> GraphValidator ──> StateGraph ──invoke()/stream()──> results
//! ```

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
