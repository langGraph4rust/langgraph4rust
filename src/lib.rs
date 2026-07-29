//! # langgraph4rust
//!
//! A Rust implementation of a workflow engine inspired by Python's LangGraph library.
//! This crate provides a flexible and powerful way to build, execute, and manage
//! stateful workflow graphs with support for parallel execution and conditional routing.
//!
//! ## Features
//!
//! - **Declarative Graph Building**: Define workflows using a builder pattern
//! - **Parallel Execution**: Multiple nodes can execute simultaneously when possible
//! - **Conditional Routing**: Dynamic path selection based on state conditions
//! - **State Management**: Built-in JSON-based state persistence with type safety
//! - **Extensible Architecture**: Custom node implementations via traits
//! - **Validation**: Comprehensive graph validation before execution
//!
//! ## Quick Start
//!
//! ```rust
//! use langgraph4rust::*;
//! use std::collections::HashSet;
//! use std::sync::Arc;
//! use async_trait::async_trait;
//!
//! // Define a custom node
//! #[derive(Clone)]
//! struct GreetingNode;
//!
//! #[async_trait]
//! impl AgentNode<DefaultMemoryState> for GreetingNode {
//!     async fn apply(&self, state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
//!         state.set("message", "Hello from langgraph4rust!").await?;
//!         Ok(())
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), LangGraphError> {
//!     let mut builder = StateGraphBuilder::new();
//!
//!     builder.add_node("greet", Box::new(GreetingNode));
//!     builder.add_edge(START_NODE, HashSet::from(["greet".to_string()]));
//!     builder.add_edge("greet", HashSet::from([END_NODE.to_string()]));
//!
//!     let graph = builder.compile()?;
//!     let state = Arc::new(DefaultMemoryState::new());
//!
//!     graph.invoke(state).await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Core Concepts
//!
//! ### Nodes
//! Nodes are the basic building blocks of your workflow. Each node implements the
//! [`AgentNode`] trait and contains the logic to process and modify the state.
//!
//! ### Edges
//! Edges define the flow between nodes. There are two types:
//! - **Static edges**: Always connect to the same target nodes
//! - **Conditional edges**: Dynamically choose targets based on current state
//!
//! ### State
//! The state is shared across all nodes and persists throughout the workflow execution.
//! By default, [`DefaultMemoryState`] provides JSON-based storage.
//!
//! ## Examples
//!
//! See the `examples/` directory for complete working examples including:
//! - Simple linear workflows
//! - Parallel execution patterns
//! - Conditional routing
//! - Custom state implementations
//!
//! ## License
//!
//! Apache License 2.0

pub mod core;

pub use async_trait::async_trait;
pub use core::agent_node::AgentNode;
pub use core::agent_state::{AgentState, DefaultMemoryState};
pub use core::error::LangGraphError;
pub use core::state_graph::StateGraph;
pub use core::state_graph_builder::{END_NODE, START_NODE, StateGraphBuilder};
