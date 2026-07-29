use crate::core::agent_state::AgentState;
use crate::core::error::LangGraphError;
use async_trait::async_trait;
use std::sync::Arc;

/// Trait for defining executable nodes in a workflow graph.
///
/// Implement this trait to create custom nodes that can be added to a [`StateGraphBuilder`].
/// Each node represents a discrete unit of work in your workflow and has access to
/// the shared state.
///
/// # Type Parameters
///
/// - `S`: The state type that this node operates on. Must implement [`AgentState`].
///
/// # Requirements
///
/// - `S: AgentState`: The state must support get/set operations
/// - `S: Send`: State must be safe to transfer between threads
/// - `S: Sync`: State must be safe to share between threads
///
/// # Example
///
/// ```rust
/// use langgraph4rust::*;
/// use std::sync::Arc;
///
/// #[derive(Clone)]
/// struct CounterNode;
///
/// #[async_trait]
/// impl AgentNode<DefaultMemoryState> for CounterNode {
///     async fn apply(&self, state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
///         let count: i32 = state.get("count").await?.unwrap_or(0);
///         state.set("count", count + 1).await?;
///         Ok(())
///     }
/// }
/// ```
///
/// # Error Handling
///
/// Return a [`LangGraphError`] if the node encounters an error during execution.
/// This will propagate up and stop the graph execution.
///
/// # Concurrency Note
///
/// Nodes may be executed concurrently with other nodes (when they are at the same
/// level in the graph). Ensure that your implementation is thread-safe when accessing
/// shared resources beyond the provided state.
#[async_trait]
pub trait AgentNode<S: AgentState + Send + Sync> {
    /// Execute the node's logic against the provided state.
    ///
    /// This method is called when the node is activated during graph execution.
    /// Use the state parameter to read and modify the workflow's shared data.
    ///
    /// # Arguments
    ///
    /// * `state` - An `Arc` reference to the shared state. Use this to:
    ///   - Read existing values via `state.get("key").await?`
    ///   - Write new or updated values via `state.set("key", value).await?`
    ///
    /// # Returns
    ///
    /// - `Ok(())` on successful execution
    /// - `Err(LangGraphError)` if an error occurs, which will halt execution
    ///
    /// # Example
    ///
    /// ```rust
    /// # use langgraph4rust::*;
    /// # use std::sync::Arc;
    /// # #[derive(Clone)]
    /// # struct MyNode;
    /// # #[async_trait]
    /// # impl AgentNode<DefaultMemoryState> for MyNode {
    /// async fn apply(&self, state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
    ///     // Read current value
    ///     let value: Option<String> = state.get("my_key").await?;
    ///
    ///     // Process and update
    ///     let new_value = value.unwrap_or_default().to_uppercase();
    ///     state.set("my_key", new_value).await?;
    ///
    ///     Ok(())
    /// }
    /// # }
    /// ```
    async fn apply(&self, state: Arc<S>) -> Result<(), LangGraphError>;
}
