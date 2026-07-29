//! # Hello World Example
//!
//! This is the simplest possible langgraph4rust workflow.
//! It demonstrates the basic concepts: creating a node, building a graph, and executing it.
//!
//! ## Run this example:
//!
//! ```bash
//! cargo run --example hello_world
//! ```

use async_trait::async_trait;
use langgraph4rust::*;
use std::collections::HashSet;
use std::sync::Arc;

/// A simple node that prints a greeting message and stores it in state
#[derive(Clone)]
struct GreetingNode;

#[async_trait]
impl AgentNode<DefaultMemoryState> for GreetingNode {
    async fn apply(&self, state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
        let message = "Hello from langgraph4rust! 🚀";
        println!("{}", message);
        state.set("greeting", message).await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), LangGraphError> {
    println!("=== Hello World Example ===\n");

    // Step 1: Create a graph builder
    let mut builder = StateGraphBuilder::new();

    // Step 2: Add nodes to the graph
    builder.add_node("greet", Box::new(GreetingNode));

    // Step 3: Define edges (connections between nodes)
    // Connect START_NODE to our node, then to END_NODE
    builder.add_edge(START_NODE, HashSet::from(["greet".to_string()]));
    builder.add_edge("greet", HashSet::from([END_NODE.to_string()]));

    // Step 4: Compile the graph (validates structure)
    println!("Building workflow...");
    let graph = builder.compile()?;

    // Step 5: Execute with initial state
    println!("Executing workflow...\n");
    let state = Arc::new(DefaultMemoryState::new());
    graph.invoke(state.clone()).await?;

    // Step 6: Check results
    if let Some(greeting) = state.get::<String>("greeting").await? {
        println!("\n✅ Success! State contains: '{}'", greeting);
    }

    println!("\n=== Example Complete ===");
    Ok(())
}
