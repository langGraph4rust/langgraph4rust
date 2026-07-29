//! # Error Handling Example
//!
//! Demonstrates how to handle errors in langgraph4rust workflows,
//! including node failures, state errors, and recovery strategies.
//!
//! ## Run this example:
//!
//! ```bash
//! cargo run --example error_handling
//! ```

use langgraph4rust::*;
use std::collections::HashSet;
use std::sync::Arc;

// Re-export async_trait for use in examples
use async_trait::async_trait;

// ============================================================================
// Node Definitions
// ============================================================================

/// A node that always succeeds
#[derive(Clone)]
struct SuccessNode;

#[async_trait]
impl AgentNode<DefaultMemoryState> for SuccessNode {
    async fn apply(&self, state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
        println!("✅ SuccessNode: Executing successfully");
        state.set("success_step", true).await?;
        Ok(())
    }
}

/// A node that always fails with a custom error message
#[derive(Clone)]
struct FailureNode {
    error_message: String,
}

impl FailureNode {
    fn new(msg: &str) -> Self {
        FailureNode {
            error_message: msg.to_string(),
        }
    }
}

#[async_trait]
impl AgentNode<DefaultMemoryState> for FailureNode {
    async fn apply(&self, _state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
        println!("❌ FailureNode: Intentionally failing!");
        Err(LangGraphError::NodeError(self.error_message.clone()))
    }
}

/// A node that demonstrates a state error (type mismatch)
#[derive(Clone)]
struct StateErrorNode;

#[async_trait]
impl AgentNode<DefaultMemoryState> for StateErrorNode {
    async fn apply(&self, state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
        println!("🔍 StateErrorNode: Attempting invalid type conversion...");

        // Store a string
        state.set("value", "hello").await?;

        // Try to read it as an integer (will fail)
        let _: i32 = state
            .get("value")
            .await?
            .ok_or_else(|| LangGraphError::StateError("Value not found".to_string()))?;

        Ok(())
    }
}

/// A node that simulates validation logic
#[derive(Clone)]
struct ValidatorNode;

#[async_trait]
impl AgentNode<DefaultMemoryState> for ValidatorNode {
    async fn apply(&self, state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
        println!("🔎 ValidatorNode: Checking preconditions...");

        let required: Option<String> = state.get("required_field").await?;

        match required {
            Some(value) => {
                println!("   ✅ Validation passed: found '{}'", value);
                Ok(())
            }
            None => {
                println!("   ❌ Validation failed: 'required_field' missing");
                Err(LangGraphError::NodeError(
                    "Validation failed: required field is missing".to_string(),
                ))
            }
        }
    }
}

/// Recovery/cleanup node that runs after errors
#[derive(Clone)]
struct CleanupNode;

#[async_trait]
impl AgentNode<DefaultMemoryState> for CleanupNode {
    async fn apply(&self, state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
        println!("🧹 CleanupNode: Performing cleanup...");
        state.set("cleaned_up", true).await?;
        println!("   ✅ Cleanup complete");
        Ok(())
    }
}

// ============================================================================
// Main Execution - Demonstrating Different Error Scenarios
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), LangGraphError> {
    println!("=== Error Handling Examples ===\n");

    scenario_1_node_failure().await?;
    scenario_2_state_error().await?;
    scenario_3_validation_error().await?;
    scenario_4_error_recovery_pattern().await?;

    println!("\n=== All Scenarios Complete ===");
    Ok(())
}

/// Scenario 1: Node that returns an error
async fn scenario_1_node_failure() -> Result<(), LangGraphError> {
    println!("--- Scenario 1: Node Failure ---");

    let mut builder = StateGraphBuilder::new();
    builder.add_node("step1", Box::new(SuccessNode));
    builder.add_node(
        "step2_fail",
        Box::new(FailureNode::new("Database connection failed")),
    );
    builder.add_node("step3", Box::new(SuccessNode));

    builder.add_edge(START_NODE, HashSet::from(["step1".to_string()]));
    builder.add_edge("step1", HashSet::from(["step2_fail".to_string()]));
    builder.add_edge("step2_fail", HashSet::from(["step3".to_string()]));
    builder.add_edge("step3", HashSet::from([END_NODE.to_string()]));

    let graph = builder.compile()?;
    let state = Arc::new(DefaultMemoryState::new());

    match graph.invoke(state).await {
        Ok(()) => println!("   ⚠️ Unexpected success"),
        Err(e) => println!("   📌 Caught error: {}\n", e),
    }

    Ok(())
}

/// Scenario 2: State operation error (type mismatch)
async fn scenario_2_state_error() -> Result<(), LangGraphError> {
    println!("--- Scenario 2: State Error ---");

    let mut builder = StateGraphBuilder::new();
    builder.add_node("bad_cast", Box::new(StateErrorNode));

    builder.add_edge(START_NODE, HashSet::from(["bad_cast".to_string()]));
    builder.add_edge("bad_cast", HashSet::from([END_NODE.to_string()]));

    let graph = builder.compile()?;
    let state = Arc::new(DefaultMemoryState::new());

    match graph.invoke(state).await {
        Ok(()) => println!("   ⚠️ Unexpected success"),
        Err(e) => println!("   📌 Caught error: {}\n", e),
    }

    Ok(())
}

/// Scenario 3: Business logic validation failure
async fn scenario_3_validation_error() -> Result<(), LangGraphError> {
    println!("--- Scenario 3: Validation Error ---");

    let mut builder = StateGraphBuilder::new();
    builder.add_node("validate", Box::new(ValidatorNode));

    builder.add_edge(START_NODE, HashSet::from(["validate".to_string()]));
    builder.add_edge("validate", HashSet::from([END_NODE.to_string()]));

    let graph = builder.compile()?;
    let state = Arc::new(DefaultMemoryState::new());
    // Deliberately NOT setting "required_field"

    match graph.invoke(state).await {
        Ok(()) => println!("   ⚠️ Unexpected success"),
        Err(e) => println!("   📌 Caught error: {}\n", e),
    }

    Ok(())
}

/// Scenario 4: Error recovery pattern (using separate workflow)
async fn scenario_4_error_recovery_pattern() -> Result<(), LangGraphError> {
    println!("--- Scenario 4: Error Recovery Pattern ---");

    // Simulate a main workflow that might fail
    let mut main_builder = StateGraphBuilder::new();
    main_builder.add_node(
        "risky_operation",
        Box::new(FailureNode::new("Service unavailable")),
    );
    main_builder.add_edge(START_NODE, HashSet::from(["risky_operation".to_string()]));
    main_builder.add_edge("risky_operation", HashSet::from([END_NODE.to_string()]));

    let main_graph = main_builder.compile()?;
    let state = Arc::new(DefaultMemoryState::new());

    // Try the main workflow
    match main_graph.invoke(state.clone()).await {
        Ok(()) => println!("   ✅ Main workflow succeeded"),
        Err(e) => {
            println!("   ⚠️ Main workflow failed: {}", e);
            println!("   🔄 Running fallback workflow...");

            // Fallback workflow for recovery
            let mut fallback_builder = StateGraphBuilder::new();
            fallback_builder.add_node("cleanup", Box::new(CleanupNode));
            fallback_builder.add_edge(START_NODE, HashSet::from(["cleanup".to_string()]));
            fallback_builder.add_edge("cleanup", HashSet::from([END_NODE.to_string()]));

            let fallback_graph = fallback_builder.compile()?;

            match fallback_graph.invoke(state).await {
                Ok(()) => println!("   ✅ Recovery successful!\n"),
                Err(recovery_err) => println!("   ❌ Recovery also failed: {}\n", recovery_err),
            }
        }
    }

    Ok(())
}
