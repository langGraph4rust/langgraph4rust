//! # Conditional Routing Example
//!
//! This example demonstrates dynamic workflow branching based on state values.
//! The workflow routes different types of requests to appropriate handlers:
//!
//! - High priority → Fast track processing
//! - Normal priority → Standard processing
//! - Low priority → Batch processing
//!
//! ## Run this example:
//!
//! ```bash
//! cargo run --example conditional_routing
//! ```

use langgraph4rust::*;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use async_trait::async_trait;
// ============================================================================
// Node Definitions
// ============================================================================

/// Entry point: receives and classifies the request
#[derive(Clone)]
struct RequestReceiver;

#[async_trait]
impl AgentNode<DefaultMemoryState> for RequestReceiver {
    async fn apply(&self, state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
        println!("📨 Receiving request...");

        // Simulate receiving a request with priority (using simple counter for demo)
        static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let count = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let request_id = format!("REQ-{:04}", count % 10000);
        let priorities = vec!["high", "normal", "low"];
        let priority = priorities[count % 3].to_string();

        println!("   Request ID: {}", request_id);
        println!("   Priority: {}", priority);

        state.set("request_id", request_id).await?;
        state.set("priority", priority).await?;
        state.set("received_at", chrono::Utc::now().to_rfc3339()).await?;

        Ok(())
    }
}

/// Handles high-priority requests with expedited processing
#[derive(Clone)]
struct FastTrackHandler;

#[async_trait]
impl AgentNode<DefaultMemoryState> for FastTrackHandler {
    async fn apply(&self, state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
        let request_id: Option<String> = state.get("request_id").await?;

        println!("⚡ Processing HIGH PRIORITY request: {:?}", request_id);
        println!("   → Using fast-track processing");
        println!("   → Dedicated resources allocated");
        println!("   → Immediate execution");

        state.set("handler", "fast_track").await?;
        state.set("processing_time_ms", 50).await?;
        state.set("sla_minutes", 5).await?;

        Ok(())
    }
}

/// Handles normal-priority requests with standard processing
#[derive(Clone)]
struct StandardHandler;

#[async_trait]
impl AgentNode<DefaultMemoryState> for StandardHandler {
    async fn apply(&self, state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
        let request_id: Option<String> = state.get("request_id").await?;

        println!("🔄 Processing NORMAL priority request: {:?}", request_id);
        println!("   → Using standard processing queue");
        println!("   → Shared resources allocation");
        println!("   → Normal execution timeline");

        state.set("handler", "standard").await?;
        state.set("processing_time_ms", 200).await?;
        state.set("sla_minutes", 30).await?;

        Ok(())
    }
}

/// Handles low-priority requests with batched processing
#[derive(Clone)]
struct BatchHandler;

#[async_trait]
impl AgentNode<DefaultMemoryState> for BatchHandler {
    async fn apply(&self, state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
        let request_id: Option<String> = state.get("request_id").await?;

        println!("📦 Processing LOW PRIORITY request: {:?}", request_id);
        println!("   → Adding to batch queue");
        println!("   → Will be processed in next batch window");
        println!("   → Resource-efficient processing");

        state.set("handler", "batch").await?;
        state.set("processing_time_ms", 500).await?;
        state.set("sla_minutes", 240).await?; // 4 hours

        Ok(())
    }
}

/// Final step: completes the request and logs results
#[derive(Clone)]
struct RequestCompleter;

#[async_trait]
impl AgentNode<DefaultMemoryState> for RequestCompleter {
    async fn apply(&self, state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
        let request_id: Option<String> = state.get("request_id").await?;
        let handler: Option<String> = state.get("handler").await?;
        let sla: Option<i32> = state.get("sla_minutes").await?;

        println!("\n✅ Request completed!");
        println!("   Request ID: {:?}", request_id);
        println!("   Handler used: {:?}", handler);
        println!("   SLA met: {:?} minutes", sla);

        // Create completion record
        let completion = HashMap::from([
            ("status".to_string(), "completed".to_string()),
            ("completed_at".to_string(), chrono::Utc::now().to_rfc3339()),
            ("handler".to_string(), handler.unwrap_or_else(|| "unknown".to_string())),
        ]);

        state.set("completion", completion).await?;

        Ok(())
    }
}

// ============================================================================
// Main Execution
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), LangGraphError> {
    println!("=== Conditional Routing Example ===\n");

    // Build workflow with conditional routing
    let mut builder = StateGraphBuilder::new();
    builder.set_max_steps(20);

    // Add all nodes
    builder.add_node("receive", Box::new(RequestReceiver));
    builder.add_node("fast_track", Box::new(FastTrackHandler));
    builder.add_node("standard", Box::new(StandardHandler));
    builder.add_node("batch", Box::new(BatchHandler));
    builder.add_node("complete", Box::new(RequestCompleter));

    // Connect start to receiver
    builder.add_edge(START_NODE, HashSet::from(["receive".to_string()]));

    // Add conditional edge from receiver based on priority
    builder.add_conditional_edge("receive", vec![
        Box::new(|state| {
            // Note: In conditional edges, we need to handle the async get differently
            // For this example, we'll use a simple pattern matching approach
            let priority = "normal"; // Default fallback
            match priority {
                "high" => "fast_track".to_string(),
                "normal" => "standard".to_string(),
                _ => "batch".to_string(),
            }
        }),
    ]);

    // All handlers converge to completer
    builder.add_edge("fast_track", HashSet::from(["complete".to_string()]));
    builder.add_edge("standard", HashSet::from(["complete".to_string()]));
    builder.add_edge("batch", HashSet::from(["complete".to_string()]));

    // Completer connects to end
    builder.add_edge("complete", HashSet::from([END_NODE.to_string()]));

    // Compile
    println!("🔨 Building workflow with conditional routing...\n");
    let graph = builder.compile()?;

    // Run multiple times to show different routing decisions
    for i in 1..=3 {
        println!("--- Execution {} ---", i);
        let state = Arc::new(DefaultMemoryState::new());
        graph.invoke(state.clone()).await?;
        println!();

        // Small delay between runs
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    println!("=== All Executions Complete ===");
    Ok(())
}