//! # Data Processing Pipeline Example
//!
//! This example demonstrates a multi-step data processing workflow:
//! 1. Load data (simulate loading from source)
//! 2. Transform data (process and modify)
//! 3. Validate data (check quality)
//! 4. Save results (output final result)
//!
//! ## Run this example:
//!
//! ```bash
//! cargo run --example data_pipeline
//! ```

use langgraph4rust::*;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use async_trait::async_trait;
// ============================================================================
// Node Definitions
// ============================================================================

/// Simulates loading raw data from a source
#[derive(Clone)]
struct DataLoader;

#[async_trait]
impl AgentNode<DefaultMemoryState> for DataLoader {
    async fn apply(&self, state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
        println!("📥 Loading data...");

        // Simulate loading some raw data
        let raw_data: Vec<String> = (1..=5)
            .map(|i| format!("raw_item_{}", i))
            .collect();

        let item_count = raw_data.len();
        state.set("raw_data", raw_data).await?;
        state.set("source", "database").await?;

        println!("   Loaded {} items from 'database'", item_count);
        Ok(())
    }
}

/// Transforms the raw data into processed format
#[derive(Clone)]
struct DataTransformer;

#[async_trait]
impl AgentNode<DefaultMemoryState> for DataTransformer {
    async fn apply(&self, state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
        println!("🔄 Transforming data...");

        // Read raw data
        let raw_data: Option<Vec<String>> = state.get("raw_data").await?;
        if let Some(data) = raw_data {
            let item_count = data.len();  // Save length before moving

            // Transform each item
            let processed: Vec<String> = data
                .into_iter()
                .map(|item| format!("PROCESSED_{}", item.to_uppercase()))
                .collect();

            state.set("processed_data", processed).await?;
            println!("   Transformed {} items", item_count);
        } else {
            println!("   ⚠️ No raw data found");
        }

        Ok(())
    }
}

/// Validates the processed data
#[derive(Clone)]
struct DataValidator;

#[async_trait]
impl AgentNode<DefaultMemoryState> for DataValidator {
    async fn apply(&self, state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
        println!("✅ Validating data...");

        let processed: Option<Vec<String>> = state.get("processed_data").await?;

        match processed {
            Some(data) => {
                // Simple validation: check all items start with "PROCESSED_"
                let valid = data.iter().all(|item| item.starts_with("PROCESSED_"));
                let invalid_count = data.iter().filter(|item| !item.starts_with("PROCESSED_")).count();

                state.set("is_valid", valid).await?;
                state.set("total_items", data.len()).await?;
                state.set("invalid_count", invalid_count).await?;

                if valid {
                    println!("   ✅ All {} items are valid!", data.len());
                } else {
                    println!("   ⚠️ {} invalid items found", invalid_count);
                }
            }
            None => {
                println!("   ❌ No processed data to validate");
                state.set("is_valid", false).await?;
            }
        }

        Ok(())
    }
}

/// Saves the validated results
#[derive(Clone)]
struct DataSaver;

#[async_trait]
impl AgentNode<DefaultMemoryState> for DataSaver {
    async fn apply(&self, state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
        println!("💾 Saving results...");

        let is_valid: Option<bool> = state.get("is_valid").await?;
        let total: Option<usize> = state.get("total_items").await?;
        let processed: Option<Vec<String>> = state.get("processed_data").await?;

        // Create a summary report
        let summary = HashMap::from([
            ("status".to_string(), if is_valid.unwrap_or(false) { "success" } else { "failed" }.to_string()),
            ("total_items".to_string(), total.unwrap_or(0).to_string()),
            ("timestamp".to_string(), chrono::Utc::now().to_rfc3339()),
        ]);

        state.set("summary", summary).await?;

        println!("   Saved summary:");
        println!("     - Status: {}", if is_valid.unwrap_or(false) { "✅ Success" } else { "❌ Failed" });
        println!("     - Items: {}", total.unwrap_or(0));
        println!("     - Timestamp: {}", chrono::Utc::now().to_rfc3339());

        if let Some(data) = processed {
            println!("\n   📊 Processed data preview:");
            for (i, item) in data.iter().take(3).enumerate() {
                println!("     {}. {}", i + 1, item);
            }
            if data.len() > 3 {
                println!("     ... and {} more items", data.len() - 3);
            }
        }

        Ok(())
    }
}

// ============================================================================
// Main Execution
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), LangGraphError> {
    println!("=== Data Processing Pipeline ===\n");

    // Build the pipeline: load → transform → validate → save
    let mut builder = StateGraphBuilder::new();
    builder.set_max_steps(10);

    // Add nodes in execution order
    builder.add_node("load", Box::new(DataLoader));
    builder.add_node("transform", Box::new(DataTransformer));
    builder.add_node("validate", Box::new(DataValidator));
    builder.add_node("save", Box::new(DataSaver));

    // Connect nodes linearly
    builder.add_edge(START_NODE, HashSet::from(["load".to_string()]));
    builder.add_edge("load", HashSet::from(["transform".to_string()]));
    builder.add_edge("transform", HashSet::from(["validate".to_string()]));
    builder.add_edge("validate", HashSet::from(["save".to_string()]));
    builder.add_edge("save", HashSet::from([END_NODE.to_string()]));

    // Compile and execute
    println!("🔨 Building pipeline...\n");
    let graph = builder.compile()?;
    let state = Arc::new(DefaultMemoryState::new());

    graph.invoke(state.clone()).await?;

    println!("\n=== Pipeline Complete ===\n");
    println!("Final State Summary:");
    if let Some(summary) = state.get::<HashMap<String, String>>("summary").await? {
        for (key, value) in &summary {
            println!("  {}: {}", key, value);
        }
    }

    Ok(())
}