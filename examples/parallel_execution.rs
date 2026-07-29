//! # Parallel Execution Example
//!
//! Demonstrates automatic parallel execution when nodes fan-out.
//!
//! ```bash
//! cargo run --example parallel_execution
//! ```

use langgraph4rust::*;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use async_trait::async_trait;
use tokio::time::{sleep, Duration};

#[derive(Clone)]
struct UserFetcher;

#[async_trait]
impl AgentNode<DefaultMemoryState> for UserFetcher {
    async fn apply(&self, state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
        println!("👤 Fetching user data...");
        sleep(Duration::from_millis(100)).await;

        let user_data = HashMap::from([
            ("id".to_string(), "user_123".to_string()),
            ("name".to_string(), "Alice Johnson".to_string()),
        ]);

        state.set("user_data", user_data).await?;
        println!("   ✅ User data fetched");
        Ok(())
    }
}

#[derive(Clone)]
struct ProductFetcher;

#[async_trait]
impl AgentNode<DefaultMemoryState> for ProductFetcher {
    async fn apply(&self, state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
        println!("📦 Fetching products...");
        sleep(Duration::from_millis(150)).await;

        let products = vec!["Book", "Guide", "Manual"];
        state.set("products", products).await?;
        println!("   ✅ Products fetched");
        Ok(())
    }
}

#[derive(Clone)]
struct Aggregator;

#[async_trait]
impl AgentNode<DefaultMemoryState> for Aggregator {
    async fn apply(&self, state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
        println!("🔗 Aggregating results...");

        let user: Option<HashMap<String, String>> = state.get("user_data").await?;
        let products: Option<Vec<String>> = state.get("products").await?;

        if let Some(u) = user {
            println!("   User: {:?}", u.get("name"));
        }
        if let Some(p) = products {
            println!("   Products: {} items", p.len());
        }

        state.set("complete", true).await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), LangGraphError> {
    println!("=== Parallel Execution Example ===\n");

    let mut builder = StateGraphBuilder::new();

    builder.add_node("fetch_user", Box::new(UserFetcher));
    builder.add_node("fetch_products", Box::new(ProductFetcher));
    builder.add_node("aggregate", Box::new(Aggregator));

    // Fan-out pattern
    builder.add_edge(START_NODE, HashSet::from([
        "fetch_user".to_string(),
        "fetch_products".to_string(),
    ]));

    builder.add_edge("fetch_user", HashSet::from(["aggregate".to_string()]));
    builder.add_edge("fetch_products", HashSet::from(["aggregate".to_string()]));
    builder.add_edge("aggregate", HashSet::from([END_NODE.to_string()]));

    let graph = builder.compile()?;
    let state = Arc::new(DefaultMemoryState::new());

    println!("Executing parallel tasks...\n");
    graph.invoke(state).await?;

    println!("\n✅ Complete!");
    Ok(())
}