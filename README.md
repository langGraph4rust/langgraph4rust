# langgraph4rust 🦀

[![Rust](https://img.shields.io/badge/Rust-2024-orange.svg)](https://www.rust-lang.org/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Crates.io](https://img.shields.io/crates/v/langgraph4rust.svg)](https://crates.io/crates/langgraph4rust)

**A powerful Rust implementation of a stateful workflow engine inspired by Python's LangGraph library.**

`langgraph4rust` provides a flexible, type-safe, and async-first framework for building, executing, and managing complex workflow graphs with support for parallel execution and conditional routing.

## ✨ Features

- **🏗️ Declarative Graph Building**: Define workflows using an intuitive builder pattern
- **⚡ Parallel Execution**: Multiple nodes can execute simultaneously when dependencies allow
- **🔀 Conditional Routing**: Dynamic path selection based on runtime state conditions
- **💾 State Management**: Built-in JSON-based state persistence with full type safety
- **🔌 Extensible Architecture**: Custom node implementations via traits
- **✅ Comprehensive Validation**: Graph structure validation before execution prevents runtime errors
- **🎯 Async-First Design**: Built on Tokio for efficient asynchronous operations

## 📦 Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
langgraph4rust = "0.1.0"
tokio = { version = "1", features = ["full"] }
```

## 🚀 Quick Start

### Basic Example

```rust
use langgraph4rust::*;
use std::collections::HashSet;
use std::sync::Arc;
use async_trait::async_trait;

// Define a custom node
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
    // Create graph builder
    let mut builder = StateGraphBuilder::new();

    // Add nodes to the graph
    builder.add_node("greet", Box::new(GreetingNode));

    // Define edges (workflow connections)
    builder.add_edge(START_NODE, HashSet::from(["greet".to_string()]));
    builder.add_edge("greet", HashSet::from([END_NODE.to_string()]));

    // Compile and execute
    let graph = builder.compile()?;
    let state = Arc::new(DefaultMemoryState::new());
    
    graph.invoke(state).await?;
    
    Ok(())
}
```

Run this example:
```bash
cargo run --example hello_world
```

## 🎯 Core Concepts

### Nodes 🔷

Nodes are the fundamental building blocks of your workflow. Each node implements the [`AgentNode`] trait and contains logic to process and modify the shared state.

```rust
#[derive(Clone)]
struct MyNode;

#[async_trait]
impl AgentNode<DefaultMemoryState> for MyNode {
    async fn apply(&self, state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
        // Your node logic here
        state.set("result", "processed").await?;
        Ok(())
    }
}
```

### Edges ➡️

Edges define the control flow between nodes:

- **Static edges**: Always connect to fixed target nodes
- **Conditional edges**: Dynamically choose targets based on current state

```rust
// Static edge
builder.add_edge("node_a", HashSet::from(["node_b".to_string()]));

// Conditional routing (see conditional_routing example)
builder.add_conditional_edges(
    "decision_node",
    |state| async move { /* routing logic */ },
    HashMap::from([
        ("option_a".to_string(), HashSet::from(["node_x".to_string()])),
        ("option_b".to_string(), HashSet::from(["node_y".to_string()])),
    ])
)?;
```

### State 💾

The state is shared across all nodes and persists throughout execution:

```rust
let state = Arc::new(DefaultMemoryState::new());

// Set values
state.set("key", "value").await?;

// Get typed values
let value: String = state.get("key").await?.unwrap();
```

## 📚 Examples

Explore the `examples/` directory for complete working examples:

| Example | Description |
|---------|-------------|
| [hello_world](examples/hello_world.rs) | Simple linear workflow - perfect starting point |
| [conditional_routing](examples/conditional_routing.rs) | Dynamic path selection based on state |
| [parallel_execution](examples/parallel_execution.rs) | Concurrent node execution |
| [custom_state](examples/custom_state.rs) | Implementing custom state backends |
| [data_pipeline](examples/data_pipeline.rs) | Multi-stage data processing pipeline |
| [error_handling](examples/error_handling.rs) | Robust error handling strategies |

Run any example:
```bash
cargo run --example <example_name>
```

## 🏗️ Architecture

```
┌─────────────────────────────────────────────┐
│              StateGraphBuilder              │
│  (Declarative graph construction API)       │
└──────────────────┬──────────────────────────┘
                   │ compile()
                   ▼
┌─────────────────────────────────────────────┐
│               StateGraph                    │
│  (Validated, executable workflow)           │
│  ┌─────────┐   ┌─────────┐   ┌─────────┐  │
│  │ Node A  │──▶│ Node B  │──▶│ Node C  │  │
│  └─────────┘   └─────────┘   └─────────┘  │
│         ▲                         │        │
│         └──────────(state)────────┘        │
└─────────────────────────────────────────────┘
                   │ invoke()
                   ▼
┌─────────────────────────────────────────────┐
│          DefaultMemoryState                 │
│  (JSON-based persistent state storage)      │
└─────────────────────────────────────────────┘
```

## 🔧 API Reference

### Core Types

- **[`StateGraphBuilder`](src/core/state_graph_builder.rs)**: Builder for constructing workflow graphs
- **[`StateGraph`](src/core/state_graph.rs)**: Compiled, executable graph instance
- **[`AgentNode`](src/core/agent_node.rs)**: Trait for implementing custom nodes
- **[`AgentState`](src/core/agent_state.rs)**: Trait for state management backends
- **[`DefaultMemoryState`](src/core/agent_state.rs)**: Built-in JSON-based state implementation
- **[`LangGraphError`](src/core/error.rs)**: Error type for the library

### Key Methods

```rust
// Building graphs
StateGraphBuilder::new()                          // Create new builder
builder.add_node(name, node)                      // Add a node
builder.add_edge(from, to)                        // Add static edge
builder.add_conditional_edges(from, router, map)  // Add conditional edge
builder.compile()                                 // Validate & build graph

// Executing workflows
graph.invoke(initial_state)                       // Run the workflow

// Managing state
state.set(key, value).await                       // Store value
state.get::<T>(key).await?                        // Retrieve typed value
```

## 🧪 Testing

Run the test suite:

```bash
# Unit tests
cargo test

# Run specific example tests
cargo test --example hello_world

# Integration tests (if available)
cargo test --test integration_test
```

## 🤝 Contributing

Contributions are welcome! Please feel free to submit Pull Requests.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Setup

```bash
# Clone the repository
git clone https://github.com/langGraph4rust/langgraph4rust.git
cd langgraph4rust

# Build the project
cargo build

# Run examples
cargo run --example hello_world

# Run tests
cargo test
```

## 📋 Requirements

- **Rust**: 2024 edition or later
- **Tokio**: Async runtime (included as dependency)
- **Platform**: macOS, Linux, Windows (tested)

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Inspired by [LangChain/LangGraph](https://github.com/langchain-ai/langgraph) Python library
- Built with [Rust](https://www.rust-lang.org/) ecosystem tools
- Async runtime powered by [Tokio](https://tokio.rs/)

## 📞 Support

- 📖 Check the [examples](examples/) for usage patterns
- 🐛 Report issues via [GitHub Issues](https://github.com/langGraph4rust/langgraph4rust/issues)
- 💬 Discussions welcome in [GitHub Discussions](https://github.com/langGraph4rust/langgraph4rust/discussions)

---

**Built with ❤️ using Rust's type system and async capabilities**