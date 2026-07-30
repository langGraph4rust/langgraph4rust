# langgraph4rust 🦀

[![CI](https://github.com/langGraph4rust/langgraph4rust/actions/workflows/ci.yml/badge.svg)](https://github.com/langGraph4rust/langgraph4rust/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/langgraph4rust.svg)](https://crates.io/crates/langgraph4rust)
[![Docs.rs](https://docs.rs/langgraph4rust/badge.svg)](https://docs.rs/langgraph4rust)
[![Downloads](https://img.shields.io/crates/d/langgraph4rust.svg)](https://crates.io/crates/langgraph4rust)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-1.85%2B-orange.svg)](https://www.rust-lang.org/)

**受 Python LangGraph 库启发的强大 Rust 有状态工作流引擎实现。**

`langgraph4rust` 提供了一个灵活、类型安全且以异步优先的框架，用于构建、执行和管理复杂的工作流图，支持并行执行和条件路由。

## ✨ 特性

- **🏗️ 声明式图构建**：使用直观的构建器模式定义工作流
- **⚡ 并行执行**：当依赖允许时，多个节点可以同时执行
- **🔀 条件路由**：根据运行时状态条件动态选择路径
- **💾 状态管理**：内置基于 JSON 的状态持久化，完全类型安全
- **🔌 可扩展架构**：通过 trait 实现自定义节点
- **✅ 全面验证**：执行前验证图结构，防止运行时错误
- **🎯 异步优先设计**：基于 Tokio 构建，实现高效异步操作

## 📦 安装

在 `Cargo.toml` 中添加：

```toml
[dependencies]
langgraph4rust = "0.1.1"
```

## 🚀 快速开始

### 基础示例

```rust
use langgraph4rust::*;
use std::collections::HashSet;
use std::sync::Arc;
use async_trait::async_trait;

// 定义自定义节点
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
    // 创建图构建器
    let mut builder = StateGraphBuilder::new();

    // 向图中添加节点
    builder.add_node("greet", Box::new(GreetingNode));

    // 定义边（工作流连接）
    builder.add_edge(START_NODE, HashSet::from(["greet".to_string()]));
    builder.add_edge("greet", HashSet::from([END_NODE.to_string()]));

    // 编译并执行
    let graph = builder.compile()?;
    let state = Arc::new(DefaultMemoryState::new());
    
    graph.invoke(state).await?;
    
    Ok(())
}
```

运行此示例：
```bash
cargo run --example hello_world
```

## 🎯 核心概念

### 节点 🔷

节点是工作流的基本构建块。每个节点都实现了 [`AgentNode`] trait，包含处理和修改共享状态的逻辑。

```rust
#[derive(Clone)]
struct MyNode;

#[async_trait]
impl AgentNode<DefaultMemoryState> for MyNode {
    async fn apply(&self, state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
        // 你的节点逻辑
        state.set("result", "processed").await?;
        Ok(())
    }
}
```

### 边 ➡️

边定义节点之间的控制流：

- **静态边**：始终连接到固定的目标节点
- **条件边**：根据当前状态动态选择目标

```rust
// 静态边
builder.add_edge("node_a", HashSet::from(["node_b".to_string()]));

// 条件路由（参见 conditional_routing 示例）
builder.add_conditional_edges(
    "decision_node",
    |state| async move { /* 路由逻辑 */ },
    HashMap::from([
        ("option_a".to_string(), HashSet::from(["node_x".to_string()])),
        ("option_b".to_string(), HashSet::from(["node_y".to_string()])),
    ])
)?;
```

### 状态 💾

状态在所有节点之间共享并在整个执行过程中持续存在：

```rust
let state = Arc::new(DefaultMemoryState::new());

// 设置值
state.set("key", "value").await?;

// 获取类型化值
let value: String = state.get("key").await?.unwrap();
```

## 📚 示例

探索 `examples/` 目录以获取完整的可运行示例：

| 示例 | 描述 |
|------|------|
| [hello_world](examples/hello_world.rs) | 简单线性工作流 - 完美的入门起点 |
| [conditional_routing](examples/conditional_routing.rs) | 基于状态的动态路径选择 |
| [parallel_execution](examples/parallel_execution.rs) | 并发节点执行 |
| [custom_state](examples/custom_state.rs) | 实现自定义状态后端 |
| [data_pipeline](examples/data_pipeline.rs) | 多阶段数据处理管道 |
| [error_handling](examples/error_handling.rs) | 健壮的错误处理策略 |

运行任何示例：
```bash
cargo run --example <example_name>
```

## 🏗️ 架构

```
┌─────────────────────────────────────────────┐
│              StateGraphBuilder              │
│  (声明式图构建 API)                         │
└──────────────────┬──────────────────────────┘
                   │ compile()
                   ▼
┌─────────────────────────────────────────────┐
│               StateGraph                    │
│  (已验证的可执行工作流)                      │
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
│  (基于 JSON 的持久化状态存储)                │
└─────────────────────────────────────────────┘
```

## 🔧 API 参考

### 核心类型

- **[`StateGraphBuilder`](src/core/state_graph_builder.rs)**：用于构建工作流图的构建器
- **[`StateGraph`](src/core/state_graph.rs)**：编译后的可执行图实例
- **[`AgentNode`](src/core/agent_node.rs)**：实现自定义节点的 trait
- **[`AgentState`](src/core/agent_state.rs)**：状态管理后端的 trait
- **[`DefaultMemoryState`](src/core/agent_state.rs)**：内置的基于 JSON 的状态实现
- **[`LangGraphError`](src/core/error.rs)**：库的错误类型

### 关键方法

```rust
// 构建图
StateGraphBuilder::new()                          // 创建新构建器
builder.add_node(name, node)                      // 添加节点
builder.add_edge(from, to)                        // 添加静态边
builder.add_conditional_edges(from, router, map)  // 添加条件边
builder.compile()                                 // 验证并构建图

// 执行工作流
graph.invoke(initial_state)                       // 运行工作流

// 管理状态
state.set(key, value).await                       // 存储值
state.get::<T>(key).await?                        // 获取类型化值
```

## 🧪 测试

运行测试套件：

```bash
# 单元测试
cargo test

# 运行特定示例测试
cargo test --example hello_world

# 集成测试（如果可用）
cargo test --test integration_test
```

## 🤝 贡献

欢迎贡献！请随时提交 Pull Request。

1. Fork 本仓库
2. 创建你的功能分支 (`git checkout -b feature/amazing-feature`)
3. 提交你的更改 (`git commit -m 'Add amazing feature'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 打开 Pull Request

### 开发环境设置

```bash
# 克隆仓库
git clone https://github.com/langGraph4rust/langgraph4rust.git
cd langgraph4rust

# 构建项目
cargo build

# 运行示例
cargo run --example hello_world

# 运行测试
cargo test
```

## 📋 要求

- **Rust**: 2024 版本或更高版本
- **Tokio**: 异步运行时（作为依赖项包含）
- **平台**: macOS、Linux、Windows（已测试）

## 📄 许可证

本项目采用 Apache 许可证 2.0 授权 - 详情请参阅 [LICENSE](LICENSE) 文件。

## 🙏 致谢

- 灵感来源于 [LangChain/LangGraph](https://github.com/langchain-ai/langgraph) Python 库
- 使用 [Rust](https://www.rust-lang.org/) 生态工具构建
- 异步运行时由 [Tokio](https://tokio.rs/) 提供支持

## 📞 支持

- 📖 查看 [examples](examples/) 了解使用模式
- 🐛 通过 [GitHub Issues](https://github.com/langGraph4rust/langgraph4rust/issues) 报告问题
- 💬 欢迎在 [GitHub Discussions](https://github.com/langGraph4rust/langgraph4rust/discussions) 中讨论

---

**使用 Rust 的类型系统和异步能力构建 ❤️**
EOF 