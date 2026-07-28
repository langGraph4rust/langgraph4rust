use langgraph4rust::{
    AgentNode, AgentState, DefaultMemoryState, LangGraphError, StateGraphBuilder,
};
use std::sync::Arc;
use std::collections::HashSet;
use futures::executor::block_on;

/// 计数器节点：每次执行将状态中的 count 值加 1
#[derive(Debug, Clone)]
struct CounterNode;

impl AgentNode<DefaultMemoryState> for CounterNode {
    fn apply(&self, state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
        let count: i32 = block_on(async {
            state.get("count").await.unwrap_or(None)
        }).unwrap_or(0);
        block_on(async {
            state.set("count", count + 1).await
        })?;
        Ok(())
    }
}

/// 消息节点：将指定消息写入状态
#[derive(Debug, Clone)]
struct MessageNode {
    message: String,
}

impl AgentNode<DefaultMemoryState> for MessageNode {
    fn apply(&self, state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
        block_on(async {
            state.set("message", self.message.clone()).await
        })?;
        Ok(())
    }
}

/// 失败节点：故意返回错误，用于测试错误处理
#[derive(Debug, Clone)]
struct FailingNode;

impl AgentNode<DefaultMemoryState> for FailingNode {
    fn apply(&self, _state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
        Err(LangGraphError::NodeError("Intentional failure".to_string()))
    }
}

/// 测试场景：简单线性工作流
/// 验证单个节点的基本执行流程：start -> counter -> end
#[tokio::test]
async fn test_simple_linear_workflow() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("counter", Box::new(CounterNode));
    builder.add_edge("__start__", HashSet::from(["counter".to_string()]));
    builder.add_edge("counter", HashSet::from(["__end__".to_string()]));
    let graph = builder.compile()?;

    let state = Arc::new(DefaultMemoryState::new());
    graph.invoke(state.clone()).await?;

    let count: i32 = state.get("count").await?.unwrap();
    assert_eq!(count, 1, "Counter should be incremented once");

    Ok(())
}

/// 测试场景：多步骤顺序工作流
/// 验证多个节点按顺序执行：start -> counter1 -> counter2 -> end
#[tokio::test]
async fn test_multi_step_workflow() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("counter1", Box::new(CounterNode));
    builder.add_node("counter2", Box::new(CounterNode));
    builder.add_edge("__start__", HashSet::from(["counter1".to_string()]));
    builder.add_edge("counter1", HashSet::from(["counter2".to_string()]));
    builder.add_edge("counter2", HashSet::from(["__end__".to_string()]));
    let graph = builder.compile()?;

    let state = Arc::new(DefaultMemoryState::new());
    graph.invoke(state.clone()).await?;

    let count: i32 = state.get("count").await?.unwrap();
    assert_eq!(count, 2, "Counter should be incremented twice");

    Ok(())
}

/// 测试场景：并行节点执行
/// 验证多个节点可以同时执行：start -> node1 + node2 -> end
#[tokio::test]
async fn test_parallel_execution() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("node1", Box::new(MessageNode { message: "hello".to_string() }));
    builder.add_node("node2", Box::new(CounterNode));
    builder.add_edge("__start__", HashSet::from(["node1".to_string(), "node2".to_string()]));
    builder.add_edge("node1", HashSet::from(["__end__".to_string()]));
    builder.add_edge("node2", HashSet::from(["__end__".to_string()]));
    let graph = builder.compile()?;

    let state = Arc::new(DefaultMemoryState::new());
    graph.invoke(state.clone()).await?;

    let message: String = state.get("message").await?.unwrap();
    let count: i32 = state.get("count").await?.unwrap();
    
    assert_eq!(message, "hello", "Message should be set");
    assert_eq!(count, 1, "Counter should be incremented");

    Ok(())
}

/// 测试场景：空图验证
/// 验证编译空图（无节点）时应返回错误
#[tokio::test]
async fn test_empty_graph_validation() {
    let result = StateGraphBuilder::<DefaultMemoryState>::new().compile();

    assert!(
        matches!(result, Err(LangGraphError::GraphError(msg)) if msg.contains("at least one node")),
        "Empty graph should fail validation"
    );
}

/// 测试场景：起始节点无出边验证
/// 验证当起始节点没有出边时应返回错误
#[tokio::test]
async fn test_start_node_without_edges() {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("node", Box::new(CounterNode));
    let result = builder.compile();

    assert!(
        matches!(result, Err(LangGraphError::GraphError(msg)) if msg.contains("outgoing edge")),
        "Start node without edges should fail validation"
    );
}

/// 测试场景：无效边目标验证
/// 验证边指向未注册的节点时应返回错误
#[tokio::test]
async fn test_invalid_edge_target() {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("node", Box::new(CounterNode));
    builder.add_edge("__start__", HashSet::from(["nonexistent".to_string()]));
    let result = builder.compile();

    assert!(
        matches!(result, Err(LangGraphError::GraphError(msg)) if msg.contains("not a registered node")),
        "Invalid edge target should fail validation"
    );
}

/// 测试场景：节点执行失败
/// 验证节点执行失败时错误能够正确传播
#[tokio::test]
async fn test_node_failure() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("failing", Box::new(FailingNode));
    builder.add_edge("__start__", HashSet::from(["failing".to_string()]));
    builder.add_edge("failing", HashSet::from(["__end__".to_string()]));
    let graph = builder.compile()?;

    let state = Arc::new(DefaultMemoryState::new());
    let result = graph.invoke(state).await;

    assert!(
        matches!(result, Err(LangGraphError::NodeError(msg)) if msg.contains("Intentional failure")),
        "Node failure should propagate error"
    );

    Ok(())
}

/// 测试场景：状态持久化
/// 验证多次调用图时状态能够正确累积
#[tokio::test]
async fn test_state_persistence() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("counter", Box::new(CounterNode));
    builder.add_edge("__start__", HashSet::from(["counter".to_string()]));
    builder.add_edge("counter", HashSet::from(["__end__".to_string()]));
    let graph = builder.compile()?;

    let state = Arc::new(DefaultMemoryState::new());
    
    graph.invoke(state.clone()).await?;
    let count1: i32 = state.get("count").await?.unwrap();
    assert_eq!(count1, 1);

    graph.invoke(state.clone()).await?;
    let count2: i32 = state.get("count").await?.unwrap();
    assert_eq!(count2, 2);

    Ok(())
}

/// 测试场景：自定义起始和结束节点
/// 验证可以使用自定义的起始和结束节点名称
#[tokio::test]
async fn test_custom_start_end_nodes() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.set_start_node("begin");
    builder.set_end_node("finish");
    builder.add_node("begin", Box::new(CounterNode));
    builder.add_node("middle", Box::new(CounterNode));
    builder.add_node("finish", Box::new(CounterNode));
    builder.add_edge("begin", HashSet::from(["middle".to_string()]));
    builder.add_edge("middle", HashSet::from(["finish".to_string()]));
    let graph = builder.compile()?;

    let state = Arc::new(DefaultMemoryState::new());
    graph.invoke(state.clone()).await?;

    let count: i32 = state.get("count").await?.unwrap();
    assert_eq!(count, 3, "All three nodes should execute");

    Ok(())
}

/// 测试场景：获取不存在的键
/// 验证从状态中获取不存在的键时返回 None
#[tokio::test]
async fn test_state_get_nonexistent_key() -> Result<(), LangGraphError> {
    let state = Arc::new(DefaultMemoryState::new());
    
    let result: Option<String> = state.get("nonexistent").await?;
    assert!(result.is_none(), "Getting non-existent key should return None");

    Ok(())
}

/// 测试场景：状态读写往返
/// 验证不同类型的数据能够正确写入和读取
#[tokio::test]
async fn test_state_set_get_roundtrip() -> Result<(), LangGraphError> {
    let state = Arc::new(DefaultMemoryState::new());
    
    state.set("string_key", "hello world").await?;
    state.set("int_key", 42).await?;
    state.set("float_key", 3.14).await?;

    let string_val: String = state.get("string_key").await?.unwrap();
    let int_val: i32 = state.get("int_key").await?.unwrap();
    let float_val: f64 = state.get("float_key").await?.unwrap();

    assert_eq!(string_val, "hello world");
    assert_eq!(int_val, 42);
    assert!((float_val - 3.14).abs() < 0.001);

    Ok(())
}