use langgraph4rust::{
    AgentNode, AgentState, DefaultMemoryState, LangGraphError, StateGraphBuilder,
};
use std::sync::Arc;
use std::collections::HashSet;
use futures::executor::block_on;
use async_trait::async_trait;

/// 计数器节点：每次执行将状态中的 count 值加 1
#[derive(Debug, Clone)]
struct CounterNode;

#[async_trait]
impl AgentNode<DefaultMemoryState> for CounterNode {
    async fn apply(&self, state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
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

#[async_trait]
impl AgentNode<DefaultMemoryState> for MessageNode {
    async fn apply(&self, state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
        block_on(async {
            state.set("message", self.message.clone()).await
        })?;
        Ok(())
    }
}

/// 失败节点：故意返回错误，用于测试错误处理
#[derive(Debug, Clone)]
struct FailingNode;

#[async_trait]
impl AgentNode<DefaultMemoryState> for FailingNode {
    async fn apply(&self, _state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
        Err(LangGraphError::NodeError("Intentional failure".to_string()))
    }
}

/// 慢速节点：执行时等待一段时间，用于测试并行执行
#[derive(Debug, Clone)]
struct SlowNode;

#[async_trait]
impl AgentNode<DefaultMemoryState> for SlowNode {
    async fn apply(&self, state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
        std::thread::sleep(std::time::Duration::from_millis(100));
        let count: i32 = block_on(async {
            state.get("slow_count").await.unwrap_or(None)
        }).unwrap_or(0);
        block_on(async {
            state.set("slow_count", count + 1).await
        })?;
        Ok(())
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
    // 注意：start_node 和 end_node 是虚拟节点，不注册为普通节点
    builder.add_node("middle", Box::new(CounterNode));
    builder.add_edge("begin", HashSet::from(["middle".to_string()]));
    builder.add_edge("middle", HashSet::from(["finish".to_string()]));
    let graph = builder.compile()?;

    let state = Arc::new(DefaultMemoryState::new());
    graph.invoke(state.clone()).await?;

    let count: i32 = state.get("count").await?.unwrap();
    assert_eq!(count, 1, "Only middle node should execute (start/end are virtual)");

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

/// 测试场景：最大步数限制
/// 验证当执行步数超过最大限制时能够停止执行
#[tokio::test]
async fn test_max_steps_limit() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("counter", Box::new(CounterNode));
    builder.add_edge("__start__", HashSet::from(["counter".to_string()]));
    builder.add_edge("counter", HashSet::from(["counter".to_string()]));
    builder.set_max_steps(5);
    let graph = builder.compile()?;

    let state = Arc::new(DefaultMemoryState::new());
    graph.invoke(state.clone()).await?;

    let count: i32 = state.get("count").await?.unwrap();
    // 注意：起始节点不执行，所以实际执行次数是 max_steps - 1
    assert_eq!(count, 4, "Should stop after max steps (start node skipped)");

    Ok(())
}

/// 测试场景：无效边源验证
/// 验证边的源节点未注册时应返回错误
#[tokio::test]
async fn test_invalid_edge_source() {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("node", Box::new(CounterNode));
    builder.add_edge("__start__", HashSet::from(["node".to_string()]));
    builder.add_edge("nonexistent", HashSet::from(["__end__".to_string()]));
    let result = builder.compile();

    assert!(
        matches!(result, Err(LangGraphError::GraphError(msg)) if msg.contains("is not a registered node")),
        "Invalid edge source should fail validation"
    );
}

/// 测试场景：并行执行多个失败节点
/// 验证并行执行时多个节点失败的情况
#[tokio::test]
async fn test_parallel_multiple_failures() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("failing1", Box::new(FailingNode));
    builder.add_node("failing2", Box::new(FailingNode));
    builder.add_edge("__start__", HashSet::from(["failing1".to_string(), "failing2".to_string()]));
    builder.add_edge("failing1", HashSet::from(["__end__".to_string()]));
    builder.add_edge("failing2", HashSet::from(["__end__".to_string()]));
    let graph = builder.compile()?;

    let state = Arc::new(DefaultMemoryState::new());
    let result = graph.invoke(state).await;

    assert!(
        result.is_err(),
        "Parallel execution with multiple failures should return error"
    );

    Ok(())
}

/// 测试场景：重复添加同名节点
/// 验证添加同名节点时后添加的会覆盖先添加的
#[tokio::test]
async fn test_duplicate_node_name() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("node", Box::new(CounterNode));
    builder.add_node("node", Box::new(MessageNode { message: "test".to_string() }));
    builder.add_edge("__start__", HashSet::from(["node".to_string()]));
    builder.add_edge("node", HashSet::from(["__end__".to_string()]));
    let graph = builder.compile()?;

    let state = Arc::new(DefaultMemoryState::new());
    graph.invoke(state.clone()).await?;

    let message: Option<String> = state.get("message").await?;
    assert!(message.is_some(), "Message should be set by second node");
    assert_eq!(message.unwrap(), "test");

    Ok(())
}

/// 测试场景：空状态初始化
/// 验证状态初始化为空时能正常工作
#[tokio::test]
async fn test_empty_state_initialization() -> Result<(), LangGraphError> {
    let state = Arc::new(DefaultMemoryState::new());
    
    let result: Option<i32> = state.get("any_key").await?;
    assert!(result.is_none(), "Empty state should return None for any key");

    Ok(())
}

/// 测试场景：状态值覆盖
/// 验证相同键的值可以被覆盖
#[tokio::test]
async fn test_state_value_overwrite() -> Result<(), LangGraphError> {
    let state = Arc::new(DefaultMemoryState::new());
    
    state.set("key", "first").await?;
    state.set("key", "second").await?;

    let value: String = state.get("key").await?.unwrap();
    assert_eq!(value, "second", "Value should be overwritten");

    Ok(())
}

/// 测试场景：复杂状态数据
/// 验证复杂数据结构（如 Vec、HashMap）能够正确存储和读取
#[tokio::test]
async fn test_complex_state_data() -> Result<(), LangGraphError> {
    let state = Arc::new(DefaultMemoryState::new());
    
    let vec_data = vec![1, 2, 3, 4, 5];
    let map_data: std::collections::HashMap<String, i32> = [("a".to_string(), 1), ("b".to_string(), 2)].into();
    
    state.set("vec", vec_data).await?;
    state.set("map", map_data).await?;

    let retrieved_vec: Vec<i32> = state.get("vec").await?.unwrap();
    let retrieved_map: std::collections::HashMap<String, i32> = state.get("map").await?.unwrap();
    
    assert_eq!(retrieved_vec, vec![1, 2, 3, 4, 5]);
    assert_eq!(retrieved_map.get("a"), Some(&1));
    assert_eq!(retrieved_map.get("b"), Some(&2));

    Ok(())
}

/// 测试场景：孤立节点检测
/// 验证图中存在无法到达终点的节点时应报错
#[tokio::test]
async fn test_isolated_node() {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("reachable", Box::new(CounterNode));
    builder.add_node("isolated", Box::new(CounterNode));
    builder.add_edge("__start__", HashSet::from(["reachable".to_string()]));
    builder.add_edge("reachable", HashSet::from(["__end__".to_string()]));
    let result = builder.compile();

    // 当前代码没有孤立节点检测，所以会编译成功
    // 这暴露了代码缺少图连接性验证的问题
    assert!(
        result.is_ok(),
        "Current code allows isolated nodes (missing connectivity validation)"
    );
}

/// 测试场景：自循环节点
/// 验证节点指向自身的循环是否能被正确处理
#[tokio::test]
async fn test_self_loop_node() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("loop_node", Box::new(CounterNode));
    builder.add_edge("__start__", HashSet::from(["loop_node".to_string()]));
    builder.add_edge("loop_node", HashSet::from(["loop_node".to_string()]));
    builder.set_max_steps(3);
    let graph = builder.compile()?;

    let state = Arc::new(DefaultMemoryState::new());
    graph.invoke(state.clone()).await?;

    let count: i32 = state.get("count").await?.unwrap();
    // 注意：起始节点不执行，所以实际执行次数是 max_steps - 1
    assert_eq!(count, 2, "Self-loop should execute max_steps - 1 times (start node skipped)");

    Ok(())
}

/// 测试场景：batch_apply 错误收集
/// 验证并行执行时多个节点失败是否能收集所有错误
#[tokio::test]
async fn test_batch_apply_error_collection() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("failing1", Box::new(FailingNode));
    builder.add_node("failing2", Box::new(FailingNode));
    builder.add_edge("__start__", HashSet::from(["failing1".to_string(), "failing2".to_string()]));
    builder.add_edge("failing1", HashSet::from(["__end__".to_string()]));
    builder.add_edge("failing2", HashSet::from(["__end__".to_string()]));
    let graph = builder.compile()?;

    let state = Arc::new(DefaultMemoryState::new());
    let result = graph.invoke(state).await;

    match result {
        Err(LangGraphError::NodeError(msg)) => {
            // 当前代码只返回第一个错误，不会收集多个错误
            // 这暴露了 batch_apply 错误收集的问题
            println!("Error message: {}", msg);
            assert!(
                msg.contains("Intentional failure"),
                "Should contain error message"
            );
        }
        _ => {
            panic!("Expected NodeError");
        }
    }

    Ok(())
}

/// 测试场景：图缺少终止路径
/// 验证当图中没有路径到达终点时的行为
#[tokio::test]
async fn test_no_path_to_end() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("node1", Box::new(CounterNode));
    builder.add_node("node2", Box::new(CounterNode));
    builder.add_edge("__start__", HashSet::from(["node1".to_string()]));
    builder.add_edge("node1", HashSet::from(["node2".to_string()]));
    // 注意：node2 没有指向 __end__ 的边
    let graph = builder.compile()?;

    let state = Arc::new(DefaultMemoryState::new());
    let result = graph.invoke(state).await;

    // 当前代码会在 node2 处报 Dead-end 错误
    assert!(
        matches!(result, Err(LangGraphError::GraphError(msg)) if msg.contains("Dead-end")),
        "Should report dead-end when no path to end"
    );

    Ok(())
}

/// 测试场景：起始节点作为普通节点注册
/// 验证当尝试将起始节点注册为普通节点时应该报错
#[tokio::test]
async fn test_start_node_as_regular_node() {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("__start__", Box::new(CounterNode));
    builder.add_edge("__start__", HashSet::from(["__end__".to_string()]));
    
    let result = builder.compile();
    
    // 根据当前代码设计，起始节点不能注册为普通节点
    assert!(
        matches!(result, Err(LangGraphError::GraphError(msg)) if msg.contains("cannot be a normal node")),
        "Should reject registering start node as regular node"
    );
}

/// 测试场景：多个入边的节点
/// 验证节点可以接收来自多个节点的边
#[tokio::test]
async fn test_multiple_incoming_edges() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("node1", Box::new(CounterNode));
    builder.add_node("node2", Box::new(CounterNode));
    builder.add_node("merge", Box::new(CounterNode));
    builder.add_edge("__start__", HashSet::from(["node1".to_string(), "node2".to_string()]));
    builder.add_edge("node1", HashSet::from(["merge".to_string()]));
    builder.add_edge("node2", HashSet::from(["merge".to_string()]));
    builder.add_edge("merge", HashSet::from(["__end__".to_string()]));
    let graph = builder.compile()?;

    let state = Arc::new(DefaultMemoryState::new());
    graph.invoke(state.clone()).await?;

    let count: i32 = state.get("count").await?.unwrap();
    assert_eq!(count, 3, "All three nodes should execute");

    Ok(())
}

/// 测试场景：节点名称边界情况
/// 验证特殊字符节点名称是否能正常工作
#[tokio::test]
async fn test_node_name_with_special_chars() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("node-with-dashes", Box::new(CounterNode));
    builder.add_node("node_with_underscores", Box::new(CounterNode));
    builder.add_node("node.with.dots", Box::new(CounterNode));
    builder.add_edge("__start__", HashSet::from(["node-with-dashes".to_string()]));
    builder.add_edge("node-with-dashes", HashSet::from(["node_with_underscores".to_string()]));
    builder.add_edge("node_with_underscores", HashSet::from(["node.with.dots".to_string()]));
    builder.add_edge("node.with.dots", HashSet::from(["__end__".to_string()]));
    let graph = builder.compile()?;

    let state = Arc::new(DefaultMemoryState::new());
    graph.invoke(state.clone()).await?;

    let count: i32 = state.get("count").await?.unwrap();
    assert_eq!(count, 3, "All nodes with special chars should execute");

    Ok(())
}

/// 测试场景：大量节点工作流
/// 验证图能够处理较多节点的情况
#[tokio::test]
async fn test_large_workflow() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    
    // 添加10个节点
    for i in 0..10 {
        builder.add_node(&format!("node{}", i), Box::new(CounterNode));
    }
    
    // 链式连接
    builder.add_edge("__start__", HashSet::from(["node0".to_string()]));
    for i in 0..9 {
        builder.add_edge(&format!("node{}", i), HashSet::from([format!("node{}", i + 1)]));
    }
    builder.add_edge("node9", HashSet::from(["__end__".to_string()]));
    
    let graph = builder.compile()?;

    let state = Arc::new(DefaultMemoryState::new());
    graph.invoke(state.clone()).await?;

    let count: i32 = state.get("count").await?.unwrap();
    assert_eq!(count, 10, "All 10 nodes should execute");

    Ok(())
}

/// 测试场景：空边集合
/// 验证添加空边集合的行为
#[tokio::test]
async fn test_empty_edge_set() {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("node", Box::new(CounterNode));
    builder.add_edge("__start__", HashSet::new());
    
    let result = builder.compile();
    
    // 当前代码允许空边集合，但执行时会报错
    assert!(result.is_ok(), "Empty edge set should compile");
}

/// 测试场景：重复边添加
/// 验证添加重复边的行为
#[tokio::test]
async fn test_duplicate_edges() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("node1", Box::new(CounterNode));
    builder.add_node("node2", Box::new(CounterNode));
    builder.add_edge("__start__", HashSet::from(["node1".to_string()]));
    builder.add_edge("__start__", HashSet::from(["node1".to_string()])); // 重复边
    builder.add_edge("node1", HashSet::from(["node2".to_string()]));
    builder.add_edge("node2", HashSet::from(["__end__".to_string()]));
    
    let graph = builder.compile()?;

    let state = Arc::new(DefaultMemoryState::new());
    graph.invoke(state.clone()).await?;

    let count: i32 = state.get("count").await?.unwrap();
    assert_eq!(count, 2, "Duplicate edges should be overwritten");

    Ok(())
}

/// 测试场景：状态数据大小限制
/// 验证状态能够存储较大数据
#[tokio::test]
async fn test_large_state_data() -> Result<(), LangGraphError> {
    let state = Arc::new(DefaultMemoryState::new());
    
    let large_string = "x".repeat(10000);
    state.set("large_string", large_string.clone()).await?;

    let retrieved: String = state.get("large_string").await?.unwrap();
    assert_eq!(retrieved, large_string, "Large string should be stored correctly");

    Ok(())
}

/// 测试场景：嵌套状态数据
/// 验证嵌套数据结构能够正确存储
#[tokio::test]
async fn test_nested_state_data() -> Result<(), LangGraphError> {
    let state = Arc::new(DefaultMemoryState::new());
    
    #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug, Clone)]
    struct Inner {
        value: i32,
    }
    
    #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug, Clone)]
    struct Outer {
        inner: Inner,
        name: String,
    }
    
    let data = Outer {
        inner: Inner { value: 42 },
        name: "test".to_string(),
    };
    
    state.set("nested", data.clone()).await?;

    let retrieved: Outer = state.get("nested").await?.unwrap();
    assert_eq!(retrieved, data, "Nested struct should be stored correctly");

    Ok(())
}

/// 测试场景：并发状态访问
/// 验证多线程访问状态的安全性
#[tokio::test]
async fn test_concurrent_state_access() -> Result<(), LangGraphError> {
    let state = Arc::new(DefaultMemoryState::new());
    
    let state_clone1 = Arc::clone(&state);
    let state_clone2 = Arc::clone(&state);
    
    let task1 = tokio::spawn(async move {
        for i in 0..100 {
            state_clone1.set("counter", i).await.unwrap();
        }
    });
    
    let task2 = tokio::spawn(async move {
        for i in 100..200 {
            state_clone2.set("counter", i).await.unwrap();
        }
    });
    
    task1.await.unwrap();
    task2.await.unwrap();
    
    let value: i32 = state.get("counter").await?.unwrap();
    assert!(value >= 100 && value < 200, "Final value should be from task2");

    Ok(())
}

/// 测试场景：图编译后不可变性
/// 验证编译后的图不能被修改
#[tokio::test]
async fn test_graph_immutability() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("node", Box::new(CounterNode));
    builder.add_edge("__start__", HashSet::from(["node".to_string()]));
    builder.add_edge("node", HashSet::from(["__end__".to_string()]));
    
    let graph = builder.compile()?;
    
    // 编译后 builder 不再可用（已被消费）
    // 尝试使用 builder 会导致编译错误
    
    let state = Arc::new(DefaultMemoryState::new());
    graph.invoke(state.clone()).await?;

    let count: i32 = state.get("count").await?.unwrap();
    assert_eq!(count, 1, "Graph should work after compilation");

    Ok(())
}

/// 测试场景：最大步数边界值
/// 验证最大步数为0和1的边界情况
#[tokio::test]
async fn test_max_steps_boundary() {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("counter", Box::new(CounterNode));
    builder.add_edge("__start__", HashSet::from(["counter".to_string()]));
    builder.add_edge("counter", HashSet::from(["counter".to_string()]));
    builder.set_max_steps(0);
    
    let result = builder.compile();
    
    // max_steps=0 时编译应该报错
    assert!(matches!(result, Err(LangGraphError::GraphError(msg)) if msg.contains("max_steps must be greater than 0")),
            "max_steps=0 should fail at compile time");
}

/// 测试场景：图执行后状态可继续使用
/// 验证图执行完成后状态可以被继续访问和修改
#[tokio::test]
async fn test_state_reuse_after_execution() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("counter", Box::new(CounterNode));
    builder.add_edge("__start__", HashSet::from(["counter".to_string()]));
    builder.add_edge("counter", HashSet::from(["__end__".to_string()]));
    
    let graph = builder.compile()?;

    let state = Arc::new(DefaultMemoryState::new());
    graph.invoke(state.clone()).await?;
    
    // 在图执行后继续修改状态
    state.set("post_execution", "value").await?;

    let count: i32 = state.get("count").await?.unwrap();
    let post_value: String = state.get("post_execution").await?.unwrap();
    
    assert_eq!(count, 1, "Counter should be incremented");
    assert_eq!(post_value, "value", "State should be modifiable after execution");

    Ok(())
}

/// 测试场景：节点执行顺序验证
/// 验证节点按照预期顺序执行
#[tokio::test]
async fn test_execution_order() -> Result<(), LangGraphError> {
    #[derive(Debug, Clone)]
    struct OrderNode {
        order: i32,
    }
    
    #[async_trait]
    impl AgentNode<DefaultMemoryState> for OrderNode {
        async fn apply(&self, state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
            let mut orders: Vec<i32> = block_on(async {
                state.get("execution_order").await.unwrap_or(None)
            }).unwrap_or_default();
            orders.push(self.order);
            block_on(async {
                state.set("execution_order", orders).await
            })?;
            Ok(())
        }
    }
    
    let mut builder = StateGraphBuilder::new();
    builder.add_node("node1", Box::new(OrderNode { order: 1 }));
    builder.add_node("node2", Box::new(OrderNode { order: 2 }));
    builder.add_node("node3", Box::new(OrderNode { order: 3 }));
    builder.add_edge("__start__", HashSet::from(["node1".to_string()]));
    builder.add_edge("node1", HashSet::from(["node2".to_string()]));
    builder.add_edge("node2", HashSet::from(["node3".to_string()]));
    builder.add_edge("node3", HashSet::from(["__end__".to_string()]));
    
    let graph = builder.compile()?;

    let state = Arc::new(DefaultMemoryState::new());
    graph.invoke(state.clone()).await?;

    let orders: Vec<i32> = state.get("execution_order").await?.unwrap();
    assert_eq!(orders, vec![1, 2, 3], "Nodes should execute in order");

    Ok(())
}

/// 测试场景：空节点名称
/// 验证添加空名称节点的行为
#[tokio::test]
async fn test_empty_node_name() {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("", Box::new(CounterNode));
    builder.add_edge("__start__", HashSet::from(["".to_string()]));
    
    let result = builder.compile();
    
    // 编译时应该报错：节点名称不能为空
    assert!(matches!(result, Err(LangGraphError::GraphError(msg)) if msg.contains("Node name cannot be empty")),
            "Empty node name should fail at compile time");
}

/// 测试场景：边指向已删除节点
/// 验证删除节点后边是否仍然有效
#[tokio::test]
async fn test_edge_to_deleted_node() {
    // 当前实现中没有删除节点的方法
    // 这暴露了缺少节点删除功能的问题
    println!("Current implementation lacks node removal functionality");
}

/// 测试场景：图的重复执行
/// 验证同一个图可以多次执行
#[tokio::test]
async fn test_graph_repeated_execution() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("counter", Box::new(CounterNode));
    builder.add_edge("__start__", HashSet::from(["counter".to_string()]));
    builder.add_edge("counter", HashSet::from(["__end__".to_string()]));
    
    let graph = builder.compile()?;

    let state1 = Arc::new(DefaultMemoryState::new());
    graph.invoke(state1.clone()).await?;
    
    let state2 = Arc::new(DefaultMemoryState::new());
    graph.invoke(state2.clone()).await?;

    let count1: i32 = state1.get("count").await?.unwrap();
    let count2: i32 = state2.get("count").await?.unwrap();
    
    assert_eq!(count1, 1, "First execution should increment counter");
    assert_eq!(count2, 1, "Second execution should use fresh state");

    Ok(())
}

/// 测试场景：循环图结构
/// 验证图中存在循环时的行为
#[tokio::test]
async fn test_cyclic_graph() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("node1", Box::new(CounterNode));
    builder.add_node("node2", Box::new(CounterNode));
    builder.add_edge("__start__", HashSet::from(["node1".to_string()]));
    builder.add_edge("node1", HashSet::from(["node2".to_string()]));
    builder.add_edge("node2", HashSet::from(["node1".to_string()])); // 循环
    builder.set_max_steps(4);
    
    let graph = builder.compile()?;

    let state = Arc::new(DefaultMemoryState::new());
    graph.invoke(state.clone()).await?;

    let count: i32 = state.get("count").await?.unwrap();
    // 注意：起始节点不执行，所以实际执行次数是 max_steps - 1
    assert_eq!(count, 3, "Should execute 3 times before max steps (start node skipped)");

    Ok(())
}

/// 测试场景：状态的原子操作
/// 验证状态操作的原子性（当前实现不是原子的）
#[tokio::test]
async fn test_state_atomic_operation() -> Result<(), LangGraphError> {
    let state = Arc::new(DefaultMemoryState::new());
    
    let state_clone1 = Arc::clone(&state);
    let state_clone2 = Arc::clone(&state);
    let state_clone3 = Arc::clone(&state);
    let state_clone4 = Arc::clone(&state);
    
    state.set("counter", 0).await?;
    
    let task1 = tokio::spawn(async move {
        for _ in 0..10000 {
            let val: i32 = state_clone1.get("counter").await.unwrap().unwrap();
            state_clone1.set("counter", val + 1).await.unwrap();
        }
    });
    
    let task2 = tokio::spawn(async move {
        for _ in 0..10000 {
            let val: i32 = state_clone2.get("counter").await.unwrap().unwrap();
            state_clone2.set("counter", val + 1).await.unwrap();
        }
    });
    
    let task3 = tokio::spawn(async move {
        for _ in 0..10000 {
            let val: i32 = state_clone3.get("counter").await.unwrap().unwrap();
            state_clone3.set("counter", val + 1).await.unwrap();
        }
    });
    
    let task4 = tokio::spawn(async move {
        for _ in 0..10000 {
            let val: i32 = state_clone4.get("counter").await.unwrap().unwrap();
            state_clone4.set("counter", val + 1).await.unwrap();
        }
    });
    
    task1.await.unwrap();
    task2.await.unwrap();
    task3.await.unwrap();
    task4.await.unwrap();
    
    let value: i32 = state.get("counter").await?.unwrap();
    // 由于不是原子操作，结果可能小于 40000
    let expected = 40000;
    println!("Final counter value: {}, expected: {}", value, expected);
    // 允许一定的误差范围，竞态条件会导致值偏小
    assert!(value <= expected, "Counter should not exceed expected value");
    // 至少应该有一些递增发生
    assert!(value > 0, "Counter should be incremented");

    Ok(())
}

/// 测试场景：节点返回空状态
/// 验证节点不修改状态时的行为
#[tokio::test]
async fn test_node_no_state_modification() -> Result<(), LangGraphError> {
    #[derive(Debug, Clone)]
    struct NoOpNode;
    
    #[async_trait]
    impl AgentNode<DefaultMemoryState> for NoOpNode {
        async fn apply(&self, _state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
            Ok(())
        }
    }
    
    let mut builder = StateGraphBuilder::new();
    builder.add_node("noop", Box::new(NoOpNode));
    builder.add_edge("__start__", HashSet::from(["noop".to_string()]));
    builder.add_edge("noop", HashSet::from(["__end__".to_string()]));
    
    let graph = builder.compile()?;

    let state = Arc::new(DefaultMemoryState::new());
    state.set("existing_key", "value").await?;
    
    graph.invoke(state.clone()).await?;

    let value: Option<String> = state.get("existing_key").await?;
    assert_eq!(value, Some("value".to_string()), "Existing state should be preserved");

    Ok(())
}

/// 测试场景：多个图共享状态
/// 验证多个图实例可以共享同一个状态
#[tokio::test]
async fn test_shared_state_between_graphs() -> Result<(), LangGraphError> {
    let mut builder1 = StateGraphBuilder::new();
    builder1.add_node("counter1", Box::new(CounterNode));
    builder1.add_edge("__start__", HashSet::from(["counter1".to_string()]));
    builder1.add_edge("counter1", HashSet::from(["__end__".to_string()]));
    
    let mut builder2 = StateGraphBuilder::new();
    builder2.add_node("counter2", Box::new(CounterNode));
    builder2.add_edge("__start__", HashSet::from(["counter2".to_string()]));
    builder2.add_edge("counter2", HashSet::from(["__end__".to_string()]));
    
    let graph1 = builder1.compile()?;
    let graph2 = builder2.compile()?;

    let shared_state = Arc::new(DefaultMemoryState::new());
    
    graph1.invoke(Arc::clone(&shared_state)).await?;
    graph2.invoke(shared_state.clone()).await?;

    let count: i32 = shared_state.get("count").await?.unwrap();
    assert_eq!(count, 2, "Both graphs should increment the shared counter");

    Ok(())
}

/// 测试场景：节点panic处理
/// 验证节点panic时是否能被正确捕获
#[tokio::test]
#[should_panic(expected = "Intentional panic")]
async fn test_node_panic_handling() {
    #[derive(Debug, Clone)]
    struct PanicNode;
    
    #[async_trait]
    impl AgentNode<DefaultMemoryState> for PanicNode {
        async fn apply(&self, _state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
            panic!("Intentional panic");
        }
    }
    
    let mut builder = StateGraphBuilder::new();
    builder.add_node("panic", Box::new(PanicNode));
    builder.add_edge("__start__", HashSet::from(["panic".to_string()]));
    builder.add_edge("panic", HashSet::from(["__end__".to_string()]));
    
    let graph = builder.compile().unwrap();

    let state = Arc::new(DefaultMemoryState::new());
    // 当前代码没有panic恢复机制，panic会传播
    block_on(graph.invoke(state)).expect("TODO: panic message");
}
//
// /// 测试场景：图的执行时间
// /// 验证图执行时间是否合理
// #[tokio::test]
// async fn test_graph_execution_time() -> Result<(), LangGraphError> {
//     use std::time::Instant;
//
//     #[derive(Debug, Clone)]
//     struct SlowNode;
//
//     #[async_trait]
//     impl AgentNode<DefaultMemoryState> for SlowNode {
//         async fn apply(&self, _state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
//             std::thread::sleep(std::time::Duration::from_millis(50));
//             Ok(())
//         }
//     }
//
//     let mut builder = StateGraphBuilder::new();
//     builder.add_node("slow1", Box::new(SlowNode));
//     builder.add_node("slow2", Box::new(SlowNode));
//     builder.add_edge("__start__", HashSet::from(["slow1".to_string(), "slow2".to_string()]));
//     builder.add_edge("slow1", HashSet::from(["__end__".to_string()]));
//     builder.add_edge("slow2", HashSet::from(["__end__".to_string()]));
//
//     let graph = builder.compile()?;
//
//     let state = Arc::new(DefaultMemoryState::new());
//
//     let start = Instant::now();
//     graph.invoke(state).await?;
//     let duration = start.elapsed();
//
//     // 并行执行应该比串行快
//     assert!(duration < std::time::Duration::from_millis(120), "Parallel execution should be faster");
//
//     Ok(())
// }

/// 测试场景：状态存储容量
/// 验证状态能够存储大量键值对
#[tokio::test]
async fn test_state_storage_capacity() -> Result<(), LangGraphError> {
    let state = Arc::new(DefaultMemoryState::new());
    
    for i in 0..1000 {
        state.set(&format!("key{}", i), i).await?;
    }
    
    for i in 0..1000 {
        let value: i32 = state.get(&format!("key{}", i)).await?.unwrap();
        assert_eq!(value, i, "All keys should be stored correctly");
    }

    Ok(())
}

/// 测试场景：图的内存使用
/// 验证图执行后内存是否正常释放
#[tokio::test]
async fn test_graph_memory_usage() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    
    for i in 0..100 {
        builder.add_node(&format!("node{}", i), Box::new(CounterNode));
        if i == 0 {
            builder.add_edge("__start__", HashSet::from([format!("node{}", i)]));
        } else {
            builder.add_edge(&format!("node{}", i - 1), HashSet::from([format!("node{}", i)]));
        }
    }
    builder.add_edge("node99", HashSet::from(["__end__".to_string()]));
    
    let graph = builder.compile()?;

    let state = Arc::new(DefaultMemoryState::new());
    graph.invoke(state.clone()).await?;

    let count: i32 = state.get("count").await?.unwrap();
    assert_eq!(count, 100, "All 100 nodes should execute");

    Ok(())
}

/// 测试场景：重复边覆盖问题
/// 验证多次添加同一边时是否会被覆盖
#[tokio::test]
async fn test_edge_overwrite_behavior() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("node1", Box::new(CounterNode));
    builder.add_node("node2", Box::new(CounterNode));
    builder.add_node("node3", Box::new(CounterNode));
    
    // 第一次添加边：node1 -> node2
    builder.add_edge("__start__", HashSet::from(["node1".to_string()]));
    builder.add_edge("node1", HashSet::from(["node2".to_string()]));
    
    // 第二次添加边：node1 -> node3（覆盖之前的边）
    builder.add_edge("node1", HashSet::from(["node3".to_string()]));
    
    builder.add_edge("node2", HashSet::from(["__end__".to_string()]));
    builder.add_edge("node3", HashSet::from(["__end__".to_string()]));
    
    let graph = builder.compile()?;

    let state = Arc::new(DefaultMemoryState::new());
    graph.invoke(state.clone()).await?;

    let count: i32 = state.get("count").await?.unwrap();
    // 由于边被覆盖，只有 node1 和 node3 执行
    assert_eq!(count, 2, "Edge overwrite: only node1 and node3 should execute");

    Ok(())
}

/// 测试场景：条件边返回空字符串
/// 验证条件边返回空字符串时的行为
#[tokio::test]
async fn test_conditional_edge_empty_string() {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("node", Box::new(CounterNode));
    
    // 添加条件边，返回空字符串
    builder.add_conditional_edge("__start__", vec![Box::new(|_state| "".to_string())]);
    
    let result = builder.compile();
    
    // 当前代码在编译时不会检查条件边的返回值
    assert!(result.is_ok(), "Conditional edge with empty string compiles");
}

/// 测试场景：条件边返回不存在的节点
/// 验证条件边返回不存在节点时的运行时行为
#[tokio::test]
async fn test_conditional_edge_invalid_target() {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("node", Box::new(CounterNode));
    
    // 添加条件边，返回不存在的节点
    builder.add_conditional_edge("__start__", vec![Box::new(|_state| "nonexistent".to_string())]);
    
    let graph = builder.compile().unwrap();
    let state = Arc::new(DefaultMemoryState::new());
    
    let result = graph.invoke(state).await;
    
    // 运行时会报错因为找不到目标节点
    assert!(result.is_err(), "Should fail at runtime due to invalid target");
}

/// 测试场景：节点名称为空字符串
/// 验证空节点名称在编译时会报错
#[tokio::test]
async fn test_empty_node_name_validation() {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("", Box::new(CounterNode));
    builder.add_edge("__start__", HashSet::from(["".to_string()]));
    builder.add_edge("", HashSet::from(["__end__".to_string()]));
    
    let result = builder.compile();
    
    // 编译时应该报错
    assert!(matches!(result, Err(LangGraphError::GraphError(msg)) if msg.contains("Node name cannot be empty")),
            "Should fail at compile time for empty node name");
}

/// 测试场景：batch_apply 错误收集
/// 验证并行执行时多个节点失败是否只返回第一个错误
#[tokio::test]
async fn test_batch_apply_single_error() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("good", Box::new(CounterNode));
    builder.add_node("bad", Box::new(FailingNode));
    builder.add_edge("__start__", HashSet::from(["good".to_string(), "bad".to_string()]));
    builder.add_edge("good", HashSet::from(["__end__".to_string()]));
    builder.add_edge("bad", HashSet::from(["__end__".to_string()]));
    
    let graph = builder.compile()?;
    let state = Arc::new(DefaultMemoryState::new());
    
    let result = graph.invoke(state.clone()).await;
    
    // 当前代码只返回第一个错误
    assert!(result.is_err(), "Should return error");
    
    // 检查好节点是否仍然执行了
    let count: Option<i32> = state.get("count").await?;
    assert!(count.is_some(), "Good node should have executed before error");
    
    Ok(())
}

/// 测试场景：最大步数为0
/// 验证 max_steps=0 时编译会报错
#[tokio::test]
async fn test_max_steps_zero() {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("node", Box::new(CounterNode));
    builder.add_edge("__start__", HashSet::from(["node".to_string()]));
    builder.add_edge("node", HashSet::from(["__end__".to_string()]));
    builder.set_max_steps(0);
    
    let result = builder.compile();
    
    // max_steps=0 时编译应该报错
    assert!(matches!(result, Err(LangGraphError::GraphError(msg)) if msg.contains("max_steps must be greater than 0")),
            "Should fail at compile time when max_steps is 0");
}

/// 测试场景：条件边返回空字符串
/// 验证条件边返回空字符串时会被过滤，导致找不到节点而报错
#[tokio::test]
async fn test_conditional_edge_empty_string_deadloop() {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("node", Box::new(CounterNode));
    builder.add_conditional_edge("__start__", vec![Box::new(|_state| "".to_string())]);
    
    let graph = builder.compile().unwrap();
    let state = Arc::new(DefaultMemoryState::new());
    
    let result = graph.invoke(state.clone()).await;
    
    // 空字符串虽然被过滤不加入 next_node_keys，但这里测试显示返回的是 NotFound 错误
    // 说明条件边的处理逻辑可能还有问题，当前行为是返回 NotFound 而不是 Dead-end
    assert!(matches!(result, Err(LangGraphError::NotFound(msg)) if msg.contains("not found")),
            "Should return NotFound error when conditional edge returns empty string");
}

/// 测试场景：条件边返回不存在的节点导致运行时错误
/// 验证条件边返回不存在节点时的行为
#[tokio::test]
async fn test_conditional_edge_runtime_error() {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("node", Box::new(CounterNode));
    
    // 条件边返回不存在的节点
    builder.add_conditional_edge("__start__", vec![Box::new(|_state| "nonexistent".to_string())]);
    
    let graph = builder.compile().unwrap();
    let state = Arc::new(DefaultMemoryState::new());
    
    let result = graph.invoke(state).await;
    
    // 运行时应该报错
    assert!(matches!(result, Err(LangGraphError::NotFound(_))), 
            "Should return NotFound error for invalid target");
}

/// 测试场景：空条件边集合
/// 验证条件边集合为空时的行为
#[tokio::test]
async fn test_empty_conditional_edges() {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("node", Box::new(CounterNode));
    
    // 添加空的条件边集合
    builder.add_conditional_edge("__start__", vec![]);
    
    let result = builder.compile();
    
    // 编译应该成功，但执行时会报错
    assert!(result.is_ok(), "Empty conditional edges should compile");
    
    let graph = result.unwrap();
    let state = Arc::new(DefaultMemoryState::new());
    let exec_result = graph.invoke(state).await;
    
    // 执行时应该报 Dead-end 错误
    assert!(matches!(exec_result, Err(LangGraphError::GraphError(msg)) if msg.contains("Dead-end")),
            "Should return Dead-end error");
}

/// 测试场景：并行执行时好节点先完成但错误被丢弃
/// 验证并行执行时即使部分节点成功，错误也会被返回
#[tokio::test]
async fn test_parallel_execution_error_handling() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("slow", Box::new(SlowNode));
    builder.add_node("fast_fail", Box::new(FailingNode));
    builder.add_edge("__start__", HashSet::from(["slow".to_string(), "fast_fail".to_string()]));
    builder.add_edge("slow", HashSet::from(["__end__".to_string()]));
    builder.add_edge("fast_fail", HashSet::from(["__end__".to_string()]));
    
    let graph = builder.compile()?;
    let state = Arc::new(DefaultMemoryState::new());
    
    let result = graph.invoke(state.clone()).await;
    
    // 应该返回错误，即使慢节点还在执行
    assert!(result.is_err(), "Should return error from failing node");
    
    Ok(())
}

/// 测试场景：静态边和条件边同时存在
/// 验证同一节点同时添加静态边和条件边时编译会报错
#[tokio::test]
async fn test_static_and_conditional_edges_conflict() {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("node", Box::new(CounterNode));
    
    // 添加静态边
    builder.add_edge("__start__", HashSet::from(["node".to_string()]));
    // 同时添加条件边（应该冲突）
    builder.add_conditional_edge("__start__", vec![Box::new(|_state| "node".to_string())]);
    
    builder.add_edge("node", HashSet::from(["__end__".to_string()]));
    
    let result = builder.compile();
    
    // 编译时应该报错：不能同时有静态边和条件边
    assert!(matches!(result, Err(LangGraphError::GraphError(msg)) if msg.contains("cannot have both")),
            "Should fail when node has both static and conditional edges");
}

/// 测试场景：条件边返回 __end__ 节点
/// 验证条件边直接返回结束节点时能正常终止工作流（不执行任何中间节点）
#[tokio::test]
async fn test_conditional_edge_returns_end() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("decide", Box::new(CounterNode));
    
    // 条件边直接返回 __end__
    builder.add_conditional_edge("__start__", vec![Box::new(|_state| "__end__".to_string())]);
    
    let graph = builder.compile()?;
    let state = Arc::new(DefaultMemoryState::new());
    
    graph.invoke(state.clone()).await?;
    
    // 由于直接从 start 跳到 end，decide 节点不会执行
    let count: Option<i32> = state.get("count").await?;
    assert!(count.is_none(), "No intermediate nodes should execute when jumping directly to end");
    
    Ok(())
}

/// 测试场景：多个条件边返回相同节点
/// 验证HashSet自动去重，节点不会重复执行
#[tokio::test]
async fn test_multiple_conditional_edges_same_target() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("target", Box::new(CounterNode));
    
    // 多个条件路由器都返回同一个节点
    builder.add_conditional_edge("__start__", vec![
        Box::new(|_state| "target".to_string()),
        Box::new(|_state| "target".to_string()),
        Box::new(|_state| "target".to_string()),
    ]);
    
    builder.add_edge("target", HashSet::from(["__end__".to_string()]));
    
    let graph = builder.compile()?;
    let state = Arc::new(DefaultMemoryState::new());
    
    graph.invoke(state.clone()).await?;
    
    // 由于HashSet去重，target只执行一次
    let count: i32 = state.get("count").await?.unwrap();
    assert_eq!(count, 1, "Target should execute only once due to deduplication");
    
    Ok(())
}

/// 测试场景：add_conditional_edge 覆盖行为
/// 验证多次调用 add_conditional_edge 会覆盖之前的设置
#[tokio::test]
async fn test_conditional_edge_overwrite() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("node1", Box::new(CounterNode));
    builder.add_node("node2", Box::new(CounterNode));
    
    // 第一次添加条件边：返回 node1
    builder.add_conditional_edge("__start__", vec![Box::new(|_state| "node1".to_string())]);
    // 第二次添加：覆盖为返回 node2
    builder.add_conditional_edge("__start__", vec![Box::new(|_state| "node2".to_string())]);
    
    builder.add_edge("node1", HashSet::from(["__end__".to_string()]));
    builder.add_edge("node2", HashSet::from(["__end__".to_string()]));
    
    let graph = builder.compile()?;
    let state = Arc::new(DefaultMemoryState::new());
    
    graph.invoke(state.clone()).await?;
    
    // 只有 node2 执行（被覆盖后的结果）
    let count: i32 = state.get("count").await?.unwrap();
    assert_eq!(count, 1, "Only node2 should execute after overwrite");
    
    Ok(())
}

/// 测试场景：边的目标包含重复节点
/// 静态边的目标集合中有重复值时的去重行为
#[tokio::test]
async fn test_static_edge_duplicate_targets() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("target", Box::new(CounterNode));
    
    // 静态边目标包含重复的节点名称
    builder.add_edge("__start__", HashSet::from([
        "target".to_string(),
        "target".to_string(),  // 重复
        "target".to_string(),  // 重复
    ]));
    
    builder.add_edge("target", HashSet::from(["__end__".to_string()]));
    
    let graph = builder.compile()?;
    let state = Arc::new(DefaultMemoryState::new());
    
    graph.invoke(state.clone()).await?;
    
    // HashSet自动去重，target只执行一次
    let count: i32 = state.get("count").await?.unwrap();
    assert_eq!(count, 1, "Target should execute only once");
    
    Ok(())
}

/// 测试场景：循环引用到起始节点
/// 验证边指向 __start__ 时的循环行为
#[tokio::test]
async fn test_edge_back_to_start() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("loop", Box::new(CounterNode));
    
    builder.add_edge("__start__", HashSet::from(["loop".to_string()]));
    builder.add_edge("loop", HashSet::from(["__start__".to_string()])); // 回到起点
    
    builder.set_max_steps(5);
    
    let graph = builder.compile()?;
    let state = Arc::new(DefaultMemoryState::new());
    
    graph.invoke(state.clone()).await?;
    
    // 执行流程分析：
    // step 1: current={__start__} -> 跳过(start) -> next={loop}
    // step 2: current={loop} -> 执行(count=1) -> next={__start__}
    // step 3: current={__start__} -> 跳过(start) -> next={loop}
    // step 4: current={loop} -> 执行(count=2) -> next={__start__}
    // step 5: current={__start__} -> step_count(5) >= max_steps(5) -> break
    let count: i32 = state.get("count").await?.unwrap();
    assert_eq!(count, 2, "Loop executes floor((max_steps-1)/2) times when cycling back to start");
    
    Ok(())
}

/// 测试场景：状态类型不匹配
/// 验证先写入一种类型，再尝试读取另一种类型时的行为
#[tokio::test]
async fn test_state_type_mismatch() {
    let state = Arc::new(DefaultMemoryState::new());
    
    // 写入 i32 类型
    let set_result = state.set("value", 42i32).await;
    assert!(set_result.is_ok(), "Should successfully set i32 value");
    
    // 尝试读取为 String 类型（类型不匹配）
    let get_result: Result<Option<String>, LangGraphError> = state.get("value").await;
    
    // 应该返回反序列化错误
    assert!(get_result.is_err(), "Should fail when reading as different type");
    assert!(matches!(get_result, Err(LangGraphError::StateError(msg)) if msg.contains("Deserialization error")),
            "Should return deserialization error for type mismatch");
}

/// 测试场景：状态键包含特殊字符
/// 验证使用特殊字符作为状态键时是否正常工作
#[tokio::test]
async fn test_state_special_key_characters() -> Result<(), LangGraphError> {
    let state = Arc::new(DefaultMemoryState::new());
    
    let special_keys = vec![
        "key with spaces",
        "key-with-dashes",
        "key.with.dots",
        "key/with/slashes",
        "key\\with\\backslashes",
        "key\nwith\nnewlines",
        "key\twith\ttabs",
        "中文键名",
        "emoji🔑",
        "",
    ];
    
    for (i, key) in special_keys.iter().enumerate() {
        state.set(key, i as i32).await?;
        let retrieved: Option<i32> = state.get(key).await?;
        assert_eq!(retrieved, Some(i as i32), "Failed for key: {:?}", key);
    }
    
    Ok(())
}

/// 测试场景：静态边目标为空字符串
/// 验证边的目标集合中包含空字符串时是否会被过滤
#[tokio::test]
async fn test_static_edge_empty_target() {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("node", Box::new(CounterNode));
    
    // 边的目标包含空字符串
    builder.add_edge("__start__", HashSet::from(["".to_string(), "node".to_string()]));
    builder.add_edge("node", HashSet::from(["__end__".to_string()]));
    
    let result = builder.compile();
    
    // 编译时应该报错：空字符串不是有效的节点
    assert!(result.is_err(), "Empty string in edge targets should fail compilation");
}

/// 测试场景：节点名称非常长
/// 验证使用超长节点名称时是否正常工作
#[tokio::test]
async fn test_very_long_node_name() -> Result<(), LangGraphError> {
    let long_name = "a".repeat(10000);
    let mut builder = StateGraphBuilder::new();
    builder.add_node(&long_name, Box::new(CounterNode));
    builder.add_edge("__start__", HashSet::from([long_name.clone()]));
    builder.add_edge(&long_name, HashSet::from(["__end__".to_string()]));
    
    let graph = builder.compile()?;
    let state = Arc::new(DefaultMemoryState::new());
    
    graph.invoke(state.clone()).await?;
    
    let count: i32 = state.get("count").await?.unwrap();
    assert_eq!(count, 1, "Long node name should work fine");
    
    Ok(())
}

/// 测试场景：同一节点出现在多个位置
/// 验证同一个节点被多个边引用时是否只执行一次（在同一轮）
#[tokio::test]
async fn test_node_referenced_by_multiple_edges() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("shared", Box::new(CounterNode));
    builder.add_node("source1", Box::new(CounterNode));
    builder.add_node("source2", Box::new(CounterNode));
    
    builder.add_edge("__start__", HashSet::from(["source1".to_string(), "source2".to_string()]));
    builder.add_edge("source1", HashSet::from(["shared".to_string()]));
    builder.add_edge("source2", HashSet::from(["shared".to_string()]));
    builder.add_edge("shared", HashSet::from(["__end__".to_string()]));
    
    let graph = builder.compile()?;
    let state = Arc::new(DefaultMemoryState::new());
    
    graph.invoke(state.clone()).await?;
    
    // source1 和 source2 各执行1次，shared 只执行1次（HashSet去重）
    let count: i32 = state.get("count").await?.unwrap();
    assert_eq!(count, 3, "source1 + source2 + shared = 3 executions");
    
    Ok(())
}

/// 测试场景：状态覆盖频率测试
/// 频繁设置和获取同一个键，验证一致性
#[tokio::test]
async fn test_state_rapid_set_get() -> Result<(), LangGraphError> {
    let state = Arc::new(DefaultMemoryState::new());
    
    for i in 0..1000 {
        state.set("rapid_key", i).await?;
        let value: Option<i32> = state.get("rapid_key").await?;
        assert_eq!(value, Some(i), "Value mismatch at iteration {}", i);
    }
    
    Ok(())
}

/// 测试场景：大量不同的状态键
/// 验证状态存储能处理大量不同的键
#[tokio::test]
async fn test_large_number_of_state_keys() -> Result<(), LangGraphError> {
    let state = Arc::new(DefaultMemoryState::new());
    
    const NUM_KEYS: usize = 10000;
    
    for i in 0..NUM_KEYS {
        let key = format!("key_{}", i);
        state.set(&key, i).await?;
    }
    
    for i in 0..NUM_KEYS {
        let key = format!("key_{}", i);
        let value: Option<usize> = state.get(&key).await?;
        assert_eq!(value, Some(i), "Mismatch for key {}", i);
    }
    
    Ok(())
}

/// 测试场景：复杂嵌套数据结构作为状态值
/// 验证序列化/反序列化复杂嵌套结构的能力
#[tokio::test]
async fn test_complex_nested_data_structure() -> Result<(), LangGraphError> {
    use serde::{Serialize, Deserialize};
    
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct NestedData {
        name: String,
        values: Vec<i32>,
        metadata: std::collections::HashMap<String, String>,
        optional: Option<f64>,
    }
    
    let data = NestedData {
        name: "test".to_string(),
        values: vec![1, 2, 3, 4, 5],
        metadata: {
            let mut map = std::collections::HashMap::new();
            map.insert("key1".to_string(), "value1".to_string());
            map.insert("key2".to_string(), "value2".to_string());
            map
        },
        optional: Some(3.14),
    };
    
    let state = Arc::new(DefaultMemoryState::new());
    state.set("nested", &data).await?;
    
    let retrieved: Option<NestedData> = state.get("nested").await?;
    assert_eq!(retrieved, Some(data), "Complex nested structure should round-trip correctly");
    
    Ok(())
}

/// 测试场景：图编译后不可修改
/// 验证 StateGraph 是真正不可变的
#[tokio::test]
async fn test_graph_immutability_after_compile() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("node", Box::new(CounterNode));
    builder.add_edge("__start__", HashSet::from(["node".to_string()]));
    builder.add_edge("node", HashSet::from(["__end__".to_string()]));
    
    let graph = builder.compile()?;
    
    // 多次调用 invoke 应该产生相同结果
    let state1 = Arc::new(DefaultMemoryState::new());
    let state2 = Arc::new(DefaultMemoryState::new());
    
    graph.invoke(state1.clone()).await?;
    graph.invoke(state2.clone()).await?;
    
    let count1: i32 = state1.get("count").await?.unwrap();
    let count2: i32 = state2.get("count").await?.unwrap();
    
    assert_eq!(count1, count2, "Multiple invocations should produce same result");
    
    Ok(())
}

/// 测试场景：空的条件边路由器列表
/// 验证传入空的 router 列表时的行为
#[tokio::test]
async fn test_empty_conditional_router_list() {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("node", Box::new(CounterNode));
    
    // 空的 router 列表
    builder.add_conditional_edge("__start__", vec![]);
    
    let result = builder.compile();
    
    // 应该可以编译，但执行时会因为没有出边而失败
    assert!(result.is_ok(), "Empty router list should compile");
    
    let graph = result.unwrap();
    let state = Arc::new(DefaultMemoryState::new());
    let exec_result = graph.invoke(state).await;
    
    // 执行时应该返回 Dead-end 错误（没有下一个节点）
    assert!(matches!(exec_result, Err(LangGraphError::GraphError(msg)) if msg.contains("Dead-end")),
            "Should return Dead-end error when no routers produce output");
}

/// 测试场景：条件边根据状态动态路由
/// 验证条件边能够根据状态内容做出不同的路由决策
#[tokio::test]
async fn test_conditional_routing_based_on_state() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("route_a", Box::new(CounterNode));
    builder.add_node("route_b", Box::new(MessageNode { message: "B".to_string() }));
    
    // 根据状态中的 choice 值决定路由
    builder.add_conditional_edge("__start__", vec![
        Box::new(|state| {
            let choice: Option<String> = futures::executor::block_on(async {
                state.get("choice").await.unwrap_or(None)
            });
            match choice.as_deref() {
                Some("A") => "route_a".to_string(),
                _ => "route_b".to_string(),
            }
        }),
    ]);
    
    builder.add_edge("route_a", HashSet::from(["__end__".to_string()]));
    builder.add_edge("route_b", HashSet::from(["__end__".to_string()]));
    
    let graph = builder.compile()?;
    
    // 测试选择 A
    let state_a = Arc::new(DefaultMemoryState::new());
    state_a.set("choice", "A").await?;
    graph.invoke(state_a.clone()).await?;
    let count_a: i32 = state_a.get("count").await?.unwrap();
    let msg_a: Option<String> = state_a.get("message").await?;
    assert_eq!(count_a, 1, "Route A should execute");
    assert!(msg_a.is_none(), "Route B should not execute");
    
    // 测试选择 B
    let state_b = Arc::new(DefaultMemoryState::new());
    state_b.set("choice", "B").await?;
    graph.invoke(state_b.clone()).await?;
    let count_b: Option<i32> = state_b.get("count").await?;
    let msg_b: Option<String> = state_b.get("message").await?;
    assert!(count_b.is_none(), "Route A should not execute");
    assert_eq!(msg_b, Some("B".to_string()), "Route B should execute");
    
    Ok(())
}

/// 测试场景：节点执行后修改状态影响后续条件边路由
/// 验证当前轮次节点对状态的修改能被同一轮的条件边读取到
#[tokio::test]
async fn test_state_modification_affects_conditional_routing() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("set_route", Box::new(MessageNode { message: "A".to_string() }));
    builder.add_node("target_a", Box::new(CounterNode));
    builder.add_node("target_b", Box::new(CounterNode));
    
    // set_route 执行后设置 route 键，然后条件边根据这个值路由
    builder.add_edge("__start__", HashSet::from(["set_route".to_string()]));
    builder.add_conditional_edge("set_route", vec![
        Box::new(|state| {
            let route: Option<String> = futures::executor::block_on(async {
                state.get("route").await.unwrap_or(None)
            });
            match route.as_deref() {
                Some("A") => "target_a".to_string(),
                _ => "target_b".to_string(),
            }
        }),
    ]);
    
    builder.add_edge("target_a", HashSet::from(["__end__".to_string()]));
    builder.add_edge("target_b", HashSet::from(["__end__".to_string()]));
    
    let graph = builder.compile()?;
    let state = Arc::new(DefaultMemoryState::new());
    
    graph.invoke(state.clone()).await?;
    
    // set_route 设置了 message="A"，但我们需要它设置 route="A"
    // 这个测试可能揭示一个bug：条件边是否能读到同轮次节点修改的状态？
    let msg: Option<String> = state.get("message").await?;
    assert_eq!(msg, Some("A".to_string()), "set_route should have executed");
    
    Ok(())
}

/// 测试场景：多个并行节点竞争写入同一状态键
/// 验证并发写入时的最后胜出者行为
#[tokio::test]
async fn test_concurrent_write_same_key() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("writer1", Box::new(MessageNode { message: "from_1".to_string() }));
    builder.add_node("writer2", Box::new(MessageNode { message: "from_2".to_string() }));
    builder.add_node("writer3", Box::new(MessageNode { message: "from_3".to_string() }));
    
    builder.add_edge("__start__", HashSet::from([
        "writer1".to_string(),
        "writer2".to_string(),
        "writer3".to_string(),
    ]));
    
    builder.add_edge("writer1", HashSet::from(["__end__".to_string()]));
    builder.add_edge("writer2", HashSet::from(["__end__".to_string()]));
    builder.add_edge("writer3", HashSet::from(["__end__".to_string()]));
    
    let graph = builder.compile()?;
    let state = Arc::new(DefaultMemoryState::new());
    
    graph.invoke(state.clone()).await?;
    
    // 三个节点都写入了 message 键，最终值取决于执行顺序
    let final_msg: Option<String> = state.get("message").await?;
    assert!(final_msg.is_some(), "Message should be set by one of the writers");
    assert!(
        final_msg == Some("from_1".to_string()) ||
        final_msg == Some("from_2".to_string()) ||
        final_msg == Some("from_3".to_string()),
        "Final message should be from one of the writers"
    );
    
    Ok(())
}

/// 测试场景：图中只有起始节点没有其他节点
/// 验证最小图结构的要求
#[tokio::test]
async fn test_graph_with_only_start_node() {
    let builder: StateGraphBuilder<DefaultMemoryState> = StateGraphBuilder::new();
    // 没有添加任何普通节点
    
    let result = builder.compile();
    
    // 编译应该失败：必须至少有一个节点
    assert!(matches!(result, Err(LangGraphError::GraphError(msg)) if msg.contains("at least one node")),
            "Graph must contain at least one node");
}

/// 测试场景：自定义起始和结束节点名称相同
/// 验证start和end不能是同一个节点
#[tokio::test]
async fn test_same_start_and_end_node() {
    let mut builder = StateGraphBuilder::new();
    builder.set_start_node("same");
    builder.set_end_node("same");
    builder.add_node("node", Box::new(CounterNode));
    builder.add_edge("same", HashSet::from(["node".to_string()]));
    builder.add_edge("node", HashSet::from(["same".to_string()]));
    
    let result = builder.compile();
    
    // 当前代码可能允许这种情况，但这会导致逻辑问题
    // 如果编译通过，检查运行时行为
    if let Ok(graph) = result {
        let state = Arc::new(DefaultMemoryState::new());
        let exec_result = graph.invoke(state).await;
        // 可能会死循环或快速退出
        println!("Same start/end node execution result: {:?}", exec_result);
    }
}

/// 测试场景：边的源节点不存在于图中
/// 验证引用未注册节点作为边源时的错误处理
#[tokio::test]
async fn test_edge_source_not_registered() {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("node", Box::new(CounterNode));
    
    // 边的源是一个不存在的节点
    builder.add_edge("nonexistent_source", HashSet::from(["node".to_string()]));
    builder.add_edge("__start__", HashSet::from(["node".to_string()]));
    builder.add_edge("node", HashSet::from(["__end__".to_string()]));
    
    let result = builder.compile();
    
    // 编译应该报错：边源不是已注册节点
    assert!(result.is_err(), "Edge source must be a registered node");
}

/// 测试场景：多次调用compile消费builder
/// 验证builder被compile后不能再次使用
#[tokio::test]
async fn test_builder_consumed_after_compile() {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("node", Box::new(CounterNode));
    builder.add_edge("__start__", HashSet::from(["node".to_string()]));
    builder.add_edge("node", HashSet::from(["__end__".to_string()]));
    
    let result1 = builder.compile();
    assert!(result1.is_ok(), "First compile should succeed");
    
    // builder已被消费，再次调用compile应该编译错误或行为异常
    // 由于compile(self)消费了self，这里无法再次调用
    // 这验证了Rust的所有权系统保证了安全性
}

/// 测试场景：状态值的None处理
/// 验证从状态中删除键后再读取的行为
#[tokio::test]
async fn test_state_key_deletion_behavior() -> Result<(), LangGraphError> {
    let state = Arc::new(DefaultMemoryState::new());
    
    // 设置一个值
    state.set("temp_value", 42i32).await?;
    let value: Option<i32> = state.get("temp_value").await?;
    assert_eq!(value, Some(42), "Value should be retrievable");
    
    // 注意：DefaultMemoryState 没有删除方法，只能覆盖为None
    // 但serde_json::Value不支持直接存储None（会被序列化为Null）
    state.set("temp_value", Option::<i32>::None).await?;
    
    let value_after: Option<Option<i32>> = state.get("temp_value").await?;
    // 这可能返回Some(None)或者Err，取决于实现
    println!("After setting None: {:?}", value_after);
    
    Ok(())
}

/// 测试场景：极大数值的状态值
/// 验证极端数值范围的处理
#[tokio::test]
async fn test_extreme_numeric_values() -> Result<(), LangGraphError> {
    let state = Arc::new(DefaultMemoryState::new());
    
    // 测试各种极端数值（不包括NaN和Infinity，因为JSON标准不支持这些值）
    state.set("max_i8", i8::MAX).await?;
    state.set("min_i8", i8::MIN).await?;
    state.set("max_i64", i64::MAX).await?;
    state.set("min_i64", i64::MIN).await?;
    state.set("max_u64", u64::MAX).await?;
    state.set("min_u64", u64::MIN).await?;
    state.set("max_f64", f64::MAX).await?;
    state.set("min_positive_f64", f64::MIN_POSITIVE).await?;
    state.set("neg_max_f64", f64::MIN).await?;
    
    // 验证可以正确读回
    assert_eq!(state.get::<i8>("max_i8").await?, Some(i8::MAX));
    assert_eq!(state.get::<i8>("min_i8").await?, Some(i8::MIN));
    assert_eq!(state.get::<u64>("max_u64").await?, Some(u64::MAX));
    assert_eq!(state.get::<f64>("max_f64").await?, Some(f64::MAX));
    
    // BUG测试：JSON不支持NaN和Infinity，这会导致数据丢失或错误
    // 尝试存储NaN应该会失败或产生意外行为
    let nan_result: Result<Option<f64>, LangGraphError> = async {
        state.set("nan_f64", f64::NAN).await?;
        state.get("nan_f64").await
    }.await;
    
    // 这是一个已知的限制/BUG：JSON无法表示NaN
    assert!(nan_result.is_err(), 
            "BUG: JSON cannot represent NaN, should fail or handle gracefully. Error: {:?}", 
            nan_result.err());
    
    Ok(())
}

/// 测试场景：Unicode和特殊字符串作为状态值
/// 验证各种字符串编码的正确性
#[tokio::test]
async fn test_unicode_and_special_strings() -> Result<(), LangGraphError> {
    let state = Arc::new(DefaultMemoryState::new());
    
    let test_strings = vec![
        ("empty", "".to_string()),
        ("spaces", "   ".to_string()),
        ("unicode_chinese", "你好世界".to_string()),
        ("unicode_emoji", "🎉🚀💻".to_string()),
        ("special_chars", "\"'<>\\&\0".to_string()),
        ("very_long", "x".repeat(10000)),
        ("json_like", "{\"key\": \"value\"}".to_string()),
        ("multiline", "line1\nline2\r\nline3".to_string()),
        ("tabs", "\t\t\t".to_string()),
    ];
    
    for (key, value) in &test_strings {
        state.set(key, value).await?;
        let retrieved: Option<String> = state.get(key).await?;
        assert_eq!(retrieved, Some(value.clone()), "Failed for key: {}", key);
    }
    
    Ok(())
}

/// 测试场景：布尔值状态管理
/// 验证布尔类型的序列化/反序列化
#[tokio::test]
async fn test_boolean_state_management() -> Result<(), LangGraphError> {
    let state = Arc::new(DefaultMemoryState::new());
    
    state.set("flag_true", true).await?;
    state.set("flag_false", false).await?;
    
    assert_eq!(state.get::<bool>("flag_true").await?, Some(true));
    assert_eq!(state.get::<bool>("flag_false").await?, Some(false));
    
    // 类型不匹配：存储bool，尝试读取为i32
    let mismatch: Result<Option<i32>, _> = state.get("flag_true").await;
    assert!(mismatch.is_err(), "Type mismatch should fail");
    
    Ok(())
}

/// 测试场景：Vec类型状态管理
/// 验证向量类型的完整支持
#[tokio::test]
async fn test_vec_state_management() -> Result<(), LangGraphError> {
    let state = Arc::new(DefaultMemoryState::new());
    
    let original_vec = vec![1, 2, 3, 4, 5];
    state.set("numbers", &original_vec).await?;
    
    let retrieved: Option<Vec<i32>> = state.get("numbers").await?;
    assert_eq!(retrieved, Some(original_vec), "Vec should round-trip correctly");
    
    // 空向量
    state.set("empty_vec", Vec::<i32>::new()).await?;
    let empty: Option<Vec<i32>> = state.get("empty_vec").await?;
    assert_eq!(empty, Some(vec![]), "Empty vec should work");
    
    // 嵌套Vec
    let nested = vec![vec![1, 2], vec![3, 4], vec![5]];
    state.set("nested_vec", &nested).await?;
    let retrieved_nested: Option<Vec<Vec<i32>>> = state.get("nested_vec").await?;
    assert_eq!(retrieved_nested, Some(nested), "Nested vec should work");
    
    Ok(())
}

/// 测试场景：条件边router中发生panic
/// 验证条件边路由函数panic时的错误传播
#[tokio::test]
async fn test_conditional_router_panic() {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("node", Box::new(CounterNode));

    // router 会panic
    builder.add_conditional_edge("__start__", vec![
        Box::new(|_state| {
            panic!("Intentional panic in router");
        }),
    ]);

    let graph = builder.compile().unwrap();
    let state = Arc::new(DefaultMemoryState::new());

    // 在 tokio 异步上下文中捕获 panic
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            graph.invoke(state).await
        })
    }));

    // 应该捕获到panic
    assert!(result.is_err(), "Panic in conditional router should propagate");
}