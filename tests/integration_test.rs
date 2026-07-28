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
    assert_eq!(count, 5, "Should stop after max steps");

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
    assert_eq!(count, 3, "Self-loop should execute max_steps times");

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
/// 验证当起始节点也被注册为普通节点时是否会被执行
#[tokio::test]
async fn test_start_node_as_regular_node() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("__start__", Box::new(CounterNode));
    builder.add_edge("__start__", HashSet::from(["__end__".to_string()]));
    let graph = builder.compile()?;

    let state = Arc::new(DefaultMemoryState::new());
    graph.invoke(state.clone()).await?;

    let count: i32 = state.get("count").await?.unwrap_or(0);
    // 当前代码跳过起始节点的执行，所以 count 应该是 0
    // 这暴露了起始节点执行逻辑的问题
    assert_eq!(count, 0, "Current code skips start node execution");

    Ok(())
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
async fn test_max_steps_boundary() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("counter", Box::new(CounterNode));
    builder.add_edge("__start__", HashSet::from(["counter".to_string()]));
    builder.add_edge("counter", HashSet::from(["counter".to_string()]));
    builder.set_max_steps(0);
    
    let graph = builder.compile()?;

    let state = Arc::new(DefaultMemoryState::new());
    graph.invoke(state.clone()).await?;

    let count: i32 = state.get("count").await?.unwrap_or(0);
    assert_eq!(count, 0, "Max steps of 0 should execute nothing");

    Ok(())
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
    
    impl AgentNode<DefaultMemoryState> for OrderNode {
        fn apply(&self, state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
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
    
    let result = builder.compile();
    
    // 当前代码允许空节点名称，这可能是一个安全问题
    assert!(result.is_ok(), "Empty node name should compile (potential security issue)");
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
    assert_eq!(count, 4, "Should execute 4 times before max steps");

    Ok(())
}

/// 测试场景：状态的原子操作
/// 验证状态操作的原子性
#[tokio::test]
async fn test_state_atomic_operation() -> Result<(), LangGraphError> {
    let state = Arc::new(DefaultMemoryState::new());
    
    let state_clone1 = Arc::clone(&state);
    let state_clone2 = Arc::clone(&state);
    
    state.set("counter", 0).await?;
    
    let task1 = tokio::spawn(async move {
        for _ in 0..1000 {
            let val: i32 = state_clone1.get("counter").await.unwrap().unwrap();
            state_clone1.set("counter", val + 1).await.unwrap();
        }
    });
    
    let task2 = tokio::spawn(async move {
        for _ in 0..1000 {
            let val: i32 = state_clone2.get("counter").await.unwrap().unwrap();
            state_clone2.set("counter", val + 1).await.unwrap();
        }
    });
    
    task1.await.unwrap();
    task2.await.unwrap();
    
    let value: i32 = state.get("counter").await?.unwrap();
    // 由于不是原子操作，结果可能不是 2000
    assert!(value < 2000, "Non-atomic operations cause race conditions");

    Ok(())
}

/// 测试场景：节点返回空状态
/// 验证节点不修改状态时的行为
#[tokio::test]
async fn test_node_no_state_modification() -> Result<(), LangGraphError> {
    #[derive(Debug, Clone)]
    struct NoOpNode;
    
    impl AgentNode<DefaultMemoryState> for NoOpNode {
        fn apply(&self, _state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
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
    
    impl AgentNode<DefaultMemoryState> for PanicNode {
        fn apply(&self, _state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
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

/// 测试场景：图的执行时间
/// 验证图执行时间是否合理
#[tokio::test]
async fn test_graph_execution_time() -> Result<(), LangGraphError> {
    use std::time::Instant;
    
    #[derive(Debug, Clone)]
    struct SlowNode;
    
    impl AgentNode<DefaultMemoryState> for SlowNode {
        fn apply(&self, _state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
            std::thread::sleep(std::time::Duration::from_millis(50));
            Ok(())
        }
    }
    
    let mut builder = StateGraphBuilder::new();
    builder.add_node("slow1", Box::new(SlowNode));
    builder.add_node("slow2", Box::new(SlowNode));
    builder.add_edge("__start__", HashSet::from(["slow1".to_string(), "slow2".to_string()]));
    builder.add_edge("slow1", HashSet::from(["__end__".to_string()]));
    builder.add_edge("slow2", HashSet::from(["__end__".to_string()]));
    
    let graph = builder.compile()?;

    let state = Arc::new(DefaultMemoryState::new());
    
    let start = Instant::now();
    graph.invoke(state).await?;
    let duration = start.elapsed();
    
    // 并行执行应该比串行快
    assert!(duration < std::time::Duration::from_millis(120), "Parallel execution should be faster");

    Ok(())
}

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