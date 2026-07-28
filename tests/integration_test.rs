use langgraph4rust::{
    AgentNode, AgentState, DefaultMemoryState, LangGraphError, StateGraphBuilder,
};
use std::sync::Arc;
use std::collections::HashSet;
use futures::executor::block_on;

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

#[derive(Debug, Clone)]
struct FailingNode;

impl AgentNode<DefaultMemoryState> for FailingNode {
    fn apply(&self, _state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
        Err(LangGraphError::NodeError("Intentional failure".to_string()))
    }
}

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

#[tokio::test]
async fn test_empty_graph_validation() {
    let result = StateGraphBuilder::<DefaultMemoryState>::new().compile();

    assert!(
        matches!(result, Err(LangGraphError::GraphError(msg)) if msg.contains("at least one node")),
        "Empty graph should fail validation"
    );
}

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

#[tokio::test]
async fn test_conditional_edge_empty_string() -> Result<(), LangGraphError> {
    let router: Box<dyn Fn(&DefaultMemoryState) -> String> = Box::new(|_state: &DefaultMemoryState| -> String {
        "".to_string()
    });

    let mut builder = StateGraphBuilder::new();
    builder.add_node("node", Box::new(CounterNode));builder.add_conditional_edge("__start__", vec![router]);
    let graph = builder.compile()?;

    let state = Arc::new(DefaultMemoryState::new());
    let result = graph.invoke(state).await;

    assert!(
        matches!(result, Err(LangGraphError::GraphError(msg)) if msg.contains("empty string")),
        "Conditional edge returning empty string should fail"
    );

    Ok(())
}

#[tokio::test]
async fn test_conditional_edge_invalid_target() -> Result<(), LangGraphError> {
    let router: Box<dyn Fn(&DefaultMemoryState) -> String> = Box::new(|_state: &DefaultMemoryState| -> String {
        "nonexistent".to_string()
    });

    let mut builder = StateGraphBuilder::new();
    builder.add_node("node", Box::new(CounterNode));
    builder.add_conditional_edge("__start__", vec![router]);
    let graph = builder.compile()?;

    let state = Arc::new(DefaultMemoryState::new());
    let result = graph.invoke(state).await;

    assert!(
        matches!(result, Err(LangGraphError::GraphError(msg)) if msg.contains("invalid target")),
        "Conditional edge returning invalid target should fail"
    );

    Ok(())
}

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

#[tokio::test]
async fn test_max_steps_limit() -> Result<(), LangGraphError> {
    let router1: Box<dyn Fn(&DefaultMemoryState) -> String> = Box::new(|_state: &DefaultMemoryState| -> String {
        "loop_node".to_string()
    });
    let router2: Box<dyn Fn(&DefaultMemoryState) -> String> = Box::new(|_state: &DefaultMemoryState| -> String {
        "loop_node".to_string()
    });

    let mut builder = StateGraphBuilder::new();
    builder.add_node("loop_node", Box::new(CounterNode));
    builder.add_conditional_edge("__start__", vec![router1]);
    builder.add_conditional_edge("loop_node", vec![router2]);
    builder.set_max_steps(5);
    let graph = builder.compile()?;

    let state = Arc::new(DefaultMemoryState::new());
    graph.invoke(state.clone()).await?;

    let count: i32 = state.get("count").await?.unwrap();
    assert_eq!(count, 5, "Should stop after max steps");

    Ok(())
}

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

#[tokio::test]
async fn test_state_get_nonexistent_key() -> Result<(), LangGraphError> {
    let state = Arc::new(DefaultMemoryState::new());
    
    let result: Option<String> = state.get("nonexistent").await?;
    assert!(result.is_none(), "Getting non-existent key should return None");

    Ok(())
}

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

#[tokio::test]
async fn test_batch_apply_multiple_failures() -> Result<(), LangGraphError> {
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
        matches!(result, Err(LangGraphError::NodeError(msg)) if msg.contains("2 nodes failed")),
        "Multiple node failures should be collected"
    );

    Ok(())
}

#[tokio::test]
async fn test_cycle_detection() {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("a", Box::new(CounterNode));
    builder.add_node("b", Box::new(CounterNode));
    builder.add_edge("__start__", HashSet::from(["a".to_string()]));
    builder.add_edge("a", HashSet::from(["b".to_string()]));
    builder.add_edge("b", HashSet::from(["a".to_string()]));
    let result = builder.compile();

    assert!(
        matches!(result, Err(LangGraphError::GraphError(msg)) if msg.contains("cycle")),
        "Graph with cycle should fail validation"
    );
}

#[tokio::test]
async fn test_graph_connectivity() {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("isolated", Box::new(CounterNode));
    builder.add_edge("__start__", HashSet::from(["isolated".to_string()]));
    let result = builder.compile();

    assert!(
        matches!(result, Err(LangGraphError::GraphError(msg)) if msg.contains("not reachable")),
        "Graph without path to end node should fail validation"
    );
}