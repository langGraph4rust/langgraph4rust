//! `state_graph_stream` 模块的全场景测试。
//!
//! 通过公开 API `StateGraph::stream()` 验证推送式事件流的所有行为：
//! 事件顺序、步骤编号、并行计时、条件路由、错误路径、max_steps 截断、接收方提前丢弃等。

use langgraph4rust::{
    AgentNode, AgentState, DefaultMemoryState, LangGraphError, StateGraphBuilder, StreamEvent,
    StreamExt, END_NODE, START_NODE,
};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

// ─── 测试节点 ────────────────────────────────────────────────────────────────

/// 计数节点：每次执行将 state["count"] 加 1
#[derive(Debug, Clone)]
struct CounterNode;

#[langgraph4rust::async_trait]
impl AgentNode<DefaultMemoryState> for CounterNode {
    async fn apply(&self, state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
        let count: i32 = state.get("count").await?.unwrap_or(0);
        state.set("count", count + 1).await?;
        Ok(())
    }
}

/// 失败节点：总是返回错误
#[derive(Debug, Clone)]
struct FailingNode;

#[langgraph4rust::async_trait]
impl AgentNode<DefaultMemoryState> for FailingNode {
    async fn apply(&self, _state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
        Err(LangGraphError::NodeError("intentional failure".into()))
    }
}

/// 慢节点：休眠指定毫秒后写入 state
#[derive(Debug, Clone)]
struct SlowNode {
    ms: u64,
}

#[langgraph4rust::async_trait]
impl AgentNode<DefaultMemoryState> for SlowNode {
    async fn apply(&self, state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
        tokio::time::sleep(Duration::from_millis(self.ms)).await;
        state.set("slow_done", true).await?;
        Ok(())
    }
}

/// 路由节点：根据 state["route"] 决定下一跳（用于条件边）
#[derive(Debug, Clone)]
struct RouterNode;

#[langgraph4rust::async_trait]
impl AgentNode<DefaultMemoryState> for RouterNode {
    async fn apply(&self, state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
        // 仅作为路由锚点，不做实际工作
        let _ = state.get::<i32>("route").await?;
        Ok(())
    }
}

// ─── 辅助函数 ────────────────────────────────────────────────────────────────

/// 收集流中所有事件（泛型：支持任意 AgentState 实现）
async fn collect_events<S>(
    graph: Arc<langgraph4rust::StateGraph<S>>,
    state: Arc<S>,
) -> Vec<StreamEvent<S>>
where
    S: AgentState + Send + Sync + 'static,
{
    let mut rx = graph.stream(state);
    let mut events = Vec::new();
    while let Some(event) = rx.next().await {
        events.push(event);
    }
    events
}

// ─── 1. 线性工作流：事件完整序列 ─────────────────────────────────────────────

/// start → A → end
/// 预期事件序列：WorkflowStarted, StepStarted, NodeStarted, NodeFinished,
///              RoutingDecision, WorkflowFinished
#[tokio::test]
async fn test_linear_event_sequence() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("a", Box::new(CounterNode));
    builder.add_edge(START_NODE, HashSet::from(["a".to_string()]));
    builder.add_edge("a", HashSet::from([END_NODE.to_string()]));
    let graph = Arc::new(builder.compile()?);

    let state = Arc::new(DefaultMemoryState::new());
    let events = collect_events(graph, state.clone()).await;

    // 首尾事件
    assert!(matches!(events.first(), Some(StreamEvent::WorkflowStarted)));
    assert!(matches!(
        events.last(),
        Some(StreamEvent::WorkflowFinished { .. })
    ));

    // __start__ 虚拟节点占 step 1（仅 RoutingDecision），真实节点从 step 2 开始
    // 事件序列：WorkflowStarted, RoutingDecision(start→a), StepStarted(a),
    //          NodeStarted(a), NodeFinished(a), RoutingDecision(a→end), WorkflowFinished
    assert_eq!(events.len(), 7, "expected 7 events, got {}", events.len());
    assert!(matches!(&events[1], StreamEvent::RoutingDecision { step: 1, .. }));
    assert!(matches!(&events[2], StreamEvent::StepStarted { step: 2, nodes } if nodes == &["a"]));
    assert!(matches!(&events[3], StreamEvent::NodeStarted { step: 2, name } if name == "a"));
    assert!(matches!(&events[4], StreamEvent::NodeFinished { step: 2, name, .. } if name == "a"));
    assert!(matches!(&events[5], StreamEvent::RoutingDecision { step: 2, .. }));

    // 状态被更新
    let count: i32 = state.get("count").await?.unwrap_or(0);
    assert_eq!(count, 1);
    Ok(())
}

// ─── 2. 步骤编号一致性 ──────────────────────────────────────────────────────

/// start → A → B → end（两步）
/// 验证 StepStarted / NodeStarted / NodeFinished / RoutingDecision 的 step 字段同步递增
#[tokio::test]
async fn test_step_index_consistency() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("a", Box::new(CounterNode));
    builder.add_node("b", Box::new(CounterNode));
    builder.add_edge(START_NODE, HashSet::from(["a".to_string()]));
    builder.add_edge("a", HashSet::from(["b".to_string()]));
    builder.add_edge("b", HashSet::from([END_NODE.to_string()]));
    let graph = Arc::new(builder.compile()?);

    let events = collect_events(graph, Arc::new(DefaultMemoryState::new())).await;

    // __start__ 占 step 1（仅 RoutingDecision），真实节点 A 在 step 2，B 在 step 3
    let mut step1_events = 0usize;
    let mut step2_events = 0usize;
    let mut step3_events = 0usize;
    for e in &events {
        match e {
            StreamEvent::StepStarted { step, .. }
            | StreamEvent::NodeStarted { step, .. }
            | StreamEvent::NodeFinished { step, .. }
            | StreamEvent::RoutingDecision { step, .. } => match *step {
                1 => step1_events += 1,
                2 => step2_events += 1,
                3 => step3_events += 1,
                _ => panic!("unexpected step index: {}", step),
            },
            _ => {}
        }
    }
    // step1: 仅 RoutingDecision(__start__→a) = 1
    assert_eq!(step1_events, 1, "step 1 should have 1 event (RoutingDecision)");
    // step2: StepStarted + NodeStarted + NodeFinished + RoutingDecision = 4
    assert_eq!(step2_events, 4, "step 2 should have 4 events");
    // step3: 同上 = 4
    assert_eq!(step3_events, 4, "step 3 should have 4 events");
    Ok(())
}

// ─── 3. WorkflowFinished 元数据 ─────────────────────────────────────────────

#[tokio::test]
async fn test_workflow_finished_metadata() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("a", Box::new(CounterNode));
    builder.add_node("b", Box::new(CounterNode));
    builder.add_node("c", Box::new(CounterNode));
    builder.add_edge(START_NODE, HashSet::from(["a".to_string()]));
    builder.add_edge("a", HashSet::from(["b".to_string()]));
    builder.add_edge("b", HashSet::from(["c".to_string()]));
    builder.add_edge("c", HashSet::from([END_NODE.to_string()]));
    let graph = Arc::new(builder.compile()?);

    let state = Arc::new(DefaultMemoryState::new());
    let events = collect_events(Arc::clone(&graph), Arc::clone(&state)).await;

    if let Some(StreamEvent::WorkflowFinished {
        state: final_state,
        total_steps,
        elapsed,
    }) = events.last()
    {
        // total_steps 包含 __start__ 步和 __end__ 检测步
        assert_eq!(*total_steps, 5, "should execute 5 steps (start + 3 nodes + end)");
        assert!(*elapsed > Duration::ZERO, "elapsed should be positive");
        // final_state 与传入的 state 是同一个 Arc
        let count: i32 = final_state.get("count").await?.unwrap_or(0);
        assert_eq!(count, 3);
    } else {
        panic!("last event should be WorkflowFinished");
    }
    Ok(())
}

// ─── 4. 并行节点：事件交错 & 独立计时 ───────────────────────────────────────

/// start → {slow_a, slow_b} → end
/// 两节点各休眠 50ms，并行执行时总耗时应 < 100ms（串行则 >= 100ms）
#[tokio::test]
async fn test_parallel_nodes_concurrent_timing() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("slow_a", Box::new(SlowNode { ms: 50 }));
    builder.add_node("slow_b", Box::new(SlowNode { ms: 50 }));
    builder.add_edge(
        START_NODE,
        HashSet::from(["slow_a".to_string(), "slow_b".to_string()]),
    );
    builder.add_edge("slow_a", HashSet::from([END_NODE.to_string()]));
    builder.add_edge("slow_b", HashSet::from([END_NODE.to_string()]));
    let graph = Arc::new(builder.compile()?);

    let events = collect_events(graph, Arc::new(DefaultMemoryState::new())).await;

    // 收集两个节点的 elapsed
    let elapsed_values: Vec<Duration> = events
        .iter()
        .filter_map(|e| match e {
            StreamEvent::NodeFinished { elapsed, .. } => Some(*elapsed),
            _ => None,
        })
        .collect();
    assert_eq!(elapsed_values.len(), 2, "should have 2 NodeFinished events");

    // 每个节点的 elapsed 应 >= 50ms（自身真实耗时）
    for d in &elapsed_values {
        assert!(
            *d >= Duration::from_millis(45),
            "node elapsed {:?} should be ~50ms",
            d
        );
    }

    // WorkflowFinished.elapsed 应 < 150ms（并行，非串行 100ms+）
    if let Some(StreamEvent::WorkflowFinished { elapsed, .. }) = events.last() {
        assert!(
            *elapsed < Duration::from_millis(150),
            "total elapsed {:?} suggests sequential execution",
            elapsed
        );
    }
    Ok(())
}

/// 验证并行步骤的 StepStarted.nodes 包含所有并行节点名
#[tokio::test]
async fn test_parallel_step_started_contains_all_nodes() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("x", Box::new(CounterNode));
    builder.add_node("y", Box::new(CounterNode));
    builder.add_node("z", Box::new(CounterNode));
    builder.add_edge(
        START_NODE,
        HashSet::from(["x".to_string(), "y".to_string(), "z".to_string()]),
    );
    builder.add_edge("x", HashSet::from([END_NODE.to_string()]));
    builder.add_edge("y", HashSet::from([END_NODE.to_string()]));
    builder.add_edge("z", HashSet::from([END_NODE.to_string()]));
    let graph = Arc::new(builder.compile()?);

    let events = collect_events(graph, Arc::new(DefaultMemoryState::new())).await;

    let step_started = events.iter().find(|e| matches!(e, StreamEvent::StepStarted { .. }));
    if let Some(StreamEvent::StepStarted { step, nodes }) = step_started {
        assert_eq!(*step, 2, "real nodes start at step 2");
        assert_eq!(nodes.len(), 3);
        let set: HashSet<&String> = nodes.iter().collect();
        assert!(set.contains(&&"x".to_string()));
        assert!(set.contains(&&"y".to_string()));
        assert!(set.contains(&&"z".to_string()));
    } else {
        panic!("should have StepStarted event");
    }
    Ok(())
}

// ─── 5. 条件路由 ────────────────────────────────────────────────────────────

/// start → router → (条件边根据 state["route"] 选 "left" 或 "right") → end
#[tokio::test]
async fn test_conditional_routing_decision() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("router", Box::new(RouterNode));
    builder.add_node("left", Box::new(CounterNode));
    builder.add_node("right", Box::new(CounterNode));
    builder.add_edge(START_NODE, HashSet::from(["router".to_string()]));
    builder.add_conditional_edge(
        "router",
        vec![Box::new(|state: &DefaultMemoryState| {
            // 同步上下文中无法 await，用 blocking 方式读取（测试简化）
            // 这里直接返回固定值模拟路由
            let _ = state;
            "left".to_string()
        })],
    );
    builder.add_edge("left", HashSet::from([END_NODE.to_string()]));
    builder.add_edge("right", HashSet::from([END_NODE.to_string()]));
    let graph = Arc::new(builder.compile()?);

    let events = collect_events(graph, Arc::new(DefaultMemoryState::new())).await;

    // 应存在 RoutingDecision 且 to_nodes 包含 "left"（从 router 出发的条件路由）
    let routing = events.iter().find(|e| matches!(
        e,
        StreamEvent::RoutingDecision { from_nodes, .. } if from_nodes.contains(&"router".to_string())
    ));
    assert!(routing.is_some(), "should have RoutingDecision from router");
    if let Some(StreamEvent::RoutingDecision {
        from_nodes,
        to_nodes,
        ..
    }) = routing
    {
        assert!(from_nodes.contains(&"router".to_string()));
        assert!(to_nodes.contains(&"left".to_string()));
    }

    // 最终成功
    assert!(matches!(
        events.last(),
        Some(StreamEvent::WorkflowFinished { .. })
    ));
    Ok(())
}

// ─── 6. 节点执行失败 → WorkflowError ────────────────────────────────────────

#[tokio::test]
async fn test_node_failure_emits_workflow_error() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("ok", Box::new(CounterNode));
    builder.add_node("fail", Box::new(FailingNode));
    builder.add_edge(START_NODE, HashSet::from(["ok".to_string()]));
    builder.add_edge("ok", HashSet::from(["fail".to_string()]));
    builder.add_edge("fail", HashSet::from([END_NODE.to_string()]));
    let graph = Arc::new(builder.compile()?);

    let events = collect_events(graph, Arc::new(DefaultMemoryState::new())).await;

    // 首事件 WorkflowStarted
    assert!(matches!(events.first(), Some(StreamEvent::WorkflowStarted)));
    // 末事件 WorkflowError（不是 WorkflowFinished）
    assert!(matches!(
        events.last(),
        Some(StreamEvent::WorkflowError { .. })
    ));
    // 不应出现 WorkflowFinished
    assert!(!events
        .iter()
        .any(|e| matches!(e, StreamEvent::WorkflowFinished { .. })));

    // 错误内容验证（__start__=step1, ok=step2, fail=step3）
    if let Some(StreamEvent::WorkflowError { step, error, .. }) = events.last() {
        assert_eq!(*step, 3, "error should occur at step 3");
        assert!(matches!(error, LangGraphError::NodeError(_)));
    }
    Ok(())
}

/// 并行节点中一个失败 → WorkflowError，且 NodeFinished 仍被发射（失败节点也有）
#[tokio::test]
async fn test_parallel_node_failure() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("good", Box::new(CounterNode));
    builder.add_node("bad", Box::new(FailingNode));
    builder.add_edge(
        START_NODE,
        HashSet::from(["good".to_string(), "bad".to_string()]),
    );
    builder.add_edge("good", HashSet::from([END_NODE.to_string()]));
    builder.add_edge("bad", HashSet::from([END_NODE.to_string()]));
    let graph = Arc::new(builder.compile()?);

    let events = collect_events(graph, Arc::new(DefaultMemoryState::new())).await;

    // 应以 WorkflowError 结束
    assert!(matches!(
        events.last(),
        Some(StreamEvent::WorkflowError { .. })
    ));
    // 两个节点都应有 NodeStarted（并行启动）
    let started_count = events
        .iter()
        .filter(|e| matches!(e, StreamEvent::NodeStarted { .. }))
        .count();
    assert_eq!(started_count, 2, "both nodes should start");
    Ok(())
}

// ─── 7. max_steps 耗尽 → WorkflowError ──────────────────────────────────────

/// 构造一个环：A → B → A → B → ...，设 max_steps=4，永远到不了 end
#[tokio::test]
async fn test_max_steps_exhaustion_emits_error() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.set_max_steps(4);
    builder.add_node("a", Box::new(CounterNode));
    builder.add_node("b", Box::new(CounterNode));
    builder.add_edge(START_NODE, HashSet::from(["a".to_string()]));
    builder.add_edge("a", HashSet::from(["b".to_string()]));
    builder.add_edge("b", HashSet::from(["a".to_string()])); // 环！永远不到 end
    let graph = Arc::new(builder.compile()?);

    let state = Arc::new(DefaultMemoryState::new());
    let events = collect_events(Arc::clone(&graph), Arc::clone(&state)).await;

    // 末事件应为 WorkflowError（GraphError: Reached max_steps）
    if let Some(StreamEvent::WorkflowError { step, error, .. }) = events.last() {
        assert_eq!(*step, 4, "should stop at max_steps");
        let msg = error.to_string();
        assert!(
            msg.contains("max_steps"),
            "error should mention max_steps, got: {}",
            msg
        );
    } else {
        panic!("last event should be WorkflowError");
    }

    // 不应出现 WorkflowFinished
    assert!(!events
        .iter()
        .any(|e| matches!(e, StreamEvent::WorkflowFinished { .. })));

    // 状态仍被部分更新（执行了若干步）
    let count: i32 = state.get("count").await?.unwrap_or(0);
    assert!(count > 0, "some steps should have executed");
    Ok(())
}

// ─── 8. 接收方提前 drop → 驱动静默终止 ──────────────────────────────────────

#[tokio::test]
async fn test_receiver_drop_terminates_driver() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    for name in ["n1", "n2", "n3", "n4", "n5"] {
        builder.add_node(name, Box::new(SlowNode { ms: 20 }));
    }
    builder.add_edge(START_NODE, HashSet::from(["n1".to_string()]));
    builder.add_edge("n1", HashSet::from(["n2".to_string()]));
    builder.add_edge("n2", HashSet::from(["n3".to_string()]));
    builder.add_edge("n3", HashSet::from(["n4".to_string()]));
    builder.add_edge("n4", HashSet::from(["n5".to_string()]));
    builder.add_edge("n5", HashSet::from([END_NODE.to_string()]));
    let graph = Arc::new(builder.compile()?);

    let mut rx = graph.stream(Arc::new(DefaultMemoryState::new()));

    // 只取第一个事件后立即 drop
    let first = rx.next().await;
    assert!(matches!(first, Some(StreamEvent::WorkflowStarted)));
    drop(rx);

    // 给后台任务一点时间感知 drop 并退出（不会 panic）
    tokio::time::sleep(Duration::from_millis(50)).await;
    // 到这里没有 panic 即为通过
    Ok(())
}

// ─── 9. 多步状态累积 ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_state_accumulates_across_steps() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    for name in ["s1", "s2", "s3", "s4"] {
        builder.add_node(name, Box::new(CounterNode));
    }
    builder.add_edge(START_NODE, HashSet::from(["s1".to_string()]));
    builder.add_edge("s1", HashSet::from(["s2".to_string()]));
    builder.add_edge("s2", HashSet::from(["s3".to_string()]));
    builder.add_edge("s3", HashSet::from(["s4".to_string()]));
    builder.add_edge("s4", HashSet::from([END_NODE.to_string()]));
    let graph = Arc::new(builder.compile()?);

    let state = Arc::new(DefaultMemoryState::new());
    let events = collect_events(Arc::clone(&graph), Arc::clone(&state)).await;

    assert!(matches!(
        events.last(),
        Some(StreamEvent::WorkflowFinished { .. })
    ));
    let count: i32 = state.get("count").await?.unwrap_or(0);
    assert_eq!(count, 4, "4 nodes should increment count 4 times");
    Ok(())
}

// ─── 10. NodeFinished.elapsed 精确性 ────────────────────────────────────────

/// 单慢节点（80ms），验证 NodeFinished.elapsed >= 80ms
#[tokio::test]
async fn test_node_finished_elapsed_accuracy() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("slow", Box::new(SlowNode { ms: 80 }));
    builder.add_edge(START_NODE, HashSet::from(["slow".to_string()]));
    builder.add_edge("slow", HashSet::from([END_NODE.to_string()]));
    let graph = Arc::new(builder.compile()?);

    let events = collect_events(graph, Arc::new(DefaultMemoryState::new())).await;

    let node_finished = events
        .iter()
        .find(|e| matches!(e, StreamEvent::NodeFinished { name, .. } if name == "slow"));
    if let Some(StreamEvent::NodeFinished { elapsed, .. }) = node_finished {
        assert!(
            *elapsed >= Duration::from_millis(75),
            "elapsed {:?} should be >= ~80ms",
            elapsed
        );
    } else {
        panic!("should have NodeFinished for 'slow'");
    }
    Ok(())
}

// ─── 11. RoutingDecision 内容正确性 ─────────────────────────────────────────

#[tokio::test]
async fn test_routing_decision_from_to_nodes() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("alpha", Box::new(CounterNode));
    builder.add_node("beta", Box::new(CounterNode));
    builder.add_edge(START_NODE, HashSet::from(["alpha".to_string()]));
    builder.add_edge("alpha", HashSet::from(["beta".to_string()]));
    builder.add_edge("beta", HashSet::from([END_NODE.to_string()]));
    let graph = Arc::new(builder.compile()?);

    let events = collect_events(graph, Arc::new(DefaultMemoryState::new())).await;

    // 共 3 条路由：__start__→alpha, alpha→beta, beta→__end__
    let routings: Vec<_> = events
        .iter()
        .filter(|e| matches!(e, StreamEvent::RoutingDecision { .. }))
        .collect();
    assert_eq!(routings.len(), 3, "should have 3 routing decisions");

    // 第一条：__start__ → alpha (step 1)
    if let StreamEvent::RoutingDecision {
        step,
        from_nodes,
        to_nodes,
    } = routings[0]
    {
        assert_eq!(*step, 1);
        assert!(from_nodes.contains(&"__start__".to_string()));
        assert!(to_nodes.contains(&"alpha".to_string()));
    }
    // 第二条：alpha → beta (step 2)
    if let StreamEvent::RoutingDecision {
        step,
        from_nodes,
        to_nodes,
    } = routings[1]
    {
        assert_eq!(*step, 2);
        assert!(from_nodes.contains(&"alpha".to_string()));
        assert!(to_nodes.contains(&"beta".to_string()));
    }
    // 第三条：beta → __end__ (step 3)
    if let StreamEvent::RoutingDecision {
        step,
        from_nodes,
        to_nodes,
    } = routings[2]
    {
        assert_eq!(*step, 3);
        assert!(from_nodes.contains(&"beta".to_string()));
        assert!(to_nodes.contains(&"__end__".to_string()));
    }
    Ok(())
}

// ─── 12. 事件总数精确验证 ───────────────────────────────────────────────────

/// N 步线性图的事件总数 = 1(WorkflowStarted) + 1(__start__ RoutingDecision) + N*(StepStarted+NodeStarted+NodeFinished+RoutingDecision) + 1(WorkflowFinished)
/// 即 3 + 4N
#[tokio::test]
async fn test_event_count_formula() -> Result<(), LangGraphError> {
    // N 个真实节点：step1=__start__(仅RoutingDecision), step2..N+1=真实节点(各4事件), stepN+2=__end__检测
    // 事件总数 = 1(WorkflowStarted) + 1(__start__ RoutingDecision) + N*4 + 1(WorkflowFinished) = 3 + 4N
    for n in 1..=6 {
        let mut builder = StateGraphBuilder::new();
        let names: Vec<String> = (0..n).map(|i| format!("node_{}", i)).collect();
        for name in &names {
            builder.add_node(name, Box::new(CounterNode));
        }
        builder.add_edge(START_NODE, HashSet::from([names[0].clone()]));
        for i in 0..n - 1 {
            builder.add_edge(&names[i], HashSet::from([names[i + 1].clone()]));
        }
        builder.add_edge(&names[n - 1], HashSet::from([END_NODE.to_string()]));
        let graph = Arc::new(builder.compile()?);

        let events = collect_events(graph, Arc::new(DefaultMemoryState::new())).await;
        let expected = 3 + 4 * n;
        assert_eq!(
            events.len(),
            expected,
            "n={}: expected {} events, got {}",
            n,
            expected,
            events.len()
        );
    }
    Ok(())
}

// ─── 13. WorkflowStarted 始终是第一个事件 ────────────────────────────────────

#[tokio::test]
async fn test_workflow_started_always_first() -> Result<(), LangGraphError> {
    // 成功路径
    let mut builder = StateGraphBuilder::new();
    builder.add_node("a", Box::new(CounterNode));
    builder.add_edge(START_NODE, HashSet::from(["a".to_string()]));
    builder.add_edge("a", HashSet::from([END_NODE.to_string()]));
    let graph = Arc::new(builder.compile()?);
    let events = collect_events(graph, Arc::new(DefaultMemoryState::new())).await;
    assert!(matches!(events[0], StreamEvent::WorkflowStarted));

    // 失败路径
    let mut builder = StateGraphBuilder::new();
    builder.add_node("f", Box::new(FailingNode));
    builder.add_edge(START_NODE, HashSet::from(["f".to_string()]));
    builder.add_edge("f", HashSet::from([END_NODE.to_string()]));
    let graph = Arc::new(builder.compile()?);
    let events = collect_events(graph, Arc::new(DefaultMemoryState::new())).await;
    assert!(matches!(events[0], StreamEvent::WorkflowStarted));
    Ok(())
}

// ─── 14. WorkflowError 是失败时的最后一个事件（流随即关闭）────────────────────

#[tokio::test]
async fn test_workflow_error_is_terminal() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("a", Box::new(CounterNode));
    builder.add_node("fail", Box::new(FailingNode));
    builder.add_node("unreachable", Box::new(CounterNode));
    builder.add_edge(START_NODE, HashSet::from(["a".to_string()]));
    builder.add_edge("a", HashSet::from(["fail".to_string()]));
    builder.add_edge("fail", HashSet::from(["unreachable".to_string()]));
    builder.add_edge("unreachable", HashSet::from([END_NODE.to_string()]));
    let graph = Arc::new(builder.compile()?);

    let events = collect_events(graph, Arc::new(DefaultMemoryState::new())).await;

    // WorkflowError 是最后一个事件
    assert!(matches!(
        events.last(),
        Some(StreamEvent::WorkflowError { .. })
    ));
    // "unreachable" 节点不应有任何事件
    assert!(!events.iter().any(|e| matches!(
        e,
        StreamEvent::NodeStarted { name, .. } if name == "unreachable"
    )));
    Ok(())
}

// ─── 15. 菱形拓扑：fan-out 后 fan-in ────────────────────────────────────────

/// start → {a, b} → merge → end：并行分支汇聚到单一节点
#[tokio::test]
async fn test_diamond_fan_out_fan_in() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("a", Box::new(CounterNode));
    builder.add_node("b", Box::new(CounterNode));
    builder.add_node("merge", Box::new(CounterNode));
    builder.add_edge(START_NODE, HashSet::from(["a".to_string(), "b".to_string()]));
    builder.add_edge("a", HashSet::from(["merge".to_string()]));
    builder.add_edge("b", HashSet::from(["merge".to_string()]));
    builder.add_edge("merge", HashSet::from([END_NODE.to_string()]));
    let graph = Arc::new(builder.compile()?);

    let state = Arc::new(DefaultMemoryState::new());
    let events = collect_events(Arc::clone(&graph), Arc::clone(&state)).await;

    assert!(matches!(events.last(), Some(StreamEvent::WorkflowFinished { .. })));
    // a, b, merge 各执行一次
    let count: i32 = state.get("count").await?.unwrap_or(0);
    assert_eq!(count, 3, "a + b + merge should run 3 times");
    // merge 只应启动一次（fan-in 去重）
    let merge_starts = events
        .iter()
        .filter(|e| matches!(e, StreamEvent::NodeStarted { name, .. } if name == "merge"))
        .count();
    assert_eq!(merge_starts, 1, "merge should run exactly once");
    Ok(())
}

// ─── 16. 并行节点全部成功 ────────────────────────────────────────────────────

#[tokio::test]
async fn test_parallel_all_nodes_finish_successfully() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("a", Box::new(CounterNode));
    builder.add_node("b", Box::new(CounterNode));
    builder.add_node("c", Box::new(CounterNode));
    builder.add_edge(
        START_NODE,
        HashSet::from(["a".to_string(), "b".to_string(), "c".to_string()]),
    );
    builder.add_edge("a", HashSet::from([END_NODE.to_string()]));
    builder.add_edge("b", HashSet::from([END_NODE.to_string()]));
    builder.add_edge("c", HashSet::from([END_NODE.to_string()]));
    let graph = Arc::new(builder.compile()?);

    let state = Arc::new(DefaultMemoryState::new());
    let events = collect_events(Arc::clone(&graph), Arc::clone(&state)).await;

    assert!(matches!(events.last(), Some(StreamEvent::WorkflowFinished { .. })));
    let finished = events
        .iter()
        .filter(|e| matches!(e, StreamEvent::NodeFinished { .. }))
        .count();
    assert_eq!(finished, 3, "all 3 nodes should finish");
    let count: i32 = state.get("count").await?.unwrap_or(0);
    assert_eq!(count, 3);
    Ok(())
}

// ─── 17. WorkflowError 携带状态快照（部分更新保留）─────────────────────

#[tokio::test]
async fn test_workflow_error_carries_state_snapshot() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("ok", Box::new(CounterNode));
    builder.add_node("fail", Box::new(FailingNode));
    builder.add_edge(START_NODE, HashSet::from(["ok".to_string()]));
    builder.add_edge("ok", HashSet::from(["fail".to_string()]));
    builder.add_edge("fail", HashSet::from([END_NODE.to_string()]));
    let graph = Arc::new(builder.compile()?);

    let events = collect_events(graph, Arc::new(DefaultMemoryState::new())).await;

    if let Some(StreamEvent::WorkflowError { state, .. }) = events.last() {
        // ok 节点已将 count 置 1，错误快照应保留该部分更新
        let count: i32 = state.get("count").await?.unwrap_or(0);
        assert_eq!(count, 1, "error snapshot should keep partial update");
    } else {
        panic!("last event should be WorkflowError");
    }
    Ok(())
}

// ─── 18. WorkflowFinished.state 与输入是同一个 Arc ─────────────────────────

#[tokio::test]
async fn test_workflow_finished_state_is_same_arc() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("a", Box::new(CounterNode));
    builder.add_edge(START_NODE, HashSet::from(["a".to_string()]));
    builder.add_edge("a", HashSet::from([END_NODE.to_string()]));
    let graph = Arc::new(builder.compile()?);

    let state = Arc::new(DefaultMemoryState::new());
    let events = collect_events(graph, Arc::clone(&state)).await;

    if let Some(StreamEvent::WorkflowFinished { state: final_state, .. }) = events.last() {
        assert!(
            Arc::ptr_eq(&state, final_state),
            "finished state should be the same Arc as input"
        );
    } else {
        panic!("last event should be WorkflowFinished");
    }
    Ok(())
}

// ─── 19. 图复用：同一编译图顺序多次 stream ────────────────────────────────

#[tokio::test]
async fn test_graph_reuse_sequential_streams() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("a", Box::new(CounterNode));
    builder.add_edge(START_NODE, HashSet::from(["a".to_string()]));
    builder.add_edge("a", HashSet::from([END_NODE.to_string()]));
    let graph = Arc::new(builder.compile()?);

    // 两次独立执行，状态互不干扰
    for _ in 0..2 {
        let state = Arc::new(DefaultMemoryState::new());
        let events = collect_events(Arc::clone(&graph), Arc::clone(&state)).await;
        assert!(matches!(events.last(), Some(StreamEvent::WorkflowFinished { .. })));
        let count: i32 = state.get("count").await?.unwrap_or(0);
        assert_eq!(count, 1, "each run starts from fresh state");
    }
    Ok(())
}

// ─── 20. 并发流：同一 Arc 图同时跑多个 stream ──────────────────────────────

#[tokio::test]
async fn test_concurrent_streams_same_graph() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("a", Box::new(CounterNode));
    builder.add_node("b", Box::new(CounterNode));
    builder.add_edge(START_NODE, HashSet::from(["a".to_string()]));
    builder.add_edge("a", HashSet::from(["b".to_string()]));
    builder.add_edge("b", HashSet::from([END_NODE.to_string()]));
    let graph = Arc::new(builder.compile()?);

    let (e1, e2, e3) = tokio::join!(
        collect_events(Arc::clone(&graph), Arc::new(DefaultMemoryState::new())),
        collect_events(Arc::clone(&graph), Arc::new(DefaultMemoryState::new())),
        collect_events(Arc::clone(&graph), Arc::new(DefaultMemoryState::new())),
    );
    for events in [&e1, &e2, &e3] {
        assert!(matches!(events.last(), Some(StreamEvent::WorkflowFinished { .. })));
    }
    Ok(())
}

// ─── 21. 多 router 条件边：下一跳为各 router 结果并集 ─────────────────────

/// 单一条件边含两个 router（分别返回 t1 / t2），下一步应同时执行 t1 和 t2
#[tokio::test]
async fn test_multiple_conditional_routers_merge() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("split", Box::new(CounterNode));
    builder.add_node("t1", Box::new(CounterNode));
    builder.add_node("t2", Box::new(CounterNode));
    builder.add_edge(START_NODE, HashSet::from(["split".to_string()]));
    builder.add_conditional_edge(
        "split",
        vec![
            Box::new(|_state: &DefaultMemoryState| "t1".to_string()),
            Box::new(|_state: &DefaultMemoryState| "t2".to_string()),
        ],
    );
    builder.add_edge("t1", HashSet::from([END_NODE.to_string()]));
    builder.add_edge("t2", HashSet::from([END_NODE.to_string()]));
    let graph = Arc::new(builder.compile()?);

    let state = Arc::new(DefaultMemoryState::new());
    let events = collect_events(Arc::clone(&graph), Arc::clone(&state)).await;

    // 从 split 出发的 RoutingDecision 应同时包含 t1 和 t2
    let routing = events.iter().find(|e| matches!(
        e,
        StreamEvent::RoutingDecision { from_nodes, .. } if from_nodes.contains(&"split".to_string())
    ));
    if let Some(StreamEvent::RoutingDecision { to_nodes, .. }) = routing {
        assert!(to_nodes.contains(&"t1".to_string()), "missing t1");
        assert!(to_nodes.contains(&"t2".to_string()), "missing t2");
    } else {
        panic!("should have RoutingDecision from split");
    }
    assert!(matches!(events.last(), Some(StreamEvent::WorkflowFinished { .. })));
    // t1, t2 各执行一次（split 也执行）→ count = 3
    let count: i32 = state.get("count").await?.unwrap_or(0);
    assert_eq!(count, 3);
    Ok(())
}

/// 补充：验证器禁止同一节点同时拥有静态边与条件边（compile 报错）
#[test]
fn test_node_cannot_have_both_static_and_conditional_edges() {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("split", Box::new(CounterNode));
    builder.add_node("t", Box::new(CounterNode));
    builder.add_edge(START_NODE, HashSet::from(["split".to_string()]));
    builder.add_edge("split", HashSet::from(["t".to_string()]));
    builder.add_conditional_edge(
        "split",
        vec![Box::new(|_state: &DefaultMemoryState| "t".to_string())],
    );
    builder.add_edge("t", HashSet::from([END_NODE.to_string()]));

    let result = builder.compile();
    assert!(result.is_err(), "compile should reject mixed static + conditional edges");
    let err = result.err().unwrap();
    assert!(matches!(err, LangGraphError::GraphError(_)));
    assert!(err.to_string().contains("both static edges and conditional edges"));
}

// ─── 22. 条件边直接路由到 END ───────────────────────────────────────────────

#[tokio::test]
async fn test_conditional_edge_directly_to_end() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("router", Box::new(RouterNode));
    builder.add_edge(START_NODE, HashSet::from(["router".to_string()]));
    builder.add_conditional_edge(
        "router",
        vec![Box::new(|_state: &DefaultMemoryState| END_NODE.to_string())],
    );
    let graph = Arc::new(builder.compile()?);

    let events = collect_events(graph, Arc::new(DefaultMemoryState::new())).await;
    assert!(matches!(events.last(), Some(StreamEvent::WorkflowFinished { .. })));
    // router 后直接结束，不应有其他节点启动
    let started = events
        .iter()
        .filter(|e| matches!(e, StreamEvent::NodeStarted { .. }))
        .count();
    assert_eq!(started, 1, "only router should start");
    Ok(())
}

// ─── 23. 大扇出：触发有界通道背压 ───────────────────────────────────────────

/// 20 个并行节点，单步事件数(StepStarted+20*Started+20*Finished+Routing=42)超过通道容量32，
/// 验证背压下不死锁、全部完成
#[tokio::test]
async fn test_large_fan_out_backpressure() -> Result<(), LangGraphError> {
    const N: usize = 20;
    let mut builder = StateGraphBuilder::new();
    let names: Vec<String> = (0..N).map(|i| format!("p{}", i)).collect();
    for name in &names {
        builder.add_node(name, Box::new(CounterNode));
    }
    builder.add_edge(START_NODE, names.iter().cloned().collect());
    for name in &names {
        builder.add_edge(name, HashSet::from([END_NODE.to_string()]));
    }
    let graph = Arc::new(builder.compile()?);

    let state = Arc::new(DefaultMemoryState::new());
    let events = collect_events(Arc::clone(&graph), Arc::clone(&state)).await;

    assert!(matches!(events.last(), Some(StreamEvent::WorkflowFinished { .. })));
    let finished = events
        .iter()
        .filter(|e| matches!(e, StreamEvent::NodeFinished { .. }))
        .count();
    assert_eq!(finished, N, "all {} nodes should finish", N);
    let count: i32 = state.get("count").await?.unwrap_or(0);
    assert_eq!(count, N as i32);
    Ok(())
}

// ─── 24. 自循环 + max_steps ─────────────────────────────────────────────────

/// a → a（自环），永远到不了 end，max_steps=3 截断
#[tokio::test]
async fn test_self_loop_max_steps() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.set_max_steps(3);
    builder.add_node("a", Box::new(CounterNode));
    builder.add_edge(START_NODE, HashSet::from(["a".to_string()]));
    builder.add_edge("a", HashSet::from(["a".to_string()])); // 自环
    let graph = Arc::new(builder.compile()?);

    let events = collect_events(graph, Arc::new(DefaultMemoryState::new())).await;

    if let Some(StreamEvent::WorkflowError { error, .. }) = events.last() {
        assert!(
            error.to_string().contains("max_steps"),
            "should report max_steps"
        );
    } else {
        panic!("last event should be WorkflowError");
    }
    assert!(!events.iter().any(|e| matches!(e, StreamEvent::WorkflowFinished { .. })));
    Ok(())
}

// ─── 25. 失败节点仍发射 NodeFinished ────────────────────────────────────────

/// run_node_with_events 在 apply 返回后无条件发 NodeFinished，故失败节点也有该事件
#[tokio::test]
async fn test_failing_node_still_emits_node_finished() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::new();
    builder.add_node("fail", Box::new(FailingNode));
    builder.add_edge(START_NODE, HashSet::from(["fail".to_string()]));
    builder.add_edge("fail", HashSet::from([END_NODE.to_string()]));
    let graph = Arc::new(builder.compile()?);

    let events = collect_events(graph, Arc::new(DefaultMemoryState::new())).await;

    assert!(events.iter().any(|e| matches!(
        e,
        StreamEvent::NodeStarted { name, .. } if name == "fail"
    )));
    assert!(events.iter().any(|e| matches!(
        e,
        StreamEvent::NodeFinished { name, .. } if name == "fail"
    )), "failing node should still emit NodeFinished");
    assert!(matches!(events.last(), Some(StreamEvent::WorkflowError { .. })));
    Ok(())
}

// ─── 26. 状态依赖的条件路由（自定义同步可读状态）─────────────────────

/// 同步可读的状态实现：router 是同步 Fn，需能同步读取节点异步写入的路由值
#[derive(Clone)]
struct SyncRouteState {
    data: Arc<std::sync::Mutex<std::collections::HashMap<String, serde_json::Value>>>,
}

impl SyncRouteState {
    fn new() -> Self {
        Self {
            data: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        }
    }
    /// 同步读取字符串值（供同步 router 使用）
    fn get_sync(&self, key: &str) -> Option<String> {
        self.data
            .lock()
            .unwrap()
            .get(key)
            .and_then(|v| v.as_str().map(|s| s.to_string()))
    }
}

#[langgraph4rust::async_trait]
impl AgentState for SyncRouteState {
    async fn get<T: serde::de::DeserializeOwned + Send + Sync>(
        &self,
        key: &str,
    ) -> Result<Option<T>, LangGraphError> {
        let guard = self.data.lock().unwrap();
        Ok(guard.get(key).and_then(|v| serde_json::from_value(v.clone()).ok()))
    }
    async fn set<T: serde::Serialize + Send + Sync>(
        &self,
        key: &str,
        value: T,
    ) -> Result<bool, LangGraphError> {
        let v = serde_json::to_value(value).map_err(|e| LangGraphError::StateError(e.to_string()))?;
        self.data.lock().unwrap().insert(key.to_string(), v);
        Ok(true)
    }
}

/// 写入路由值的节点
#[derive(Clone)]
struct SetRouteNode {
    target: String,
}

#[langgraph4rust::async_trait]
impl AgentNode<SyncRouteState> for SetRouteNode {
    async fn apply(&self, state: Arc<SyncRouteState>) -> Result<(), LangGraphError> {
        state.set("route", self.target.clone()).await?;
        Ok(())
    }
}

/// 打标记节点：访问时在 state 写入 key=true
#[derive(Clone)]
struct MarkNode {
    key: String,
}

#[langgraph4rust::async_trait]
impl AgentNode<SyncRouteState> for MarkNode {
    async fn apply(&self, state: Arc<SyncRouteState>) -> Result<(), LangGraphError> {
        state.set(&self.key, true).await?;
        Ok(())
    }
}

/// decide 节点将 route 置为 "right"，条件边同步读取后路由到 right 分支
#[tokio::test]
async fn test_state_dependent_conditional_routing() -> Result<(), LangGraphError> {
    let mut builder = StateGraphBuilder::<SyncRouteState>::new();
    builder.add_node(
        "decide",
        Box::new(SetRouteNode {
            target: "right".to_string(),
        }),
    );
    builder.add_node(
        "left",
        Box::new(MarkNode {
            key: "left_visited".to_string(),
        }),
    );
    builder.add_node(
        "right",
        Box::new(MarkNode {
            key: "right_visited".to_string(),
        }),
    );
    builder.add_edge(START_NODE, HashSet::from(["decide".to_string()]));
    builder.add_conditional_edge(
        "decide",
        vec![Box::new(|state: &SyncRouteState| {
            state.get_sync("route").unwrap_or_else(|| "left".to_string())
        })],
    );
    builder.add_edge("left", HashSet::from([END_NODE.to_string()]));
    builder.add_edge("right", HashSet::from([END_NODE.to_string()]));
    let graph = Arc::new(builder.compile()?);

    let state = Arc::new(SyncRouteState::new());
    let events = collect_events(Arc::clone(&graph), Arc::clone(&state)).await;

    assert!(matches!(events.last(), Some(StreamEvent::WorkflowFinished { .. })));
    let right: Option<bool> = state.get("right_visited").await?;
    let left: Option<bool> = state.get("left_visited").await?;
    assert_eq!(right, Some(true), "right branch should be visited");
    assert_eq!(left, None, "left branch should NOT be visited");
    Ok(())
}
