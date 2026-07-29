use crate::core::agent_node::AgentNode;
use crate::core::agent_state::AgentState;
use crate::core::error::LangGraphError;
use crate::core::graph_validator::GraphValidator;
use std::collections::{HashMap, HashSet};
use std::future::ready;
use std::ops::Deref;
use std::sync::Arc;
use tokio::task::JoinSet;

pub const START_NODE: &str = "__start__";
pub const END_NODE: &str = "__end__";

/// 图构建器：用于构建图结构，compile 后生成 StateGraph
pub struct StateGraphBuilder<S: AgentState> {
    nodes: HashMap<String, Box<dyn AgentNode<S>>>,
    edges: HashMap<String, HashSet<String>>,
    conditional_edges: HashMap<String, Vec<Box<dyn Fn(&S) -> String>>>,
    start_node: String,
    end_node: String,
    max_steps: usize,
}

/// 状态图：编译后的只读图，用于执行工作流
pub struct StateGraph<S: AgentState> {
    nodes: HashMap<String, Box<dyn AgentNode<S>>>,
    edges: HashMap<String, HashSet<String>>,
    conditional_edges: HashMap<String, Vec<Box<dyn Fn(&S) -> String>>>,
    start_node: String,
    end_node: String,
    max_steps: usize,
}

impl<S: AgentState> StateGraphBuilder<S> {
    pub fn new() -> Self {
        StateGraphBuilder {
            max_steps: usize::MAX,
            nodes: Default::default(),
            edges: Default::default(),
            conditional_edges: Default::default(),
            start_node: START_NODE.to_string(),
            end_node: END_NODE.to_string(),
        }
    }
    pub fn set_max_steps(&mut self, max_steps: usize) -> &mut Self {
        self.max_steps = max_steps;
        self
    }

    pub fn add_node(&mut self, name: &str, node: Box<dyn AgentNode<S>>) -> &mut Self {
        self.nodes.insert(name.to_string(), node);
        self
    }

    pub fn add_edge(&mut self, from: &str, to: HashSet<String>) -> &mut Self {
        self.edges.insert(from.to_string(), to);
        self
    }

    pub fn add_conditional_edge(
        &mut self,
        from: &str,
        routers: Vec<Box<dyn Fn(&S) -> String>>,
    ) -> &mut Self {
        self.conditional_edges.insert(from.to_string(), routers);
        self
    }

    pub fn set_start_node(&mut self, start_node: &str) -> &mut Self {
        self.start_node = start_node.to_string();
        self
    }

    pub fn set_end_node(&mut self, end_node: &str) -> &mut Self {
        self.end_node = end_node.to_string();
        self
    }

    /// 编译图：消费 builder，校验合法性后生成不可变的 StateGraph
    pub fn compile(self) -> Result<StateGraph<S>, LangGraphError> {
        let validator = GraphValidator {
            max_steps: self.max_steps,
            nodes: self.nodes,
            edges: self.edges,
            conditional_edges: self.conditional_edges,
            start_node: self.start_node,
            end_node: self.end_node,
        };

        let (max_steps, nodes, edges, conditional_edges, start_node, end_node) = validator.validate()?;

        Ok(StateGraph {
            max_steps,
            nodes,
            edges,
            conditional_edges,
            start_node,
            end_node,
        })
    }
}

impl<S: AgentState> StateGraph<S> {
    /// 执行图：从 start_node 开始，依次执行节点直到 end_node

    fn get_node_by_key(&self, key: &String) -> Result<&Box<dyn AgentNode<S>>, LangGraphError> {
        Ok(self
            .nodes
            .get(key)
            .ok_or_else(|| LangGraphError::NotFound(format!("Key '{}' not found", key)))?)
    }

    fn get_node_by_keys(
        &self,
        keys: &HashSet<String>,
    ) -> Result<Vec<&Box<dyn AgentNode<S>>>, LangGraphError> {
        let mut nodes = Vec::new();
        for key in keys {
            let node = self.get_node_by_key(key)?;
            nodes.push(node);
        }
        Ok(nodes)
    }

    fn is_start_node(&self, keys: HashSet<String>) -> Result<bool, LangGraphError> {
        if keys.is_empty() {
            return Ok(false);
        }
        if keys.contains(&self.start_node) {
            return Ok(true);
        }
        Ok(false)
    }

    fn is_end_node(&self, keys: HashSet<String>) -> Result<bool, LangGraphError> {
        if keys.is_empty() {
            return Ok(false);
        }
        if keys.contains(&self.end_node) {
            return Ok(true);
        }
        Ok(false)
    }

    fn get_next_node_key(
        &self,
        keys: &HashSet<String>,
        state: &S,
    ) -> Result<HashSet<String>, LangGraphError> {
        if keys.is_empty() {
            return Ok(HashSet::new());
        }
        let mut next_node_keys = HashSet::new();
        for key in keys {
            // 静态边：直接收集目标节点
            if let Some(targets) = self.edges.get(key) {
                if !targets.is_empty() {
                    for target in targets {
                        next_node_keys.insert(target.clone());
                    }
                }
            }
            // 条件边：调用 router 求值目标节点
            if let Some(routers) = self.conditional_edges.get(key) {
                for router in routers {
                    next_node_keys.insert(router(state));
                }
            }
        }
        Ok(next_node_keys)
    }
    pub async fn invoke(&self, state: Arc<S>) -> Result<(), LangGraphError> {
        let mut current = HashSet::new();
        current.insert(self.start_node.to_string());

        let mut step_count: usize = 0;
        let max_steps = self.max_steps;

        loop {
            if step_count>= max_steps {
                break;
            }
            step_count = step_count + 1;
            if self.is_end_node(current.clone())? {
                current.remove(&self.end_node);
            }
            if current.is_empty() {
                break;
            }
            if !self.is_start_node(current.clone())? {
                let nodes = self.get_node_by_keys(&current)?;
                if nodes.is_empty() {
                    return Err(LangGraphError::NotFound(format!(
                        "Dead-end: nodes {:?} have no find by keys",
                        current
                    )));
                }
                self.batch_apply(nodes, Arc::clone(&state)).await?;
            }
            let next = self.get_next_node_key(&current, state.as_ref())?;
            if next.is_empty() {
                return Err(LangGraphError::GraphError(format!(
                    "Dead-end: nodes {:?} have no outgoing edges",
                    current
                )));
            }
            current = next;
        }
        Ok(())
    }


    async fn batch_apply(
        &self,
        nodes: Vec<&Box<dyn AgentNode<S>>>,
        state: Arc<S>,
    ) -> Result<(), LangGraphError> {
        let mut batch_tasks: JoinSet<Result<(), LangGraphError>> = JoinSet::new();
        for node in nodes {
            let ret = ready(node.apply(Arc::clone(&state)));
            batch_tasks.spawn(ret);
        }
        let results = batch_tasks.join_all().await;
        for result in results {
            if let Err(e) = result {
                return Err(e);
            }
        }
        Ok(())
    }
}