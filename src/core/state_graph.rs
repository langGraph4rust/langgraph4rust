use crate::core::agent_node::AgentNode;
use crate::core::agent_state::AgentState;
use crate::core::error::LangGraphError;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex as TokioMutex;
use tokio::task::JoinSet;

pub const START_NODE: &str = "__start__";
pub const END_NODE: &str = "__end__";

/// 图构建器：用于构建图结构，compile 后生成 StateGraph
pub struct StateGraphBuilder<S: AgentState> {
    nodes: HashMap<String, Box<dyn AgentNode<S>>>,
    edges: HashMap<String, HashSet<String>>,
    conditional_edges: HashMap<String, HashSet<Box<dyn Fn(&S) -> String>>>,
    start_node: String,
    end_node: String,
    max_steps: usize,
}

/// 状态图：编译后的只读图，用于执行工作流
pub struct StateGraph<S: AgentState> {
    nodes: HashMap<String, Box<dyn AgentNode<S>>>,
    edges: HashMap<String, HashSet<String>>,
    conditional_edges: HashMap<String, HashSet<Box<dyn Fn(&S) -> String>>>,
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

    pub fn add_edge(&mut self, from: &str, to:HashSet<String>) -> &mut Self {
        self.edges.insert(from.to_string(), to);
        self
    }

    pub fn add_conditional_edge<F>(&mut self, from: &str, routers: HashSet<Box<dyn Fn(&S) -> String>>) -> &mut Self
    {
        self.conditional_edges.insert(from.to_string(), routers);
        self
    }

    pub fn set_start_node(&mut self, start_node: &str) -> &mut Self {
        self.start_node =
            start_node.to_string();
        self
    }

    pub fn set_end_node(&mut self, end_node: &str) -> &mut Self {
        self.end_node = end_node.to_string();
        self
    }

    /// 编译图：消费 builder，校验合法性后生成不可变的 StateGraph
    pub fn compile(self) -> Result<StateGraph<S>, LangGraphError> {
        // 1. 节点不能为空
        if self.nodes.is_empty() {
            return Err(LangGraphError::GraphError("Graph must contain at least one node".to_string()));
        }

        // 2. start_node 必须有出边（静态边或条件边）
        let start_has_edge = self.edges.contains_key(&self.start_node)
            || self.conditional_edges.contains_key(&self.start_node);
        if !start_has_edge {
            return Err(LangGraphError::GraphError(
                format!("Start node '{}' must have at least one outgoing edge", self.start_node),
            ));
        }

        // 3. 静态边的源/目标必须是已注册节点（或 START_NODE/END_NODE）
        for (from, targets) in &self.edges {
            if from != &self.start_node && from != &self.end_node && !self.nodes.contains_key(from) {
                return Err(LangGraphError::GraphError(
                    format!("Static edge source '{}' is not a registered node", from),
                ));
            }
            for target in targets {
                if target != &self.start_node && target != &self.end_node && !self.nodes.contains_key(target) {
                    return Err(LangGraphError::GraphError(
                        format!("Static edge target '{}' (from '{}') is not a registered node", target, from),
                    ));
                }
            }
        }

        // 4. 条件边的源必须是已注册节点（或 START_NODE）
        for from in self.conditional_edges.keys() {
            if from != &self.start_node && !self.nodes.contains_key(from) {
                return Err(LangGraphError::GraphError(
                    format!("Conditional edge source '{}' is not a registered node", from),
                ));
            }
        }

        // 5. 同一节点禁止同时配置静态边和条件边
        for from in self.edges.keys() {
            if self.conditional_edges.contains_key(from) {
                return Err(LangGraphError::GraphError(
                    format!("Node '{}' cannot have both static edges and conditional edges", from),
                ));
            }
        }

        Ok(StateGraph {
            max_steps: self.max_steps,
            nodes: self.nodes,
            edges: self.edges,
            conditional_edges: self.conditional_edges,
            start_node: self.start_node,
            end_node: self.end_node,
        })
    }
}


impl<S: AgentState> StateGraph<S> {
    /// 执行图：从 start_node 开始，依次执行节点直到 end_node

    fn get_node_by_key(&self, key: &String) -> Result<&Box<dyn AgentNode<S>>, LangGraphError> {

        Ok(self.nodes.get(key).ok_or_else(|| LangGraphError::NotFound(format!("Key '{}' not found", key)))?)
    }

    fn get_node_by_keys(&self, keys: &HashSet<String>) -> Result<Vec<&Box<dyn AgentNode<S>>>, LangGraphError> {
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

    fn get_next_node_key(&self, keys: &HashSet<String>, state: &S) -> Result<HashSet<String>, LangGraphError> {
        if keys.is_empty() {
            return Ok(HashSet::new());
        }
        let mut next_node_keys = HashSet::new();
        for key in keys {
            // 静态边：直接收集目标节点
            if let Some(targets) = self.edges.get(key) {
                for target in targets {
                    next_node_keys.insert(target.clone());
                }
            }
            // 条件边：调用 router 求值目标节点
            if let Some(routers) = self.conditional_edges.get(key) {
                for router in routers {
                    let target = router(state);
                    if target != END_NODE && !self.nodes.contains_key(&target) {
                        return Err(LangGraphError::GraphError(
                            format!("Conditional edge from '{}' returned invalid target '{}'", key, target),
                        ));
                    }
                    next_node_keys.insert(target);
                }
            }
        }
        Ok(next_node_keys)
    }
    pub fn invoke(&self, state: &mut S) -> Result<(), LangGraphError> {
        let mut current = HashSet::new();
        current.insert(self.start_node.to_string());

        let mut step_count: usize = 0;
        let max_steps = self.max_steps;

        loop {
            if step_count > max_steps {
                break
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
                    return Err(LangGraphError::NotFound(
                        format!("Dead-end: nodes {:?} have no find by keys", current),
                    ));
                }
                // for node in nodes {
                //     node.apply(state)?;
                // }
            }
            let next = self.get_next_node_key(&current, state)?;
            if next.is_empty() {
                return Err(LangGraphError::GraphError(
                    format!("Dead-end: nodes {:?} have no outgoing edges", current),
                ));
            }
            current = next;
        }
        Ok(())
    }

    async fn batch_apply(
        &self,
        nodes: Vec<Box<dyn AgentNode<S>>>,
        state: S,
    ) -> Result<(), LangGraphError>
    {
        let mut batch_tasks: JoinSet<Result<(), LangGraphError>> = JoinSet::new();
        nodes.into_iter().for_each(|node| {
            let ret = node.apply(state);
            batch_tasks.spawn(ret);
        });
        Ok(())
    }

}