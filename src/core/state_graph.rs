use crate::core::agent_node::AgentNode;
use crate::core::agent_state::AgentState;
use crate::core::error::LangGraphError;
use std::collections::{HashMap, HashSet};
use std::future::ready;
use std::sync::Arc;
use tokio::task::JoinSet;

/// 状态图：编译后的只读图，用于执行工作流
pub struct StateGraph<S: AgentState> {
    nodes: HashMap<String, Box<dyn AgentNode<S>>>,
    edges: HashMap<String, HashSet<String>>,
    conditional_edges: HashMap<String, Vec<Box<dyn Fn(&S) -> String>>>,
    start_node: String,
    end_node: String,
    max_steps: usize,
}

impl<S: AgentState> StateGraph<S> {
    /// 创建新的状态图（仅供 builder 模块使用）
    pub(crate) fn new(
        max_steps: usize,
        nodes: HashMap<String, Box<dyn AgentNode<S>>>,
        edges: HashMap<String, HashSet<String>>,
        conditional_edges: HashMap<String, Vec<Box<dyn Fn(&S) -> String>>>,
        start_node: String,
        end_node: String,
    ) -> Self {
        StateGraph {
            max_steps,
            nodes,
            edges,
            conditional_edges,
            start_node,
            end_node,
        }
    }

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