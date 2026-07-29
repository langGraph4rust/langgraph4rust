use std::collections::{HashMap, HashSet};

use crate::core::agent_node::AgentNode;
use crate::core::agent_state::AgentState;
use crate::core::error::LangGraphError;
use crate::core::graph_validator::{GraphValidator, ValidatedGraph};
use super::state_graph::StateGraph;

pub const START_NODE: &str = "__start__";
pub const END_NODE: &str = "__end__";

/// 图构建器：用于构建图结构，compile 后生成 StateGraph
pub struct StateGraphBuilder<S: AgentState + Send + Sync> {
    nodes: HashMap<String, Box<dyn AgentNode<S>>>,
    edges: HashMap<String, HashSet<String>>,
    conditional_edges: HashMap<String, Vec<Box<dyn Fn(&S) -> String>>>,
    start_node: String,
    end_node: String,
    max_steps: usize,
}

impl<S: AgentState + Send + Sync> StateGraphBuilder<S> {
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

        let validated: ValidatedGraph<S> = validator.validate()?;

        Ok(StateGraph::new(
            validated.max_steps,
            validated.nodes,
            validated.edges,
            validated.conditional_edges,
            validated.start_node,
            validated.end_node,
        ))
    }
}