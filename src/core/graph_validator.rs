use std::collections::{HashMap, HashSet};

use crate::core::agent_node::AgentNode;
use crate::core::agent_state::AgentState;
use crate::core::error::LangGraphError;

pub struct ValidatedGraph<S: AgentState + Send + Sync> {
    pub max_steps: usize,
    pub nodes: HashMap<String, Box<dyn AgentNode<S>>>,
    pub edges: HashMap<String, HashSet<String>>,
    pub conditional_edges: HashMap<String, Vec<Box<dyn Fn(&S) -> String>>>,
    pub start_node: String,
    pub end_node: String,
}

pub struct GraphValidator<S: AgentState + Send + Sync> {
    pub max_steps: usize,
    pub nodes: HashMap<String, Box<dyn AgentNode<S>>>,
    pub edges: HashMap<String, HashSet<String>>,
    pub conditional_edges: HashMap<String, Vec<Box<dyn Fn(&S) -> String>>>,
    pub start_node: String,
    pub end_node: String,
}

impl<S: AgentState + Send + Sync> GraphValidator<S> {
    pub fn validate(self) -> Result<ValidatedGraph<S>, LangGraphError> {
        Self::validate_max_steps(self.max_steps)?;
        Self::validate_start_end_nodes(&self.start_node, &self.end_node)?;
        Self::validate_nodes_exist(&self.nodes)?;
        Self::validate_node_names_not_empty(&self.nodes)?;
        Self::validate_start_not_in_nodes(&self.start_node, &self.nodes)?;
        Self::validate_start_end_different(&self.start_node, &self.end_node)?;
        Self::validate_start_has_outgoing_edges(
            &self.start_node,
            &self.edges,
            &self.conditional_edges,
        )?;
        Self::validate_static_edges_valid(
            &self.edges,
            &self.nodes,
            &self.start_node,
            &self.end_node,
        )?;
        Self::validate_conditional_edges_valid(
            &self.conditional_edges,
            &self.nodes,
            &self.start_node,
        )?;
        Self::validate_no_mixed_edge_types(&self.edges, &self.conditional_edges)?;

        Ok(ValidatedGraph {
            max_steps: self.max_steps,
            nodes: self.nodes,
            edges: self.edges,
            conditional_edges: self.conditional_edges,
            start_node: self.start_node,
            end_node: self.end_node,
        })
    }

    fn validate_max_steps(max_steps: usize) -> Result<(), LangGraphError> {
        if max_steps <= 0 {
            return Err(LangGraphError::GraphError(
                "max_steps must be greater than 0".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_start_end_nodes(start_node: &str, end_node: &str) -> Result<(), LangGraphError> {
        if start_node.is_empty() {
            return Err(LangGraphError::GraphError(
                "Start node cannot be empty".to_string(),
            ));
        }
        if end_node.is_empty() {
            return Err(LangGraphError::GraphError(
                "End node cannot be empty".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_nodes_exist(
        nodes: &HashMap<String, Box<dyn AgentNode<S>>>,
    ) -> Result<(), LangGraphError> {
        if nodes.is_empty() {
            return Err(LangGraphError::GraphError(
                "Graph must contain at least one node".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_node_names_not_empty(
        nodes: &HashMap<String, Box<dyn AgentNode<S>>>,
    ) -> Result<(), LangGraphError> {
        for name in nodes.keys() {
            if name.is_empty() {
                return Err(LangGraphError::GraphError(
                    "Node name cannot be empty".to_string(),
                ));
            }
        }
        Ok(())
    }

    fn validate_start_not_in_nodes(
        start_node: &str,
        nodes: &HashMap<String, Box<dyn AgentNode<S>>>,
    ) -> Result<(), LangGraphError> {
        if nodes.contains_key(start_node) {
            return Err(LangGraphError::GraphError(format!(
                "Start node '{}' cannot be a normal node",
                start_node
            )));
        }
        Ok(())
    }

    fn validate_start_end_different(
        start_node: &str,
        end_node: &str,
    ) -> Result<(), LangGraphError> {
        if start_node == end_node {
            return Err(LangGraphError::GraphError(
                "Start node and end node cannot be the same".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_start_has_outgoing_edges(
        start_node: &str,
        edges: &HashMap<String, HashSet<String>>,
        conditional_edges: &HashMap<String, Vec<Box<dyn Fn(&S) -> String>>>,
    ) -> Result<(), LangGraphError> {
        let start_has_edge =
            edges.contains_key(start_node) || conditional_edges.contains_key(start_node);
        if !start_has_edge {
            return Err(LangGraphError::GraphError(format!(
                "Start node '{}' must have at least one outgoing edge",
                start_node
            )));
        }
        Ok(())
    }

    fn validate_static_edges_valid(
        edges: &HashMap<String, HashSet<String>>,
        nodes: &HashMap<String, Box<dyn AgentNode<S>>>,
        start_node: &str,
        end_node: &str,
    ) -> Result<(), LangGraphError> {
        for (from, targets) in edges {
            if from != start_node && from != end_node && !nodes.contains_key(from) {
                return Err(LangGraphError::GraphError(format!(
                    "Static edge source '{}' is not a registered node",
                    from
                )));
            }
            for target in targets {
                if target != start_node && target != end_node && !nodes.contains_key(target) {
                    return Err(LangGraphError::GraphError(format!(
                        "Static edge target '{}' (from '{}') is not a registered node",
                        target, from
                    )));
                }
            }
        }
        Ok(())
    }

    fn validate_conditional_edges_valid(
        conditional_edges: &HashMap<String, Vec<Box<dyn Fn(&S) -> String>>>,
        nodes: &HashMap<String, Box<dyn AgentNode<S>>>,
        start_node: &str,
    ) -> Result<(), LangGraphError> {
        for from in conditional_edges.keys() {
            if from != start_node && !nodes.contains_key(from) {
                return Err(LangGraphError::GraphError(format!(
                    "Conditional edge source '{}' is not a registered node",
                    from
                )));
            }
        }
        Ok(())
    }

    fn validate_no_mixed_edge_types(
        edges: &HashMap<String, HashSet<String>>,
        conditional_edges: &HashMap<String, Vec<Box<dyn Fn(&S) -> String>>>,
    ) -> Result<(), LangGraphError> {
        for from in edges.keys() {
            if conditional_edges.contains_key(from) {
                return Err(LangGraphError::GraphError(format!(
                    "Node '{}' cannot have both static edges and conditional edges",
                    from
                )));
            }
        }
        Ok(())
    }
}
