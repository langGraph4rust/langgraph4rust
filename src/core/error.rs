use std::error::Error;
use std::fmt;
use std::fmt::Display;

/// Comprehensive error type for the langgraph4rust workflow engine.
///
/// This enum represents all possible errors that can occur during workflow
/// construction and execution. Each variant provides specific context about
/// what went wrong, making debugging easier.
///
/// # Error Categories
///
/// - **Runtime Errors**: [`NodeError`], [`StateError`] - occur during execution
/// - **Construction Errors**: [`GraphError`] - occur when building/validating graphs
/// - **Lookup Errors**: [`NotFound`] - occur when accessing missing resources
/// - **System Errors**: [`Timeout`], [`RetryExhausted`] - infrastructure issues
///
/// # Example
///
/// ```rust
/// use langgraph4rust::LangGraphError;
///
/// fn handle_error(err: LangGraphError) {
///     match &err {
///         LangGraphError::NodeError(msg) => eprintln!("Node failed: {}", msg),
///         LangGraphError::StateError(msg) => eprintln!("State issue: {}", msg),
///         LangGraphError::GraphError(msg) => eprintln!("Graph invalid: {}", msg),
///         LangGraphError::NotFound(msg) => eprintln!("Missing resource: {}", msg),
///         LangGraphError::Timeout(msg) => eprintln!("Operation timed out: {}", msg),
///         LangGraphError::RetryExhausted(msg) => eprintln!("Gave up after retries: {}", msg),
///     }
/// }
/// ```
///
/// # Conversions
///
/// The type implements `From<String>` and `From<&str>` for convenience,
/// converting to [`LangGraphError::NodeError`] by default:
///
/// ```rust
/// use langgraph4rust::LangGraphError;
///
/// // These are equivalent
/// let err1: LangGraphError = "something went wrong".into();
/// let err2: LangGraphError = String::from("something went wrong").into();
/// ```
///
/// [`NodeError`]: LangGraphError::NodeError
/// [`StateError`]: LangGraphError::StateError
/// [`GraphError`]: LangGraphError::GraphError
/// [`NotFound`]: LangGraphError::NotFound
/// [`Timeout`]: LangGraphError::Timeout
/// [`RetryExhausted`]: LangGraphError::RetryExhausted
#[derive(Debug)]
pub enum LangGraphError {
    /// Error occurred during node execution.
    ///
    /// This is returned when a node's [`AgentNode::apply`](crate::AgentNode::apply) method returns an error,
    /// or when the engine encounters issues executing a node (e.g., panic recovery).
    ///
    /// # Common Causes
    ///
    /// - Node logic encountered invalid state
    /// - External service call failed
    /// - Business rule violation
    /// - Unexpected panic in async code
    ///
    /// # Example
    ///
    /// ```rust
    /// # use langgraph4rust::LangGraphError;
    /// # fn example() -> Result<(), LangGraphError> {
    /// return Err(LangGraphError::NodeError("Database connection failed".to_string()));
    /// # }
    /// ```
    NodeError(String),

    /// Error related to state management operations.
    ///
    /// Returned when reading from or writing to the state fails. This typically
    /// involves serialization/deserialization issues or storage backend problems.
    ///
    /// # Common Causes
    ///
    /// - Type mismatch during deserialization (e.g., stored string, tried to read as i32)
    /// - JSON serialization failure
    /// - Storage backend I/O error
    /// - Concurrent access conflicts
    ///
    /// # Example
    ///
    /// ```rust
    /// # use langgraph4rust::LangGraphError;
    /// # fn example() -> Result<(), LangGraphError> {
    /// return Err(LangGraphError::StateError(
    ///     "Cannot deserialize 'hello' as i32".to_string()
    /// ));
    /// # }
    /// ```
    StateError(String),

    /// Error in graph structure or validation.
    ///
    /// Returned during graph construction ([`StateGraphBuilder::compile`](crate::StateGraphBuilder::compile)) when
    /// the graph definition violates structural constraints.
    ///
    /// # Common Causes
    ///
    /// - Empty graph (no nodes defined)
    /// - Invalid edge references (pointing to non-existent nodes)
    /// - Missing required edges (start/end nodes disconnected)
    /// - Circular dependencies without max_steps limit
    /// - Disconnected components (isolated nodes)
    ///
    /// # Example
    ///
    /// ```rust
    /// # use langgraph4rust::LangGraphError;
    /// # fn example() -> Result<(), LangGraphError> {
    /// return Err(LangGraphError::GraphError(
    ///     "Node 'processor' has no outgoing edges".to_string()
    /// ));
    /// # }
    /// ```
    GraphError(String),

    /// Requested resource not found.
    ///
    /// Returned when attempting to access a node, edge, or other graph element
    /// that doesn't exist in the compiled graph.
    ///
    /// # Common Causes
    ///
    /// - Referencing a node that was never added
    /// - Conditional edge router returns invalid target name
    /// - Typo in node name string
    ///
    /// # Example
    ///
    /// ```rust
    /// # use langgraph4rust::LangGraphError;
    /// # fn example() -> Result<(), LangGraphError> {
    /// return Err(LangGraphError::NotFound(
    ///     "Node 'missing_node' not found in graph".to_string()
    /// ));
    /// # }
    /// ```
    NotFound(String),

    /// Operation exceeded time limit.
    ///
    /// Returned when a node or entire graph execution takes longer than the
    /// configured timeout duration.
    ///
    /// # Note
    ///
    /// Timeout support depends on configuration. Not all setups enforce timeouts.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use langgraph4rust::LangGraphError;
    /// # fn example() -> Result<(), LangGraphError> {
    /// return Err(LangGraphError::Timeout(
    ///     "Node 'slow_api' exceeded 30s limit".to_string()
    /// ));
    /// # }
    /// ```
    Timeout(String),

    /// Maximum retry attempts exhausted.
    ///
    /// When automatic retry is configured and a transient failure keeps occurring,
    /// this error indicates all retry attempts have been used.
    ///
    /// # Note
    ///
    /// Retry support depends on configuration. Basic setups may not include retries.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use langgraph4rust::LangGraphError;
    /// # fn example() -> Result<(), LangGraphError> {
    /// return Err(LangGraphError::RetryExhausted(
    ///     "Failed after 3 attempts: API rate limited".to_string()
    /// ));
    /// # }
    /// ```
    RetryExhausted(String),
}

impl Display for LangGraphError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LangGraphError::NodeError(msg) => write!(f, "Node error: {}", msg),
            LangGraphError::StateError(msg) => write!(f, "State error: {}", msg),
            LangGraphError::GraphError(msg) => write!(f, "Graph error: {}", msg),
            LangGraphError::NotFound(msg) => write!(f, "Not found: {}", msg),
            LangGraphError::Timeout(msg) => write!(f, "Timeout: {}", msg),
            LangGraphError::RetryExhausted(msg) => write!(f, "Retry exhausted: {}", msg),
        }
    }
}

impl Error for LangGraphError {}

impl From<String> for LangGraphError {
    /// Convert a `String` into a [`LangGraphError::NodeError`].
    ///
    /// This is a convenience conversion for quickly creating errors from string messages.
    /// The resulting error will always be a `NodeError` variant.
    ///
    /// # Example
    ///
    /// ```rust
    /// use langgraph4rust::LangGraphError;
    ///
    /// let error: LangGraphError = "Something failed".to_string().into();
    /// assert!(matches!(error, LangGraphError::NodeError(_)));
    /// ```
    fn from(msg: String) -> Self {
        LangGraphError::NodeError(msg)
    }
}

impl From<&str> for LangGraphError {
    /// Convert a string slice (`&str`) into a [`LangGraphError::NodeError`].
    ///
    /// This is the most convenient way to create errors from string literals.
    /// The resulting error will always be a `NodeError` variant.
    ///
    /// # Example
    ///
    /// ```rust
    /// use langgraph4rust::LangGraphError;
    ///
    /// let error: LangGraphError = "Something failed".into();
    /// assert!(matches!(error, LangGraphError::NodeError(_)));
    /// ```
    fn from(msg: &str) -> Self {
        LangGraphError::NodeError(msg.to_string())
    }
}
