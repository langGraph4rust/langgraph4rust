# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-07-29

### Added
- Initial release of langgraph4rust
- `StateGraphBuilder` for declarative workflow construction
- `StateGraph` for compiled, executable workflows
- `AgentNode` trait for custom node implementations
- `DefaultMemoryState` with JSON-based state persistence
- Support for parallel node execution
- Conditional routing based on state conditions
- Comprehensive graph validation before execution
- Full async support via Tokio runtime

### Examples
- Hello World example demonstrating basic workflow
- Conditional routing example showing dynamic path selection
- Parallel execution example for concurrent processing
- Custom state implementation example
- Data pipeline example for multi-stage processing
- Error handling strategies example

### Documentation
- Complete API documentation with doc comments
- README with quick start guide and examples
- Inline code examples throughout the codebase

[Unreleased]: https://github.com/langGraph4rust/langgrap4rust/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/langGraph4rust/langgrap4rust/releases/tag/v0.1.0