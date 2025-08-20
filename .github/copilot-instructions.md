# GitHub Copilot Instructions for StarRocks

## Project Overview

StarRocks is an open-source, high-performance analytical database system designed for real-time analytics. This is a large-scale, multi-language project with complex architecture spanning C++, Java, and Python components.

## 🚨 Critical Build System Warning

**DO NOT attempt to build or run unit tests for this project unless explicitly requested by the user.**
- Building is extremely resource-intensive and time-consuming (hours)
- Focus on code analysis, reading, and targeted changes only
- Avoid running `build.sh`, `run-*-ut.sh`, or any make commands

## Project Architecture

### Backend (be/) - C++
**Purpose**: Core analytical engine and storage layer
- **Performance Critical**: Focus on memory management, SIMD optimizations, and efficient algorithms
- **Key Components**:
  - `be/src/exec/` - Query execution engine (vectorized processing)
  - `be/src/storage/` - Storage engine (columnar storage, indexes)
  - `be/src/exprs/` - Expression evaluation and JIT compilation
  - `be/src/runtime/` - Runtime components (memory management, stream processing)
  - `be/src/connector/` - External data source connectors
  - `be/src/formats/` - Data format parsers (Parquet, ORC, etc.)

### Frontend (fe/) - Java
**Purpose**: SQL parsing, query planning, and metadata management
- **Key Components**:
  - `fe/fe-core/src/main/java/com/starrocks/sql/` - SQL parser and analyzer
  - `fe/fe-core/src/main/java/com/starrocks/planner/` - Query planner and optimizer
  - `fe/fe-core/src/main/java/com/starrocks/catalog/` - Metadata catalog management
  - `fe/fe-core/src/main/java/com/starrocks/service/` - Core frontend services

### Java Extensions (java-extensions/) - Java
**Purpose**: External connectors and data format support
- `java-extensions/hive-reader/` - Hive integration
- `java-extensions/iceberg-metadata-reader/` - Apache Iceberg support
- `java-extensions/jdbc-bridge/` - JDBC connectivity

### Testing (test/) - Python
**Purpose**: Integration and SQL testing framework
- `test/sql/` - SQL test cases organized by functionality
- `test/lib/` - Test framework libraries

## Code Review Guidelines

### C++ Code Review (Backend)
- **Memory Management**: Check for proper RAII, smart pointers, and memory leak prevention
- **Performance**: Look for vectorization opportunities, cache-friendly data structures
- **Thread Safety**: Verify proper synchronization and lock-free algorithms where appropriate
- **Error Handling**: Ensure proper exception handling and status code propagation
- **Style**: Follow Google C++ style guide principles

### Java Code Review (Frontend & Extensions)
- **SQL Processing**: Focus on correctness of parsing, semantic analysis, and optimization rules
- **Resource Management**: Check for proper connection pooling and resource cleanup
- **Concurrency**: Verify thread-safe operations and proper synchronization
- **Error Handling**: Ensure comprehensive exception handling with meaningful messages
- **Performance**: Look for unnecessary object creation and inefficient collections usage

### Python Code Review (Tests)
- **Test Coverage**: Ensure comprehensive test scenarios including edge cases
- **SQL Correctness**: Validate SQL syntax and expected results
- **Test Organization**: Follow existing test structure and naming conventions
- **Error Cases**: Include negative test cases and error condition validation

## PR Title and Commit Message Standards

### Required PR Title Prefixes
- `[BugFix]` - Bug fixes and error corrections
- `[Enhancement]` - Improvements to existing functionality
- `[Feature]` - New features and capabilities
- `[Refactor]` - Code refactoring without functional changes
- `[UT]` - Unit test related changes
- `[Doc]` - Documentation updates
- `[Tool]` - Build system and tooling changes


## Review Focus Areas

### Performance Critical Areas
- Query execution pipelines (vectorized operations)
- Memory allocation patterns and object pooling
- Lock contention and synchronization overhead
- Data serialization/deserialization efficiency
- Cache utilization and data locality

### Correctness Critical Areas
- SQL semantic analysis and type checking
- Query optimization rule correctness
- Data format compatibility and schema evolution
- Transaction isolation and consistency
- Error propagation and recovery mechanisms

### Security Considerations
- Input validation and SQL injection prevention
- Authentication and authorization mechanisms
- Resource limit enforcement
- Secure data access patterns

## Common Patterns and Anti-patterns

### Good Patterns
- Use StatusOr<T> for error handling in C++
- Implement proper resource cleanup with RAII
- Use vectorized processing for data operations
- Follow existing naming conventions and code organization
- Write comprehensive unit tests for new functionality

### Anti-patterns to Avoid
- Manual memory management in C++ (prefer smart pointers)
- Blocking operations on main threads
- Inefficient string operations in hot paths
- Missing error handling or swallowing exceptions
- Breaking existing API contracts without proper deprecation

## Testing Philosophy
- **Integration Tests**: Focus on end-to-end SQL functionality
- **Unit Tests**: Test individual components in isolation
- **Performance Tests**: Benchmark critical path operations
- **Compatibility Tests**: Ensure backward compatibility with existing data

## Documentation Requirements
- Update relevant documentation for user-facing changes
- Add code comments for complex algorithms or business logic
- Include examples for new SQL features or functions
- Update API documentation for interface changes

## Language-Specific Guidelines

### C++ (Backend)
- Use modern C++ features (C++17/20) appropriately
- Prefer const-correctness and immutable data structures
- Use appropriate STL containers and algorithms
- Follow RAII principles for resource management
- Write cache-friendly code with good data locality

### Java (Frontend)
- Use appropriate design patterns (Builder, Factory, Strategy)
- Leverage Java streams and functional programming where beneficial
- Ensure proper exception handling hierarchy
- Use appropriate concurrent collections for thread-safe operations
- Follow defensive programming practices

### Python (Tests)
- Write clear, readable test cases with descriptive names
- Use appropriate test fixtures and setup/teardown
- Follow PEP 8 style guidelines
- Include both positive and negative test scenarios
- Use meaningful assertion messages

## Architecture Decisions
When reviewing architectural changes, consider:
- Impact on query performance and scalability
- Backward compatibility with existing deployments
- Resource utilization and memory footprint
- Maintainability and code complexity
- Integration with existing components

## Code Quality Metrics
- Cyclomatic complexity should be reasonable
- Functions should have single responsibility
- Code should be self-documenting with clear variable names
- Error messages should be actionable and user-friendly
- Performance-critical paths should be optimized and profiled