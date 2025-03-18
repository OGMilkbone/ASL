# Adaptive Schema Layer (ASL)

A revolutionary approach to schema evolution in distributed systems, providing zero downtime, seamless backward/forward compatibility, and elegant version management.

## Overview

ASL introduces a hybrid approach to schema management that combines intelligent schema evolution through differential schemas and lazy transformations. Instead of relying purely on schema registry tools or schema-on-read/write alone, ASL provides a dynamic layer that manages schema evolution in real-time.

### Core Features

- **Schema Deltas**: Store schemas as versioned diffs rather than whole copies
- **Universal Schema Index (USI)**: Distributed metadata store for schema deltas and compatibility rules
- **Dynamic, Lazy Schema Transformation (DLST)**: Runtime schema materialization and transformation
- **Zero Downtime Evolution**: Producers and consumers evolve independently
- **Built-in Compatibility**: Automatic backward and forward compatibility

## Getting Started

### Prerequisites

- Python 3.8+
- Redis (for USI implementation)
- Docker (optional, for containerized deployment)

### Installation

```bash
pip install -r requirements.txt
```

### Quick Start

```python
from asl import SchemaRegistry, SchemaDelta

# Initialize the schema registry
registry = SchemaRegistry()

# Define a schema delta
delta = SchemaDelta(
    added=["firstName", "lastName"],
    removed=["name"],
    transformations={
        "firstName": "split(name, ' ')[0]",
        "lastName": "split(name, ' ')[1]"
    }
)

# Register the schema delta
registry.register_delta("user", "v2", delta)
```

## Architecture

### Schema Deltas
Schemas are stored as versioned diffs, making evolution lightweight and efficient. Each schema change produces a minimal diff that captures only the changes.

### Universal Schema Index (USI)
A distributed, highly available metadata store that manages schema deltas and compatibility rules. The USI provides fast lookups and automatic compatibility resolution.

### Dynamic, Lazy Schema Transformation (DLST)
Schemas are materialized at read-time, with transformations being cached and optimized for subsequent reads.

## Development

### Project Structure

```
asl/
├── core/           # Core ASL functionality
│   ├── registry/   # Schema registry implementation
│   ├── delta/      # Schema delta management
│   └── transform/  # Schema transformation logic
├── usi/            # Universal Schema Index
├── cache/          # Caching layer
└── utils/          # Utility functions
```

### Running Tests

```bash
pytest tests/
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details. 