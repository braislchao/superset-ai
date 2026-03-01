# SupersetAI

A natural language interface for creating Apache Superset dashboards using LLM-powered agents.

## Features

- Chat with an AI agent to create datasets, charts, and dashboards
- MCP server for integration with Claude Desktop, Cursor, VS Code, etc.
- Supports 5 chart types: bar, line, pie, table, and big number/KPI
- Automatic asset reuse (finds existing datasets/charts when possible)
- Session-based memory for multi-turn conversations

## Quick Start

```bash
# Create virtual environment
python3.11 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -e ".[dev]"

# Start Superset (Docker required)
docker-compose up -d

# Configure environment
cp .env.example .env
# Edit .env with your settings

# Test connection
superset-ai test-connection

# Start chatting
superset-ai chat
```

## MCP Server

Expose Superset tools via the Model Context Protocol:

```bash
# stdio transport (for Claude Desktop, Cursor, etc.)
superset-ai mcp

# HTTP transport
superset-ai mcp -t http -p 8000

# Or via fastmcp CLI
fastmcp run superset_ai.mcp.server:mcp --transport stdio
```

## Usage

```bash
# Interactive chat mode (requires superset-ai[agent])
superset-ai chat

# Test Superset connection
superset-ai test-connection

# List available databases
superset-ai list-databases

# Start MCP server
superset-ai mcp
```

## Development

```bash
# Run tests
pytest

# Run linter
ruff check src/

# Type checking
mypy src/
```
