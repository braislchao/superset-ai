# SupersetAI

A natural language interface for creating Apache Superset dashboards using LLM-powered agents.

## Features

- Chat with an AI agent to create datasets, charts, and dashboards
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
# Edit .env and add your OPENAI_API_KEY

# Test connection
supersetai test-connection

# Start chatting
supersetai chat
```

## Usage

```bash
# Interactive chat mode
supersetai chat

# Test Superset connection
supersetai test-connection

# List available databases
supersetai list-databases
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
