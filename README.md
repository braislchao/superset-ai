# superset-ai

A Python library and LangGraph agent for managing Apache Superset via natural language.

## Features

- **16 chart types**: bar, line, pie, table, big number (KPI), area, big number with trendline, timeseries bar, bubble, funnel, gauge, treemap, histogram, box plot, heatmap
- **Full CRUD**: create, read, update, delete for charts and dashboards
- **Tabbed dashboards**: organize charts into named tabs
- **Native filters**: add/remove/list interactive filters (select, range, time, etc.)
- **Color schemes**: 12 built-in color schemes for dashboards
- **Dataset management**: find or create datasets, list columns, discover databases/schemas/tables
- **Automatic asset reuse**: finds existing datasets and charts when possible
- **Session-based memory**: multi-turn conversations with context (agent mode)

## Quick Start

```bash
# Create virtual environment
python3.13 -m venv .venv
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

# Start chatting (requires agent extras)
superset-ai chat
```

## LangChain Agent

Embed the Superset agent in your Python application:

```python
from superset_ai.agent.graph import create_graph
from superset_ai.agent.tools import set_tool_context

# Create the agent graph
graph = create_graph()

# Set tool context (provides service instances)
set_tool_context(your_context)

# Invoke the agent
result = await graph.ainvoke({"messages": [("user", "Create a bar chart of sales by region")]})
```

## CLI

```bash
# Interactive chat mode (requires superset-ai[agent])
superset-ai chat

# Test Superset connection
superset-ai test-connection

# List available databases
superset-ai list-databases
```

## Configuration

All settings use the `SUPERSET_AI_` environment variable prefix:

| Variable | Default | Description |
|----------|---------|-------------|
| `SUPERSET_AI_SUPERSET_BASE_URL` | `http://localhost:8088` | Superset instance URL |
| `SUPERSET_AI_SUPERSET_USERNAME` | `admin` | Superset username |
| `SUPERSET_AI_SUPERSET_PASSWORD` | `admin` | Superset password |
| `SUPERSET_AI_LLM_PROVIDER` | `copilot` | LLM provider (`copilot` or `openai`) |
| `SUPERSET_AI_COPILOT_MODEL` | `gpt-4o` | Model for GitHub Copilot provider |
| `SUPERSET_AI_OPENAI_API_KEY` | — | OpenAI API key (when using `openai` provider) |
| `SUPERSET_AI_OPENAI_MODEL` | `gpt-4o` | Model for OpenAI provider |
| `SUPERSET_AI_REQUEST_TIMEOUT` | `30` | HTTP request timeout (seconds) |
| `SUPERSET_AI_MAX_RETRIES` | `3` | Max retry attempts |
| `SUPERSET_AI_LOG_LEVEL` | `INFO` | Log level |

## Architecture

```
Agent tools (agent/tools.py) ──→  operations/  ──→  api/  ──→  Superset REST API
```

- **`api/`** — HTTP client and service classes that talk to the Superset REST API
- **`schemas/`** — Chart/dashboard payload builders and layout generators
- **`operations/`** — Shared business logic (pure functions, no side effects)
- **`agent/`** — LangChain `@tool` wrappers with session caching and asset tracking
- **`core/`** — Configuration, authentication, exceptions
- **`cli/`** — Typer CLI application

## Development

```bash
# Run tests
pytest

# Type checking
python -m pyright src/superset_ai/

# Linter
ruff check src/
```

## License

MIT
