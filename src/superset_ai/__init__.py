"""SupersetAI - Natural language interface for Apache Superset dashboards.

Primary interfaces:

    - ``superset_ai.api`` — typed async REST API client for Superset
    - ``superset_ai.mcp`` — MCP server exposing Superset tools
    - ``superset_ai.agent`` — LangGraph ReAct agent (requires ``pip install superset-ai[agent]``)
    - ``superset_ai.cli`` — CLI (``superset-ai chat``, ``superset-ai mcp``, etc.)
"""

__version__ = "0.1.0"
