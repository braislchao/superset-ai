"""Shared business logic for Superset operations.

This module contains the core logic that both the MCP server and the
LangChain agent tools delegate to. Each function accepts service instances
as arguments and returns plain dicts — no framework-specific decorators,
no global state, no side-effects (caching, asset tracking, etc.).

The wrappers (mcp/server.py and agent/tools.py) handle:
- Framework integration (FastMCP @mcp.tool, LangChain @tool)
- Service resolution (lazy singletons vs ContextVar)
- Side-effects (session caching, asset tracking)
- Error handling (@_handle_errors decorator)
"""
