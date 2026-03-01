"""MCP (Model Context Protocol) server for SupersetAI.

Exposes Superset operations as MCP tools that can be consumed by
any MCP-compatible client (Claude Desktop, Cursor, VS Code, etc.).

Usage:
    # Start MCP server via CLI
    superset-ai mcp

    # Or run directly
    fastmcp run superset_ai.mcp.server:mcp --transport stdio
    fastmcp run superset_ai.mcp.server:mcp --transport http --port 8000
"""

from superset_ai.mcp.server import mcp

__all__ = ["mcp"]
