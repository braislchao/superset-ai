"""MCP (Model Context Protocol) server for SupersetAI.

Exposes Superset operations as MCP tools that can be consumed by
any MCP-compatible client (Claude Desktop, Cursor, VS Code, etc.).

Usage:
    # Start MCP server via CLI
    supersetai mcp

    # Or run directly
    fastmcp run supersetai.mcp.server:mcp --transport stdio
    fastmcp run supersetai.mcp.server:mcp --transport http --port 8000
"""

from supersetai.mcp.server import mcp

__all__ = ["mcp"]
