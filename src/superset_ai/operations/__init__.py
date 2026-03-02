"""Shared business logic for Superset operations.

This module contains the core logic that the agent tools delegate to. Each
function accepts service instances as arguments and returns plain dicts — no
framework-specific decorators, no global state, no side-effects (caching,
asset tracking, etc.).

The agent wrapper (agent/tools.py) handles:
- Framework integration (LangChain @tool)
- Service resolution (ContextVar)
- Side-effects (session caching, asset tracking)
- Error handling (@_handle_errors decorator)
"""
