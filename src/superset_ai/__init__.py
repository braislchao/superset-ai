"""SupersetAI - Natural language interface for Apache Superset dashboards.

Primary interfaces:

    - ``superset_ai.api`` — typed async REST API client for Superset
    - ``superset_ai.agent`` — LangGraph ReAct agent (requires ``pip install superset-ai[agent]``)
    - ``superset_ai.cli`` — CLI (``superset-ai chat``, etc.)
"""

import logging

__version__ = "0.2.1"

# Suppress noisy httpx INFO logs (e.g. expected 401s during auth fallback)
logging.getLogger("httpx").setLevel(logging.WARNING)
