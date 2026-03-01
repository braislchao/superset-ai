"""Agent state definitions for LangGraph."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Annotated, Any, Literal, TypedDict

from langgraph.graph.message import add_messages
from pydantic import BaseModel, Field


class AssetReference(BaseModel):
    """Reference to a created Superset asset."""

    type: Literal["database", "dataset", "chart", "dashboard"]
    id: int
    name: str
    created_at: datetime = Field(default_factory=datetime.now)


class SupersetContext(BaseModel):
    """
    Cached context about Superset environment.
    
    Reduces redundant API calls during agent execution.
    """

    databases: list[dict[str, Any]] = Field(default_factory=list)
    discovered_tables: dict[int, list[str]] = Field(default_factory=dict)
    discovered_columns: dict[int, list[str]] = Field(default_factory=dict)
    
    def get_database_by_name(self, name: str) -> dict[str, Any] | None:
        """Find database by name."""
        for db in self.databases:
            if db.get("database_name", "").lower() == name.lower():
                return db
        return None
    
    def get_database_by_id(self, db_id: int) -> dict[str, Any] | None:
        """Find database by ID."""
        for db in self.databases:
            if db.get("id") == db_id:
                return db
        return None


class SessionState(BaseModel):
    """
    Persistent state within a CLI session.
    
    Maintains conversation history and created assets.
    """

    session_id: str
    started_at: datetime = Field(default_factory=datetime.now)
    
    # Conversation history
    messages: list[dict[str, Any]] = Field(default_factory=list)
    
    # Superset context (cached)
    superset_context: SupersetContext = Field(default_factory=SupersetContext)
    
    # Created assets
    created_assets: list[AssetReference] = Field(default_factory=list)
    
    # Currently active dashboard context
    active_dashboard_id: int | None = None
    active_dashboard_title: str | None = None
    
    def add_asset(
        self,
        asset_type: Literal["database", "dataset", "chart", "dashboard"],
        asset_id: int,
        name: str,
    ) -> None:
        """Record a created asset."""
        self.created_assets.append(
            AssetReference(type=asset_type, id=asset_id, name=name)
        )
    
    def get_recent_charts(self, limit: int = 10) -> list[AssetReference]:
        """Get recently created charts."""
        charts = [a for a in self.created_assets if a.type == "chart"]
        return sorted(charts, key=lambda x: x.created_at, reverse=True)[:limit]


class AgentState(TypedDict, total=False):
    """
    LangGraph agent state.
    
    This TypedDict defines the state that flows through the agent graph.
    """

    # Core conversation - using Annotated with add_messages to properly accumulate
    messages: Annotated[list[Any], add_messages]
    
    # User request context
    user_request: str
    
    # Superset context
    superset_context: SupersetContext
    
    # Session state
    session: SessionState
    
    # Current action results
    last_action_result: dict[str, Any] | None
    
    # Error tracking
    errors: list[dict[str, Any]]
    
    # Final response
    response: str | None


@dataclass
class ToolContext:
    """
    Context passed to agent tools.
    
    Provides access to Superset services without global state.
    """

    from superset_ai.api.client import SupersetClient
    from superset_ai.api.charts import ChartService
    from superset_ai.api.dashboards import DashboardService
    from superset_ai.api.databases import DatabaseService
    from superset_ai.api.datasets import DatasetService

    client: "SupersetClient"
    session: SessionState
    
    _datasets: "DatasetService | None" = field(default=None, repr=False)
    _charts: "ChartService | None" = field(default=None, repr=False)
    _dashboards: "DashboardService | None" = field(default=None, repr=False)
    _databases: "DatabaseService | None" = field(default=None, repr=False)
    
    @property
    def datasets(self) -> "DatasetService":
        if self._datasets is None:
            from superset_ai.api.datasets import DatasetService
            self._datasets = DatasetService(self.client)
        return self._datasets
    
    @property
    def charts(self) -> "ChartService":
        if self._charts is None:
            from superset_ai.api.charts import ChartService
            self._charts = ChartService(self.client)
        return self._charts
    
    @property
    def dashboards(self) -> "DashboardService":
        if self._dashboards is None:
            from superset_ai.api.dashboards import DashboardService
            self._dashboards = DashboardService(self.client)
        return self._dashboards
    
    @property
    def databases(self) -> "DatabaseService":
        if self._databases is None:
            from superset_ai.api.databases import DatabaseService
            self._databases = DatabaseService(self.client)
        return self._databases
