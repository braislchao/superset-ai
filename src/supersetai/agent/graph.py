"""LangGraph agent definition for Superset dashboard builder."""

import logging
import uuid
from typing import Any

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from langgraph.graph import END, StateGraph
from langgraph.prebuilt import ToolNode

from supersetai.agent.prompts import SYSTEM_PROMPT, build_session_context
from supersetai.agent.state import AgentState, SessionState, SupersetContext, ToolContext
from supersetai.agent.tools import ALL_TOOLS, set_tool_context
from supersetai.api.client import SupersetClient
from supersetai.core.config import SupersetConfig

logger = logging.getLogger(__name__)


class SupersetAgent:
    """
    LangGraph-based agent for building Superset dashboards from natural language.
    
    Uses a ReAct-style agent with tools for:
    - Discovering databases and tables
    - Creating/finding datasets
    - Building charts
    - Assembling dashboards
    """

    def __init__(
        self,
        config: SupersetConfig | None = None,
        session: SessionState | None = None,
    ) -> None:
        from supersetai.core.config import get_config

        self.config = config or get_config()
        self.session = session or SessionState(session_id=str(uuid.uuid4()))
        self.client: SupersetClient | None = None
        self.graph = self._build_graph()

    def _build_graph(self) -> StateGraph:
        """Build the LangGraph agent graph."""
        # Initialize LLM based on provider
        llm_kwargs = {
            "model": self.config.get_llm_model(),
            "api_key": self.config.get_llm_api_key(),
            "temperature": 0,
        }
        
        # Add base_url for Copilot
        base_url = self.config.get_llm_base_url()
        if base_url:
            llm_kwargs["base_url"] = base_url
            # GitHub Copilot requires editor headers
            llm_kwargs["default_headers"] = {
                "Editor-Version": "vscode/1.95.0",
                "Editor-Plugin-Version": "copilot-chat/0.22.0",
            }
        
        llm = ChatOpenAI(**llm_kwargs)
        
        # Bind tools to LLM
        llm_with_tools = llm.bind_tools(ALL_TOOLS)

        # Define nodes
        def agent_node(state: AgentState) -> dict[str, Any]:
            """Main agent reasoning node."""
            messages = list(state.get("messages", []))
            
            # Debug: log message structure
            logger.debug(f"agent_node called with {len(messages)} messages")
            for i, m in enumerate(messages):
                msg_type = type(m).__name__
                has_tools = hasattr(m, "tool_calls") and m.tool_calls
                logger.debug(f"  [{i}] {msg_type}, has_tool_calls={has_tools}")
            
            # Check if system message already exists
            has_system = messages and isinstance(messages[0], SystemMessage)
            
            if not has_system:
                # Build and prepend system message
                session_context = build_session_context(
                    databases=state.get("superset_context", SupersetContext()).databases,
                    active_dashboard={
                        "id": self.session.active_dashboard_id,
                        "title": self.session.active_dashboard_title,
                    } if self.session.active_dashboard_id else None,
                    recent_assets=[
                        {"type": a.type, "name": a.name, "id": a.id}
                        for a in self.session.created_assets[-5:]
                    ],
                )
                system_msg = SystemMessage(
                    content=SYSTEM_PROMPT.format(session_context=session_context)
                )
                # Prepend to messages for this invocation
                messages = [system_msg] + messages
                # Also update state with the system message
                result_messages = [system_msg]
            else:
                result_messages = []
            
            # Debug: log final messages being sent to LLM
            logger.debug(f"Sending {len(messages)} messages to LLM")
            for i, m in enumerate(messages):
                msg_type = type(m).__name__
                has_tools = hasattr(m, "tool_calls") and m.tool_calls
                logger.debug(f"  [{i}] {msg_type}, has_tool_calls={has_tools}")
            
            # Invoke LLM
            response = llm_with_tools.invoke(messages)
            
            # Return the response (and system message if we added it)
            result_messages.append(response)
            return {"messages": result_messages}

        def should_continue(state: AgentState) -> str:
            """Determine if agent should continue or end."""
            messages = state.get("messages", [])
            if not messages:
                return "end"
            
            last_message = messages[-1]
            
            # If LLM made tool calls, continue to tools
            if hasattr(last_message, "tool_calls") and last_message.tool_calls:
                return "tools"
            
            # Otherwise, we're done
            return "end"

        # Build graph
        graph = StateGraph(AgentState)
        
        # Add nodes
        graph.add_node("agent", agent_node)
        graph.add_node("tools", ToolNode(ALL_TOOLS))
        
        # Set entry point
        graph.set_entry_point("agent")
        
        # Add edges
        graph.add_conditional_edges(
            "agent",
            should_continue,
            {
                "tools": "tools",
                "end": END,
            },
        )
        graph.add_edge("tools", "agent")
        
        return graph.compile()

    async def initialize(self) -> None:
        """Initialize the agent and connect to Superset."""
        self.client = SupersetClient(self.config)
        
        # Set up tool context
        tool_ctx = ToolContext(
            client=self.client,
            session=self.session,
        )
        set_tool_context(tool_ctx)
        
        # Pre-discover databases
        try:
            from supersetai.api.databases import DatabaseService
            db_service = DatabaseService(self.client)
            databases = await db_service.list_databases()
            self.session.superset_context.databases = [
                {"id": db.id, "database_name": db.database_name, "backend": db.backend}
                for db in databases
            ]
            logger.info(f"Discovered {len(databases)} database(s)")
        except Exception as e:
            logger.warning(f"Failed to pre-discover databases: {e}")

    async def close(self) -> None:
        """Close connections."""
        if self.client:
            await self.client.close()

    async def __aenter__(self) -> "SupersetAgent":
        """Async context manager entry."""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()

    async def chat(self, user_message: str) -> str:
        """
        Process a user message and return the agent's response.
        
        Args:
            user_message: Natural language request from the user
        
        Returns:
            The agent's text response
        """
        # Add user message to session history
        self.session.messages.append({"role": "user", "content": user_message})
        
        # Convert full session history to LangChain message objects
        history_messages = []
        for msg in self.session.messages:
            if msg["role"] == "user":
                history_messages.append(HumanMessage(content=msg["content"]))
            elif msg["role"] == "assistant":
                history_messages.append(AIMessage(content=msg["content"]))
        
        # Build initial state with full conversation history
        initial_state: AgentState = {
            "messages": history_messages,
            "user_request": user_message,
            "superset_context": self.session.superset_context,
            "session": self.session,
            "errors": [],
        }
        
        # Run the graph
        try:
            result = await self.graph.ainvoke(initial_state)
            
            # Extract response
            messages = result.get("messages", [])
            if messages:
                last_message = messages[-1]
                if isinstance(last_message, AIMessage):
                    response = last_message.content
                    
                    # Add to session history
                    self.session.messages.append({
                        "role": "assistant",
                        "content": response,
                    })
                    
                    return response
            
            return "I was unable to process your request. Please try again."
            
        except Exception as e:
            import traceback
            logger.error(f"Agent error: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            error_msg = f"An error occurred: {str(e)}"
            self.session.messages.append({
                "role": "assistant",
                "content": error_msg,
            })
            return error_msg

    def get_session_summary(self) -> dict[str, Any]:
        """Get a summary of the current session."""
        return {
            "session_id": self.session.session_id,
            "started_at": self.session.started_at.isoformat(),
            "messages_count": len(self.session.messages),
            "created_assets": [
                {"type": a.type, "id": a.id, "name": a.name}
                for a in self.session.created_assets
            ],
            "active_dashboard": {
                "id": self.session.active_dashboard_id,
                "title": self.session.active_dashboard_title,
            } if self.session.active_dashboard_id else None,
            "databases": self.session.superset_context.databases,
        }


async def create_agent(config: SupersetConfig | None = None) -> SupersetAgent:
    """
    Create and initialize a Superset agent.
    
    Convenience function for one-off usage.
    """
    agent = SupersetAgent(config=config)
    await agent.initialize()
    return agent
