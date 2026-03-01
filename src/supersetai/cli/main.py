"""CLI interface for SupersetAI using Typer."""

import asyncio
import logging
import sys
from typing import Optional

import typer
from rich.console import Console
from rich.logging import RichHandler
from rich.markdown import Markdown
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table

from supersetai import __version__

app = typer.Typer(
    name="supersetai",
    help="Natural language interface for creating Apache Superset dashboards",
    add_completion=False,
)
console = Console()


def setup_logging(level: str = "INFO") -> None:
    """Configure logging with Rich handler."""
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )


@app.command()
def chat(
    url: Optional[str] = typer.Option(
        None,
        "--url",
        "-u",
        help="Superset base URL",
        envvar="SUPERSETAI_SUPERSET_BASE_URL",
    ),
    username: Optional[str] = typer.Option(
        None,
        "--username",
        help="Superset username",
        envvar="SUPERSETAI_SUPERSET_USERNAME",
    ),
    password: Optional[str] = typer.Option(
        None,
        "--password",
        help="Superset password",
        envvar="SUPERSETAI_SUPERSET_PASSWORD",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Enable verbose logging",
    ),
) -> None:
    """
    Start an interactive chat session with the Superset agent.
    
    The agent can help you create datasets, charts, and dashboards
    from natural language requests.
    """
    setup_logging("DEBUG" if verbose else "INFO")
    
    asyncio.run(_chat_loop(url, username, password))


async def _chat_loop(
    url: str | None,
    username: str | None,
    password: str | None,
) -> None:
    """Main chat loop."""
    from supersetai.agent.graph import SupersetAgent
    from supersetai.core.config import SupersetConfig
    
    # Build config with overrides
    config_kwargs = {}
    if url:
        config_kwargs["superset_base_url"] = url
    if username:
        config_kwargs["superset_username"] = username
    if password:
        from pydantic import SecretStr
        config_kwargs["superset_password"] = SecretStr(password)
    
    try:
        config = SupersetConfig(**config_kwargs)
    except Exception as e:
        console.print(f"[red]Configuration error:[/red] {e}")
        console.print("\nMake sure you have set the required environment variables:")
        console.print("  SUPERSETAI_SUPERSET_BASE_URL")
        console.print("  SUPERSETAI_SUPERSET_USERNAME")
        console.print("  SUPERSETAI_SUPERSET_PASSWORD")
        console.print("\nFor LLM authentication, either:")
        console.print("  - Run 'supersetai login' for GitHub Copilot")
        console.print("  - Set SUPERSETAI_OPENAI_API_KEY for OpenAI")
        raise typer.Exit(1)
    
    # Print welcome message
    console.print(Panel.fit(
        "[bold blue]SupersetAI[/bold blue] - Natural Language Dashboard Builder\n"
        f"Version {__version__}",
        border_style="blue",
    ))
    console.print(f"\nConnecting to [cyan]{config.superset_base_url}[/cyan]...")
    
    try:
        async with SupersetAgent(config=config) as agent:
            console.print("[green]Connected![/green]")
            
            # Show discovered databases
            if agent.session.superset_context.databases:
                db_names = [
                    d.get("database_name", "unknown")
                    for d in agent.session.superset_context.databases
                ]
                console.print(f"Available databases: [cyan]{', '.join(db_names)}[/cyan]")
            
            console.print("\nType your requests in natural language.")
            console.print("Commands: [dim]'help', 'status', 'exit'[/dim]\n")
            
            while True:
                try:
                    user_input = Prompt.ask("[bold green]You[/bold green]")
                except (KeyboardInterrupt, EOFError):
                    console.print("\n[yellow]Goodbye![/yellow]")
                    break
                
                user_input = user_input.strip()
                
                if not user_input:
                    continue
                
                # Handle special commands
                if user_input.lower() in ("exit", "quit", "q"):
                    console.print("[yellow]Goodbye![/yellow]")
                    break
                
                if user_input.lower() == "help":
                    _show_help()
                    continue
                
                if user_input.lower() == "status":
                    _show_status(agent)
                    continue
                
                # Process with agent
                console.print()
                with console.status("[bold blue]Thinking...", spinner="dots"):
                    try:
                        response = await agent.chat(user_input)
                    except Exception as e:
                        console.print(f"[red]Error:[/red] {e}")
                        continue
                
                # Display response
                console.print(Panel(
                    Markdown(response),
                    title="[bold blue]SupersetAI[/bold blue]",
                    border_style="blue",
                ))
                console.print()
                
    except Exception as e:
        console.print(f"[red]Failed to connect:[/red] {e}")
        raise typer.Exit(1)


def _show_help() -> None:
    """Display help information."""
    help_text = """
## Available Commands

- `help` - Show this help message
- `status` - Show session status and created assets
- `exit` / `quit` - Exit the chat

## Example Requests

**Discover data:**
- "What databases are available?"
- "Show me the tables in the analytics database"
- "What columns does the orders table have?"

**Create charts:**
- "Create a bar chart showing sales by region"
- "Show me a line chart of daily active users over time"
- "Make a pie chart of revenue breakdown by product category"
- "Create a table showing the top 10 customers by order count"

**Create dashboards:**
- "Create a dashboard called 'Sales Overview' with the charts I just made"
- "Add a new chart to the current dashboard showing monthly trends"

## Tips

- Be specific about column names and metrics
- The agent will ask clarifying questions if needed
- Charts are automatically added to the active dashboard context
"""
    console.print(Panel(Markdown(help_text), title="Help", border_style="green"))


def _show_status(agent: "SupersetAgent") -> None:
    """Display session status."""
    from supersetai.agent.graph import SupersetAgent
    
    summary = agent.get_session_summary()
    
    # Session info
    console.print(Panel(
        f"Session ID: [cyan]{summary['session_id'][:8]}...[/cyan]\n"
        f"Started: [cyan]{summary['started_at']}[/cyan]\n"
        f"Messages: [cyan]{summary['messages_count']}[/cyan]",
        title="Session Info",
        border_style="blue",
    ))
    
    # Active dashboard
    if summary["active_dashboard"]:
        console.print(Panel(
            f"ID: [cyan]{summary['active_dashboard']['id']}[/cyan]\n"
            f"Title: [cyan]{summary['active_dashboard']['title']}[/cyan]",
            title="Active Dashboard",
            border_style="green",
        ))
    
    # Created assets
    if summary["created_assets"]:
        table = Table(title="Created Assets")
        table.add_column("Type", style="cyan")
        table.add_column("ID", style="magenta")
        table.add_column("Name", style="green")
        
        for asset in summary["created_assets"]:
            table.add_row(asset["type"], str(asset["id"]), asset["name"])
        
        console.print(table)
    
    # Databases
    if summary["databases"]:
        db_table = Table(title="Available Databases")
        db_table.add_column("ID", style="cyan")
        db_table.add_column("Name", style="green")
        db_table.add_column("Backend", style="magenta")
        
        for db in summary["databases"]:
            db_table.add_row(
                str(db.get("id", "")),
                db.get("database_name", ""),
                db.get("backend", ""),
            )
        
        console.print(db_table)


@app.command()
def version() -> None:
    """Show version information."""
    console.print(f"SupersetAI version {__version__}")


@app.command()
def login() -> None:
    """
    Authenticate with GitHub Copilot.
    
    Opens a browser for OAuth device flow authentication.
    Token is cached for future use.
    """
    from supersetai.core.copilot_auth import authenticate_copilot, CopilotAuthError
    
    try:
        token = authenticate_copilot(
            open_browser=True,
            print_fn=lambda msg: console.print(msg),
        )
        console.print(f"[green]Successfully authenticated![/green]")
        console.print(f"Token expires: {token.expires_at.strftime('%Y-%m-%d %H:%M:%S')}")
    except CopilotAuthError as e:
        console.print(f"[red]Authentication failed:[/red] {e}")
        raise typer.Exit(1)
    except KeyboardInterrupt:
        console.print("\n[yellow]Authentication cancelled.[/yellow]")
        raise typer.Exit(1)


@app.command()
def logout() -> None:
    """
    Clear cached GitHub Copilot token.
    """
    from supersetai.core.copilot_auth import clear_cached_token, TOKEN_CACHE_PATH
    
    if TOKEN_CACHE_PATH.exists():
        clear_cached_token()
        console.print("[green]Logged out successfully.[/green]")
    else:
        console.print("[yellow]No cached token found.[/yellow]")


@app.command()
def test_connection(
    url: Optional[str] = typer.Option(
        None,
        "--url",
        "-u",
        help="Superset base URL",
        envvar="SUPERSETAI_SUPERSET_BASE_URL",
    ),
    username: Optional[str] = typer.Option(
        None,
        "--username",
        help="Superset username",
        envvar="SUPERSETAI_SUPERSET_USERNAME",
    ),
    password: Optional[str] = typer.Option(
        None,
        "--password",
        help="Superset password",
        envvar="SUPERSETAI_SUPERSET_PASSWORD",
    ),
) -> None:
    """Test connection to Superset API."""
    setup_logging("INFO")
    asyncio.run(_test_connection(url, username, password))


async def _test_connection(
    url: str | None,
    username: str | None,
    password: str | None,
) -> None:
    """Test Superset connection."""
    from supersetai.api.client import SupersetClient
    from supersetai.core.config import SupersetConfig
    
    config_kwargs = {}
    if url:
        config_kwargs["superset_base_url"] = url
    if username:
        config_kwargs["superset_username"] = username
    if password:
        from pydantic import SecretStr
        config_kwargs["superset_password"] = SecretStr(password)
    
    try:
        config = SupersetConfig(**config_kwargs)
    except Exception as e:
        console.print(f"[red]Configuration error:[/red] {e}")
        raise typer.Exit(1)
    
    console.print(f"Testing connection to [cyan]{config.superset_base_url}[/cyan]...")
    
    try:
        async with SupersetClient(config) as client:
            # Test authentication
            console.print("[green]Authentication successful![/green]")
            
            # List databases
            from supersetai.api.databases import DatabaseService
            db_service = DatabaseService(client)
            databases = await db_service.list_databases()
            
            console.print(f"\nFound [cyan]{len(databases)}[/cyan] database(s):")
            for db in databases:
                console.print(f"  - {db.database_name} ({db.backend})")
            
            console.print("\n[green]All tests passed![/green]")
            
    except Exception as e:
        console.print(f"[red]Connection failed:[/red] {e}")
        raise typer.Exit(1)


@app.command()
def list_databases(
    url: Optional[str] = typer.Option(
        None,
        "--url",
        "-u",
        help="Superset base URL",
        envvar="SUPERSETAI_SUPERSET_BASE_URL",
    ),
) -> None:
    """List available databases in Superset."""
    setup_logging("WARNING")
    asyncio.run(_list_databases(url))


async def _list_databases(url: str | None) -> None:
    """List databases."""
    from supersetai.api.client import SupersetClient
    from supersetai.api.databases import DatabaseService
    from supersetai.core.config import SupersetConfig
    
    config_kwargs = {}
    if url:
        config_kwargs["superset_base_url"] = url
    
    try:
        config = SupersetConfig(**config_kwargs)
    except Exception as e:
        console.print(f"[red]Configuration error:[/red] {e}")
        raise typer.Exit(1)
    
    try:
        async with SupersetClient(config) as client:
            db_service = DatabaseService(client)
            databases = await db_service.list_databases()
            
            table = Table(title="Superset Databases")
            table.add_column("ID", style="cyan")
            table.add_column("Name", style="green")
            table.add_column("Backend", style="magenta")
            
            for db in databases:
                table.add_row(
                    str(db.id),
                    db.database_name,
                    db.backend or "unknown",
                )
            
            console.print(table)
            
    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)


@app.command(name="mcp")
def mcp_server(
    transport: str = typer.Option(
        "stdio",
        "--transport",
        "-t",
        help="MCP transport: 'stdio' or 'http'",
    ),
    port: int = typer.Option(
        8000,
        "--port",
        "-p",
        help="Port for HTTP transport (ignored for stdio)",
    ),
) -> None:
    """
    Start the MCP (Model Context Protocol) server.

    Exposes Superset tools via MCP so any compatible client
    (Claude Desktop, Cursor, VS Code, etc.) can use them.

    Examples:
        supersetai mcp                    # stdio (default)
        supersetai mcp -t http -p 8000    # HTTP on port 8000
    """
    from supersetai.mcp.server import mcp as mcp_app

    if transport == "http":
        console.print(
            f"Starting SupersetAI MCP server on [cyan]http://0.0.0.0:{port}/mcp[/cyan]"
        )
        mcp_app.run(transport="http", port=port)
    else:
        # stdio — no console output (stdout is the MCP channel)
        mcp_app.run(transport="stdio")


if __name__ == "__main__":
    app()
