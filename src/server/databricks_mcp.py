"""
Databricks MCP server implementation.
"""


from ..core.config import settings, Settings
from ..core.logging import create_logger
from mcp.server.fastmcp import FastMCP
from src.mcp_components import mcp_app

# Constants
USER_AGENT = "databricks-mcp/1.0"

class DatabricksMCPServer:
    """Server for Databricks with MCP integration."""
    
    def __init__(self, 
                 mcp_app: FastMCP = mcp_app,
                 config: Settings = settings,
                 server_name: str = "databricks", 
                 user_agent: str = USER_AGENT,
        ):
        """
        Initialize the Databricks MCP server.
        
        Args:
            mcp_app: FastMCP instance
            config: Settings instance
            server_name: Name of the MCP server
            user_agent: User agent for the MCP server
        """
        # Configure logging
        self.logger = create_logger(__name__)
        self.logger.info(f"Initializing Databricks MCP server: {server_name}")
        self.logger.info(f"Databricks host: {config.databricks_host}")
        self.logger.info(f"Transport: {config.transport}")
        if config.transport == "sse":
            self.logger.info(f"Host: {config.server_host}")
            self.logger.info(f"Port: {config.server_port}")
        
        # Store reference to the MCP app
        self.mcp_app = mcp_app
        self.user_agent = user_agent
        self.is_running = False
        self.config = config
  
    async def run(self) -> None:
        """Run the MCP server."""
        try:
            self.logger.info("Starting Databricks MCP server")
            self.is_running = True
            if self.config.transport == "stdio":
                await self.mcp_app.run_stdio_async()
            elif self.config.transport == "sse":
                await self.mcp_app.run_sse_async()
            else:
                raise ValueError(f"Invalid transport: {self.config.transport}")
        except Exception as e:
            self.logger.error(f"Error running MCP server: {e}")
        finally:
            self.is_running = False
