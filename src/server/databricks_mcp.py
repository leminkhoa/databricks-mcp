"""
Databricks MCP server implementation.
"""

import sys
import traceback
from typing import Any, Dict, Optional

from ..core.config import settings
from ..core.logging import create_logger
from ..mcp_components import mcp_app

# Import tool modules to register tools
from ..mcp_components.tools import tools_cluster, tools_sql, tools_workspace

# Constants
API_VERSION = "2.1"
USER_AGENT = "databricks-mcp/1.0"

class DatabricksMCPServer:
    """Server for Databricks with MCP integration."""
    
    def __init__(self, 
                 server_name: str = "databricks", 
                 api_version: str = API_VERSION, 
                 user_agent: str = USER_AGENT
        ):
        """
        Initialize the Databricks MCP server.
        
        Args:
            server_name: Name of the MCP server
        """
        # Configure logging
        self.logger = create_logger(__name__)
        self.logger.info(f"Initializing Databricks MCP server: {server_name}")
        self.logger.info(f"Databricks host: {settings.databricks_host}")
        
        # Store reference to the MCP app
        self.mcp = mcp_app
        self.api_version = api_version
        self.user_agent = user_agent
        self.is_running = False
  
    async def run(self) -> None:
        """Run the MCP server."""
        try:
            self.logger.info("Starting Databricks MCP server")
            self.is_running = True
            await self.mcp.run_stdio_async()
        except Exception as e:
            self.logger.error(f"Error running MCP server: {e}")
            traceback.print_exc(file=sys.stderr)
            sys.exit(1)
        finally:
            self.is_running = False
