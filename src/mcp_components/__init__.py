"""
MCP Components package initialization.
Provides the central FastMCP instance for the application.
"""

from mcp.server.fastmcp import FastMCP

# Create a single FastMCP instance to be used across the application
mcp_app = FastMCP("databricks")

# Import tools and prompts to register them with mcp_app
import src.mcp_components.tools
import src.mcp_components.prompts
