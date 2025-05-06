"""
Main entry point for the Databricks MCP server.
"""

import asyncio
import sys

from src.core.logging import create_logger
from src.server.databricks_mcp import DatabricksMCPServer

logger = create_logger(__name__)

async def main():
    """Run the Databricks MCP server."""
    try:
        server = DatabricksMCPServer(server_name="databricks-mcp")
        await server.run()
    except Exception as e:
        logger.error(f"Error running MCP server: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
