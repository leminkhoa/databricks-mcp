"""
Databricks SQL MCP tools implementation.
"""

import json
from typing import Any, Dict, List

from mcp.types import TextContent
from src.api.sql import sql_warehouses, statement_execution
from .. import mcp_app

@mcp_app.tool(
    name="list_sql_warehouses",
    description="List all SQL warehouses in the workspace"
)
async def tool_list_sql_warehouses() -> List[TextContent]:
    """List all SQL warehouses in the workspace."""
    try:
        result = await sql_warehouses.list_sql_warehouses()
        return [{"text": json.dumps(result)}]
    except Exception as e:
        return [{"text": json.dumps({"error": str(e)})}]

@mcp_app.tool(
    name="create_sql_warehouse",
    description="""
    Create a new SQL warehouse.
    
    Required parameters:
    - name (str): Name of the warehouse
    - cluster_size (str): Size of the clusters (2X-Small to 4X-Large)
    
    Optional parameters:
    - min_num_clusters (int): Minimum number of clusters (default: 1)
    - max_num_clusters (int): Maximum number of clusters (default: 1)
    - auto_stop_mins (int): Minutes of inactivity after which warehouse will be stopped (default: 120)
    - enable_photon (bool): Enable Photon acceleration (default: True)
    - spot_instance_policy (str): Policy for spot instances (default: COST_OPTIMIZED)
    - warehouse_type (str): Type of warehouse (PRO or CLASSIC) (default: PRO)
    - channel (str): Release channel (CHANNEL_NAME_CURRENT or CHANNEL_NAME_PREVIEW)
    - tags (dict): Resource tags
    """
)
async def tool_create_sql_warehouse(params: Dict[str, Any]) -> List[TextContent]:
    """Create a new SQL warehouse."""
    try:
        # Validate required parameters
        required_params = ["name", "cluster_size"]
        missing_params = [p for p in required_params if p not in params]
        if missing_params:
            raise ValueError(f"Missing required parameters: {', '.join(missing_params)}")
        
        # Create the warehouse
        result = await sql_warehouses.create_sql_warehouse(**params)
        return [{"text": json.dumps(result)}]
    except Exception as e:
        return [{"text": json.dumps({"error": str(e)})}]

@mcp_app.tool(
    name="execute_sql_statement",
    description="""
    Execute a SQL statement using a SQL warehouse.
    
    Required parameters:
    - warehouse_id (str): ID of the SQL warehouse to use
    - statement (str): The SQL statement to execute
    
    Optional parameters:
    - catalog (str): The catalog to use
    - schema (str): The schema to use
    - disposition (str): How to return the result (INLINE or EXTERNAL_LINKS) (default: INLINE)
    - wait_timeout (int): Time in seconds to wait for completion
    """
)
async def tool_execute_sql_statement(params: Dict[str, Any]) -> List[TextContent]:
    """Execute a SQL statement using a SQL warehouse."""
    try:
        # Validate required parameters
        required_params = ["warehouse_id", "statement"]
        missing_params = [p for p in required_params if p not in params]
        if missing_params:
            raise ValueError(f"Missing required parameters: {', '.join(missing_params)}")
        
        # Execute the statement
        result = await statement_execution.execute_sql_statement(**params)
        return [{"text": json.dumps(result)}]
    except Exception as e:
        return [{"text": json.dumps({"error": str(e)})}] 