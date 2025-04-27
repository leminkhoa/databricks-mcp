"""
Databricks MCP tools implementation for command execution operations.
"""

import json
from typing import Any, Dict, List

from mcp.types import TextContent
from src.api.compute import command_execution
from .. import mcp_app

@mcp_app.tool(
    name="execute_command",
    description="""
    Execute a command on a running Databricks cluster.
    
    This API only supports (classic) all-purpose clusters. Serverless compute is not supported.
    
    Required parameters:
    - cluster_id (str): ID of the cluster to run the command on
    - language (str): Language of the command (must be one of: `python`, `scala`, `sql`)
    - command (str): The command to execute
    
    Optional parameters:
    - context_id (str): Context ID for maintaining session state
    """
)
async def tool_execute_command(params: Dict[str, Any]) -> List[TextContent]:
    """Execute a command on a running Databricks cluster."""
    try:
        # Validate required parameters
        required_params = ["cluster_id", "language", "command"]
        missing_params = [p for p in required_params if p not in params]
        if missing_params:
            raise ValueError(f"Missing required parameters: {', '.join(missing_params)}")
        
        # Extract parameters
        cluster_id = params["cluster_id"]
        language = params["language"]
        command = params["command"]
        context_id = params.get("context_id")
        
        # Execute the command
        result = await command_execution.execute_command(
            cluster_id=cluster_id,
            language=language,
            command=command,
            context_id=context_id
        )
        
        # Format the results nicely
        formatted_result = {
            "command_results": result.get("results", {}),
            "command_id": result.get("id"),
            "status": result.get("status"),
            "context_id": result.get("contextId", context_id)
        }
        
        return [{"text": json.dumps(formatted_result, indent=2)}]
    except Exception as e:
        return [{"text": json.dumps({"error": str(e)})}]

@mcp_app.tool(
    name="create_context",
    description="""
    Create an execution context on a running Databricks cluster.
    
    This API only supports (classic) all-purpose clusters. Serverless compute is not supported.
    
    Required parameters:
    - cluster_id (str): ID of the cluster to create the context on
    - language (str): Language for the execution context (must be one of: `python`, `scala`, `sql`)
    """
)
async def tool_create_context(params: Dict[str, Any]) -> List[TextContent]:
    """Create an execution context on a running Databricks cluster."""
    try:
        # Validate required parameters
        required_params = ["cluster_id", "language"]
        missing_params = [p for p in required_params if p not in params]
        if missing_params:
            raise ValueError(f"Missing required parameters: {', '.join(missing_params)}")
        
        # Extract parameters
        cluster_id = params["cluster_id"]
        language = params["language"]
        
        # Create the context
        result = await command_execution.create_context(
            cluster_id=cluster_id,
            language=language
        )
        
        # Format the results nicely
        formatted_result = {
            "context_id": result.get("id"),
            "status": result.get("status"),
            "language": language,
            "cluster_id": cluster_id
        }
        
        return [{"text": json.dumps(formatted_result, indent=2)}]
    except Exception as e:
        return [{"text": json.dumps({"error": str(e)})}]
