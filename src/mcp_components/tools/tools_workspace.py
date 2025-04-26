"""
Databricks workspace MCP tools implementation.
"""

import json
from typing import Any, Dict, List

from mcp.types import TextContent
from src.api.workspace import objects
from .. import mcp_app

@mcp_app.tool(
    name="delete_workspace_object",
    description="""
    Delete an object from the Databricks workspace.
    
    Required parameters:
    - path (str): The absolute path of the object to delete
    
    Optional parameters:
    - recursive (bool): Whether to recursively delete the object and its contents
    """
)
async def tool_delete_workspace_object(params: Dict[str, Any]) -> List[TextContent]:
    """Delete an object from the Databricks workspace."""
    try:
        # Validate required parameters
        if "path" not in params:
            raise ValueError("Missing required parameter: path")
        
        # Get optional parameters
        recursive = params.get("recursive", False)
        
        # Delete the workspace object
        result = await objects.delete_workspace_object(
            path=params["path"],
            recursive=recursive
        )
        return [{"text": json.dumps(result)}]
    except Exception as e:
        return [{"text": json.dumps({"error": str(e)})}]

@mcp_app.tool(
    name="get_workspace_object_status",
    description="""
    Get the status of an object in the Databricks workspace.
    
    Required parameters:
    - path (str): The absolute path of the object
    """
)
async def tool_get_workspace_object_status(params: Dict[str, Any]) -> List[TextContent]:
    """Get the status of an object in the Databricks workspace."""
    try:
        # Validate required parameters
        if "path" not in params:
            raise ValueError("Missing required parameter: path")
        
        # Get the workspace object status
        result = await objects.get_workspace_object_status(params["path"])
        return [{"text": json.dumps(result)}]
    except Exception as e:
        return [{"text": json.dumps({"error": str(e)})}]

@mcp_app.tool(
    name="import_workspace_object",
    description="""
    Import an object into the Databricks workspace.
    
    Required parameters:
    - path (str): The absolute path where the object will be imported
    - content (str): The content to import (will be base64 encoded if not already)
    - format (str): The format of the object (SOURCE, HTML, JUPYTER, DBC)
    
    Optional parameters:
    - language (str): The language (PYTHON, R, SQL)
    - overwrite (bool): Whether to overwrite an existing object
    """
)
async def tool_import_workspace_object(params: Dict[str, Any]) -> List[TextContent]:
    """Import an object into the Databricks workspace."""
    try:
        # Validate required parameters
        required_params = ["path", "content", "format"]
        missing_params = [p for p in required_params if p not in params]
        if missing_params:
            raise ValueError(f"Missing required parameters: {', '.join(missing_params)}")
        
        # Get optional parameters
        language = params.get("language")
        overwrite = params.get("overwrite", False)
        
        # Import the workspace object
        result = await objects.import_workspace_object(
            path=params["path"],
            content=params["content"],
            format=params["format"],
            language=language,
            overwrite=overwrite
        )
        return [{"text": json.dumps(result)}]
    except Exception as e:
        return [{"text": json.dumps({"error": str(e)})}]

@mcp_app.tool(
    name="create_workspace_directory",
    description="""
    Create a directory in the Databricks workspace.
    
    Required parameters:
    - path (str): The absolute path of the directory to create
    """
)
async def tool_create_workspace_directory(params: Dict[str, Any]) -> List[TextContent]:
    """Create a directory in the Databricks workspace."""
    try:
        # Validate required parameters
        if "path" not in params:
            raise ValueError("Missing required parameter: path")
        
        # Create the directory
        result = await objects.create_workspace_directory(params["path"])
        return [{"text": json.dumps(result)}]
    except Exception as e:
        return [{"text": json.dumps({"error": str(e)})}] 