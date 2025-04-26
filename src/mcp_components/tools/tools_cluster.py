"""
Databricks MCP tools implementation for cluster operations.
"""

import json
from typing import Any, Dict, List

from mcp.types import TextContent
from src.api.compute import clusters
from .. import mcp_app

@mcp_app.tool(
    name="list_clusters",
    description="List all Databricks clusters in the workspace"
)
async def tool_list_clusters() -> List[TextContent]:
    """List all clusters in the Databricks workspace."""
    try:
        response = await clusters.list_clusters()
        
        # Format the response
        cluster_list = response.get("clusters", [])
        if not cluster_list:
            return [{"text": "No clusters found in the workspace"}]
            
        formatted_response = {
            "clusters": cluster_list,
            "total_clusters": len(cluster_list),
            "has_more": bool(response.get("next_page_token"))
        }
        return [{"text": json.dumps(formatted_response, separators=(',', ':'))}]
    except Exception as e:
        return [{"text": f"Error listing clusters: {str(e)}"}]

@mcp_app.tool(
    name="create_cluster",
    description="""
    Create a new Databricks cluster.
    
    Required parameters:
    - cluster_name (str): Name of the cluster
    - node_type_id (str): Type of nodes (e.g., 'Standard_DS3_v2' for Azure, 'i3.xlarge' for AWS)
    - spark_version (str): Spark version to use (e.g., '7.3.x-scala2.12')
    
    Optional parameters:
    - autoscale (dict): Dict with min_workers (int) and max_workers (int)
    - num_workers (int): Fixed number of workers (don't use with autoscale)
    - spark_conf (dict): Dict of Spark configuration properties
    - custom_tags (dict): Dict of custom tags
    - init_scripts (list): List of initialization script configurations
    """
)
async def tool_create_cluster(params: Dict[str, Any]) -> List[TextContent]:
    """Create a new Databricks cluster with the specified configuration."""
    try:
        # Validate required parameters
        required_params = ["cluster_name", "node_type_id", "spark_version"]
        missing_params = [p for p in required_params if p not in params]
        if missing_params:
            raise ValueError(f"Missing required parameters: {', '.join(missing_params)}")
        
        # Create the cluster
        result = await clusters.create_cluster(**params)
        return [{"text": json.dumps(result)}]
    except Exception as e:
        return [{"text": json.dumps({"error": str(e)})}]

@mcp_app.tool(
    name="delete_cluster",
    description="""
    Delete a Databricks cluster.
    
    Required parameters:
    - cluster_id (str): ID of the cluster to delete
    """
)
async def tool_delete_cluster(params: Dict[str, Any]) -> List[TextContent]:
    """Delete a Databricks cluster by its ID."""
    try:
        # Validate required parameters
        if "cluster_id" not in params:
            raise ValueError("Missing required parameter: cluster_id")
        
        # Delete the cluster
        result = await clusters.delete_cluster(params["cluster_id"])
        return [{"text": json.dumps(result)}]
    except Exception as e:
        return [{"text": json.dumps({"error": str(e)})}]

@mcp_app.tool(
    name="start_cluster",
    description="""
    Start a terminated Databricks cluster.
    
    Required parameters:
    - cluster_id (str): ID of the cluster to start
    """
)
async def tool_start_cluster(params: Dict[str, Any]) -> List[TextContent]:
    """Start a terminated Databricks cluster."""
    try:
        # Validate required parameters
        if "cluster_id" not in params:
            raise ValueError("Missing required parameter: cluster_id")
        
        # Start the cluster
        result = await clusters.start_cluster(params["cluster_id"])
        return [{"text": json.dumps(result)}]
    except Exception as e:
        return [{"text": json.dumps({"error": str(e)})}]

@mcp_app.tool(
    name="list_node_types",
    description="List all available node types for Databricks clusters"
)
async def tool_list_node_types() -> List[TextContent]:
    """List all available node types for Databricks clusters."""
    try:
        result = await clusters.list_node_types()
        filtered = []
        for node in result.get("node_types", []):
            filtered.append({
                "node_type_id": node.get("node_type_id"),
                "memory_mb": node.get("memory_mb"),
                "num_cores": node.get("num_cores"),
                "description": node.get("description"),
                "instance_type_id": node.get("instance_type_id"),
                "category": node.get("category"),
                "num_gpus": node.get("num_gpus"),
                "node_info": node.get("node_info", {})
            })
        return [{"text": json.dumps(filtered, indent=2)}]
    except Exception as e:
        return [{"text": json.dumps({"error": str(e)})}]

@mcp_app.tool(
    name="list_spark_versions",
    description="List all available Spark versions for Databricks clusters"
)
async def tool_list_spark_versions() -> List[TextContent]:
    """List all available Spark versions for Databricks clusters."""
    try:
        result = await clusters.list_spark_versions()
        return [{"text": json.dumps(result)}]
    except Exception as e:
        return [{"text": json.dumps({"error": str(e)})}]

@mcp_app.tool(
    name="get_cluster",
    description="""
    Get information about a specific Databricks cluster.
    
    Required parameters:
    - cluster_id (str): ID of the cluster to get information about
    """
)
async def tool_get_cluster(params: Dict[str, Any]) -> List[TextContent]:
    """Get information about a specific Databricks cluster."""
    try:
        # Validate required parameters
        if "cluster_id" not in params:
            raise ValueError("Missing required parameter: cluster_id")
        
        # Get cluster information
        result = await clusters.get_cluster(params["cluster_id"])
        return [{"text": json.dumps(result)}]
    except Exception as e:
        return [{"text": json.dumps({"error": str(e)})}] 
