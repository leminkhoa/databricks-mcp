"""
Databricks cluster management functionality.
"""

from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field

from src.core.http import make_databricks_request
from src.core.logging import create_logger

logger = create_logger(__name__)

CLUSTER_CREATE_ENDPOINT = "/api/2.1/clusters/create"
CLUSTER_LIST_ENDPOINT = "/api/2.1/clusters/list"
CLUSTER_DELETE_ENDPOINT = "/api/2.1/clusters/delete"
CLUSTER_START_ENDPOINT = "/api/2.1/clusters/start"
CLUSTER_LIST_NODE_TYPES_ENDPOINT = "/api/2.1/clusters/list-node-types"
CLUSTER_SPARK_VERSIONS_ENDPOINT = "/api/2.1/clusters/spark-versions"
CLUSTER_GET_ENDPOINT = "/api/2.1/clusters/get"



class AutoScale(BaseModel):
    """Autoscaling configuration for a cluster."""
    min_workers: int = Field(..., description="Minimum number of workers")
    max_workers: int = Field(..., description="Maximum number of workers")

class ClusterConfig(BaseModel):
    """Configuration for creating a Databricks cluster."""
    cluster_name: str = Field(..., description="Cluster name")
    spark_version: str = Field(..., description="Spark version to use")
    node_type_id: str = Field(..., description="Type of nodes to use")
    autoscale: Optional[AutoScale] = Field(None, description="Autoscaling configuration")
    num_workers: Optional[int] = Field(None, description="Fixed number of workers (if not using autoscale)")
    spark_conf: Optional[Dict[str, str]] = Field(default_factory=dict, description="Spark configuration properties")
    aws_attributes: Optional[Dict[str, Any]] = Field(None, description="AWS-specific attributes")
    azure_attributes: Optional[Dict[str, Any]] = Field(None, description="Azure-specific attributes")
    custom_tags: Optional[Dict[str, str]] = Field(default_factory=dict, description="Custom tags")
    init_scripts: Optional[List[Dict[str, str]]] = Field(default_factory=list, description="Initialization scripts")

    class Config:
        extra = "allow"  # Allow additional fields that might be supported by Databricks

async def create_cluster(
    cluster_name: str,
    node_type_id: str,
    spark_version: str,
    *,
    autoscale: Optional[Dict[str, int]] = None,
    num_workers: Optional[int] = None,
    spark_conf: Optional[Dict[str, str]] = None,
    custom_tags: Optional[Dict[str, str]] = None,
    init_scripts: Optional[List[Dict[str, str]]] = None,
    **additional_config: Any
) -> Dict[str, Any]:
    """
    Create a new Databricks cluster with the specified configuration.

    Documentation page: https://docs.databricks.com/api/workspace/clusters/create

    Args:
        cluster_name: Name of the cluster
        node_type_id: The node type ID (e.g., "Standard_DS3_v2" for Azure, "i3.xlarge" for AWS)
        spark_version: Spark version to use (e.g., "7.3.x-scala2.12")
        autoscale: Optional autoscaling configuration with min_workers and max_workers
        num_workers: Optional fixed number of workers (don't use with autoscale)
        spark_conf: Optional Spark configuration properties
        custom_tags: Optional custom tags for the cluster
        init_scripts: Optional list of initialization scripts
        **additional_config: Additional configuration parameters supported by Databricks

    Returns:
        Dict containing the cluster information including the cluster_id

    Raises:
        DatabricksAPIError: If the cluster creation fails
        ValueError: If both autoscale and num_workers are provided

    Example:
        >>> cluster = await create_cluster(
        ...     cluster_name="My Cluster",
        ...     node_type_id="Standard_DS3_v2",
        ...     spark_version="7.3.x-scala2.12",
        ...     autoscale={"min_workers": 2, "max_workers": 8},
        ...     spark_conf={"spark.speculation": "true"}
        ... )
        >>> cluster_id = cluster["cluster_id"]
    """
    if autoscale is not None and num_workers is not None:
        raise ValueError("Cannot specify both autoscale and num_workers")

    # Construct the cluster configuration
    cluster_config = {
        "cluster_name": cluster_name,
        "node_type_id": node_type_id,
        "spark_version": spark_version,
    }

    # Add optional configurations
    if autoscale:
        cluster_config["autoscale"] = AutoScale(**autoscale).dict()
    elif num_workers is not None:
        cluster_config["num_workers"] = num_workers

    if spark_conf:
        cluster_config["spark_conf"] = spark_conf
    if custom_tags:
        cluster_config["custom_tags"] = custom_tags
    if init_scripts:
        cluster_config["init_scripts"] = init_scripts

    # Add any additional configurations
    cluster_config.update(additional_config)

    # Validate the configuration
    ClusterConfig(**cluster_config)

    logger.info(f"Creating cluster: {cluster_name}")
    logger.debug(f"Cluster configuration: {cluster_config}")

    # Make the API request
    response = await make_databricks_request(
        endpoint=CLUSTER_CREATE_ENDPOINT,
        method="POST",
        data=cluster_config
    )

    logger.info(f"Successfully created cluster {cluster_name} with ID: {response.get('cluster_id')}")
    return response

async def list_clusters() -> Dict[str, Any]:
    """
    List all clusters in the Databricks workspace.

    Documentation page: https://docs.databricks.com/api/workspace/clusters/list

    Returns:
        Dict containing the clusters list and pagination tokens

    Example:
        >>> response = await list_clusters()
        >>> clusters = response.get("clusters", [])
        >>> for cluster in clusters:
        ...     print(f"Cluster: {cluster['cluster_name']} (ID: {cluster['cluster_id']})")
    """
    logger.info("Start listing clusters")
    try:
        response = await make_databricks_request(CLUSTER_LIST_ENDPOINT)
        logger.debug(f"Raw API response: {response}")
        
        if not isinstance(response, dict):
            logger.error(f"Unexpected response type: {type(response)}")
            return {"clusters": [], "next_page_token": "", "prev_page_token": ""}
            
        # Ensure the response has the expected structure
        if "clusters" not in response:
            response["clusters"] = []
            
        return response
    except Exception as e:
        logger.error(f"Error listing clusters: {str(e)}")
        return {"clusters": [], "next_page_token": "", "prev_page_token": "", "error": str(e)}

async def delete_cluster(cluster_id: str) -> Dict[str, Any]:
    """
    Delete a Databricks cluster by its ID.

    Documentation page: https://docs.databricks.com/api/workspace/clusters/delete

    Args:
        cluster_id: The ID of the cluster to delete

    Returns:
        Dict containing the response from the API

    Raises:
        DatabricksAPIError: If the cluster deletion fails

    Example:
        >>> response = await delete_cluster("1234-567890-abc123")
        >>> print(f"Cluster deletion status: {response}")
    """
    logger.info(f"Deleting cluster with ID: {cluster_id}")
    
    # Make the API request
    response = await make_databricks_request(
        endpoint=CLUSTER_DELETE_ENDPOINT,
        method="POST",
        data={"cluster_id": cluster_id}
    )

    logger.info(f"Successfully deleted cluster with ID: {cluster_id}")
    return response

async def start_cluster(cluster_id: str) -> Dict[str, Any]:
    """
    Start a terminated Databricks cluster given its ID.

    Documentation page: https://docs.databricks.com/api/workspace/clusters/start

    Args:
        cluster_id: The ID of the cluster to start

    Returns:
        Dict containing the response from the API

    Raises:
        DatabricksAPIError: If the cluster start fails
    """
    logger.info(f"Starting cluster with ID: {cluster_id}")
    try:
        response = await make_databricks_request(
            endpoint=CLUSTER_START_ENDPOINT,
            method="POST",
            data={"cluster_id": cluster_id}
        )
        logger.info(f"Successfully started cluster with ID: {cluster_id}")
        return response
    except Exception as e:
        error_msg = f"Error starting cluster {cluster_id}: {str(e)}"
        logger.error(error_msg)
        return {"error": error_msg}

async def list_node_types() -> Dict[str, Any]:
    """
    List all available node types for Databricks clusters.

    Documentation page: https://docs.databricks.com/api/workspace/clusters/list-node-types

    Returns:
        Dict containing the list of available node types
    """
    logger.info("Listing available node types")
    try:
        response = await make_databricks_request(CLUSTER_LIST_NODE_TYPES_ENDPOINT)
        logger.debug(f"Raw API response: {response}")
        return response
    except Exception as e:
        error_msg = f"Error listing node types: {str(e)}"
        logger.error(error_msg)
        return {"error": error_msg}

async def list_spark_versions() -> Dict[str, Any]:
    """
    List all available Spark versions for Databricks clusters.

    Documentation page: https://docs.databricks.com/api/workspace/clusters/spark-versions

    Returns:
        Dict containing the list of available Spark versions
    """
    logger.info("Listing available Spark versions")
    try:
        response = await make_databricks_request(CLUSTER_SPARK_VERSIONS_ENDPOINT)
        logger.debug(f"Raw API response: {response}")
        return response
    except Exception as e:
        error_msg = f"Error listing Spark versions: {str(e)}"
        logger.error(error_msg)
        return {"error": error_msg}

async def get_cluster(cluster_id: str) -> Dict[str, Any]:
    """
    Get information about a specific Databricks cluster.

    Documentation page: https://docs.databricks.com/api/workspace/clusters/get

    Args:
        cluster_id: The ID of the cluster to get information about

    Returns:
        Dict containing the cluster information
    """
    logger.info(f"Getting information for cluster with ID: {cluster_id}")
    try:
        response = await make_databricks_request(
            endpoint=CLUSTER_GET_ENDPOINT,
            method="GET",
            params={"cluster_id": cluster_id}
        )
        logger.debug(f"Raw API response: {response}")
        return response
    except Exception as e:
        error_msg = f"Error getting cluster information for {cluster_id}: {str(e)}"
        logger.error(error_msg)
        return {"error": error_msg}

# def create_cluster(cluster_name: str, node_type: str, min_workers: int, max_workers: int) -> dict:
#     """
#     Create a new Databricks cluster.

