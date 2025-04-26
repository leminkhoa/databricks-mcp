"""
Databricks SQL warehouses management functionality.
"""

from typing import Any, Dict, Optional
from pydantic import BaseModel, Field

from src.core.http import make_databricks_request
from src.core.logging import create_logger

logger = create_logger(__name__)

WAREHOUSES_LIST_ENDPOINT = "/api/2.0/sql/warehouses"
WAREHOUSES_CREATE_ENDPOINT = "/api/2.0/sql/warehouses"

class WarehouseConfig(BaseModel):
    """Configuration for creating a SQL warehouse."""
    name: str = Field(..., description="Name of the warehouse")
    cluster_size: str = Field(..., description="Size of the clusters: 2X-Small to 4X-Large")
    min_num_clusters: int = Field(1, description="Minimum number of clusters")
    max_num_clusters: int = Field(1, description="Maximum number of clusters")
    auto_stop_mins: int = Field(120, description="Number of minutes of inactivity after which warehouse will be stopped")
    enable_photon: Optional[bool] = Field(True, description="Enable Photon acceleration")
    spot_instance_policy: Optional[str] = Field("COST_OPTIMIZED", description="Policy for spot instances")
    warehouse_type: Optional[str] = Field("PRO", description="Type of warehouse (PRO or CLASSIC)")
    channel: Optional[str] = Field(None, description="Release channel (CHANNEL_NAME_CURRENT or CHANNEL_NAME_PREVIEW)")
    tags: Optional[Dict[str, str]] = Field(default_factory=dict, description="Resource tags")

    class Config:
        extra = "allow"  # Allow additional fields that might be supported by Databricks

async def list_sql_warehouses() -> Dict[str, Any]:
    """
    List all SQL warehouses in the workspace.

    Documentation page: https://docs.databricks.com/api/workspace/warehouses/list

    Returns:
        Dict containing the list of SQL warehouses

    Example:
        >>> response = await list_sql_warehouses()
        >>> warehouses = response.get("warehouses", [])
        >>> for warehouse in warehouses:
        ...     print(f"Warehouse: {warehouse['name']} (ID: {warehouse['id']})")
    """
    logger.info("Listing SQL warehouses")
    try:
        response = await make_databricks_request(WAREHOUSES_LIST_ENDPOINT)
        logger.debug(f"Raw API response: {response}")
        return response
    except Exception as e:
        error_msg = f"Error listing SQL warehouses: {str(e)}"
        logger.error(error_msg)
        return {"error": error_msg}

async def create_sql_warehouse(
    name: str,
    cluster_size: str,
    *,
    min_num_clusters: int = 1,
    max_num_clusters: int = 1,
    auto_stop_mins: int = 120,
    enable_photon: bool = True,
    spot_instance_policy: str = "COST_OPTIMIZED",
    warehouse_type: str = "PRO",
    channel: Optional[str] = None,
    tags: Optional[Dict[str, str]] = None,
    **additional_config: Any
) -> Dict[str, Any]:
    """
    Create a new SQL warehouse.

    Documentation page: https://docs.databricks.com/api/workspace/warehouses/create

    Args:
        name: Name of the warehouse
        cluster_size: Size of the clusters (2X-Small to 4X-Large)
        min_num_clusters: Minimum number of clusters (default: 1)
        max_num_clusters: Maximum number of clusters (default: 1)
        auto_stop_mins: Minutes of inactivity after which warehouse will be stopped (default: 120)
        enable_photon: Enable Photon acceleration (default: True)
        spot_instance_policy: Policy for spot instances (default: COST_OPTIMIZED)
        warehouse_type: Type of warehouse (PRO or CLASSIC) (default: PRO)
        channel: Release channel (CHANNEL_NAME_CURRENT or CHANNEL_NAME_PREVIEW)
        tags: Resource tags
        **additional_config: Additional configuration parameters supported by Databricks

    Returns:
        Dict containing the warehouse information including the warehouse_id

    Example:
        >>> warehouse = await create_sql_warehouse(
        ...     name="My Warehouse",
        ...     cluster_size="2X-Small",
        ...     min_num_clusters=1,
        ...     max_num_clusters=2,
        ...     auto_stop_mins=60
        ... )
        >>> warehouse_id = warehouse["id"]
    """
    logger.info(f"Creating SQL warehouse: {name}")

    # Construct the warehouse configuration
    warehouse_config = {
        "name": name,
        "cluster_size": cluster_size,
        "min_num_clusters": min_num_clusters,
        "max_num_clusters": max_num_clusters,
        "auto_stop_mins": auto_stop_mins,
        "enable_photon": enable_photon,
        "spot_instance_policy": spot_instance_policy,
        "warehouse_type": warehouse_type
    }

    if channel:
        warehouse_config["channel"] = channel
    if tags:
        warehouse_config["tags"] = tags

    # Add any additional configurations
    warehouse_config.update(additional_config)

    # Validate the configuration
    WarehouseConfig(**warehouse_config)

    logger.debug(f"Warehouse configuration: {warehouse_config}")

    # Make the API request
    response = await make_databricks_request(
        endpoint=WAREHOUSES_CREATE_ENDPOINT,
        method="POST",
        data=warehouse_config
    )

    logger.info(f"Successfully created SQL warehouse {name} with ID: {response.get('id')}")
    return response
