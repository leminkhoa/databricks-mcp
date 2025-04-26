"""
Databricks workspace objects management functionality.
"""

import base64
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field

from src.core.http import make_databricks_request
from src.core.logging import create_logger
from src.core.utils import is_base64

logger = create_logger(__name__)

WORKSPACE_DELETE_ENDPOINT = "/api/2.0/workspace/delete"
WORKSPACE_GET_STATUS_ENDPOINT = "/api/2.0/workspace/get-status"
WORKSPACE_IMPORT_ENDPOINT = "/api/2.0/workspace/import"
WORKSPACE_MKDIRS_ENDPOINT = "/api/2.0/workspace/mkdirs"

class WorkspaceObjectImport(BaseModel):
    """Configuration for importing a workspace object."""
    path: str = Field(..., description="The absolute path of the object to import")
    content: str = Field(..., description="The base64-encoded content of the object")
    format: str = Field(..., description="The format of the object (SOURCE, HTML, JUPYTER, DBC)")
    language: Optional[str] = Field(None, description="The language (PYTHON, R, SQL)")
    overwrite: Optional[bool] = Field(False, description="Whether to overwrite an existing object")

async def delete_workspace_object(path: str, recursive: bool = False) -> Dict[str, Any]:
    """
    Delete an object from the Databricks workspace.

    Documentation page: https://docs.databricks.com/api/workspace/workspace/delete

    Args:
        path: The absolute path of the object to delete
        recursive: Optional flag to recursively delete the object and its contents

    Returns:
        Dict containing the response from the API

    Raises:
        DatabricksAPIError: If the deletion fails

    Example:
        >>> response = await delete_workspace_object("/path/to/notebook", recursive=True)
    """
    logger.info(f"Deleting workspace object at path: {path}")
    
    data = {
        "path": path,
        "recursive": recursive
    }
    
    response = await make_databricks_request(
        endpoint=WORKSPACE_DELETE_ENDPOINT,
        method="POST",
        data=data
    )

    logger.info(f"Successfully deleted workspace object at path: {path}")
    return response

async def get_workspace_object_status(path: str) -> Dict[str, Any]:
    """
    Get the status of an object in the Databricks workspace.

    Documentation page: https://docs.databricks.com/api/workspace/workspace/getstatus

    Args:
        path: The absolute path of the object

    Returns:
        Dict containing object information including:
        - object_type: The type of the object (NOTEBOOK, DIRECTORY, LIBRARY, etc.)
        - path: The absolute path of the object
        - language: The language of the object (if applicable)

    Example:
        >>> status = await get_workspace_object_status("/path/to/notebook")
        >>> print(f"Object type: {status.get('object_type')}")
    """
    logger.info(f"Getting status for workspace object at path: {path}")
    
    response = await make_databricks_request(
        endpoint=WORKSPACE_GET_STATUS_ENDPOINT,
        method="GET",
        params={"path": path}
    )

    logger.debug(f"Raw API response: {response}")
    return response

async def import_workspace_object(
    path: str,
    content: str,
    format: str,
    language: Optional[str] = None,
    overwrite: bool = False
) -> Dict[str, Any]:
    """
    Import an object into the Databricks workspace.

    Documentation page: https://docs.databricks.com/api/workspace/workspace/import

    Args:
        path: The absolute path where the object will be imported
        content: The content to import (will be base64 encoded if not already)
        format: The format of the object (SOURCE, HTML, JUPYTER, DBC)
        language: Optional language specification (PYTHON, R, SQL)
        overwrite: Whether to overwrite an existing object

    Returns:
        Dict containing the response from the API

    Raises:
        DatabricksAPIError: If the import fails

    Example:
        >>> response = await import_workspace_object(
        ...     path="/path/to/notebook",
        ...     content="print('Hello World')",
        ...     format="SOURCE",
        ...     language="PYTHON"
        ... )
    """
    logger.info(f"Importing workspace object to path: {path}")
    
    # Ensure content is base64 encoded
    if not is_base64(content):
        content = base64.b64encode(content.encode("utf-8")).decode("utf-8")
    
    # Prepare import configuration
    import_config = {
        "path": path,
        "content": content,
        "format": format,
        "overwrite": overwrite
    }
    
    if language:
        import_config["language"] = language
    
    # Validate the configuration
    WorkspaceObjectImport(**import_config)
    
    logger.debug(f"Import configuration: {import_config}")
    
    response = await make_databricks_request(
        endpoint=WORKSPACE_IMPORT_ENDPOINT,
        method="POST",
        data=import_config
    )

    logger.info(f"Successfully imported workspace object to path: {path}")
    return response

async def create_workspace_directory(path: str) -> Dict[str, Any]:
    """
    Create a directory in the Databricks workspace.

    Documentation page: https://docs.databricks.com/api/workspace/workspace/mkdirs

    Args:
        path: The absolute path of the directory to create

    Returns:
        Dict containing the response from the API

    Raises:
        DatabricksAPIError: If the directory creation fails

    Example:
        >>> response = await create_workspace_directory("/path/to/new/directory")
    """
    logger.info(f"Creating directory at path: {path}")
    
    response = await make_databricks_request(
        endpoint=WORKSPACE_MKDIRS_ENDPOINT,
        method="POST",
        data={"path": path}
    )

    logger.info(f"Successfully created directory at path: {path}")
    return response
