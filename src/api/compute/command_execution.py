"""
Databricks Command Execution API functionality.
"""

from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field

from src.core.http import make_databricks_request
from src.core.logging import create_logger

logger = create_logger(__name__)

COMMAND_EXECUTION_ENDPOINT = "/api/1.2/commands/execute"
CREATE_CONTEXT_ENDPOINT = "/api/1.2/contexts/create"
COMMAND_STATUS_ENDPOINT = "/api/1.2/commands/status"

class CommandExecutionConfig(BaseModel):
    """Configuration for executing a command on a Databricks cluster."""
    cluster_id: str = Field(..., description="ID of the cluster to run the command on")
    language: str = Field(..., description="Language of the command (python, scala, sql)")
    command: str = Field(..., description="The command to execute")
    context_id: Optional[str] = Field(None, description="Context ID for maintaining session state (optional)")

    class Config:
        extra = "allow"  # Allow additional fields that might be supported by Databricks

class ContextCreationConfig(BaseModel):
    """Configuration for creating an execution context on a Databricks cluster."""
    cluster_id: str = Field(..., description="ID of the cluster to create the context on")
    language: str = Field(..., description="Language for the execution context (python, scala, sql)")

    class Config:
        extra = "allow"  # Allow additional fields that might be supported by Databricks

class CommandStatusConfig(BaseModel):
    """Configuration for retrieving the status of a command execution."""
    command_id: str = Field(..., description="ID of the command to get status for")
    cluster_id: str = Field(..., description="ID of the cluster where the command was executed")
    context_id: str = Field(..., description="Context ID for the command execution")

    class Config:
        extra = "allow"  # Allow additional fields that might be supported by Databricks


async def execute_command(
    cluster_id: str,
    language: str,
    command: str,
    context_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Execute a command on a running Databricks cluster.

    Documentation page: https://docs.databricks.com/api/workspace/commandexecution/execute

    Args:
        cluster_id: ID of the cluster to run the command on
        language: Language of the command (python, scala, sql, or r)
        command: The command to execute
        context_id: Optional context ID for maintaining session state

    Returns:
        Dict containing the command execution response including results and metadata

    Raises:
        DatabricksAPIError: If the command execution fails
        ValueError: If an invalid language is provided

    Example:
        >>> result = await execute_command(
        ...     cluster_id="1234-567890-abcdef",
        ...     language="python",
        ...     command="print('Hello, world!')"
        ... )
        >>> print(result["results"]["data"])
    """
    # Validate language
    valid_languages = ["python", "scala", "sql"]
    if language.lower() not in valid_languages:
        raise ValueError(f"Invalid language: {language}. Must be one of: {', '.join(valid_languages)}")

    # Construct the command execution request
    execution_config = {
        "clusterId": cluster_id,
        "language": language.lower(),
        "command": command
    }

    # Add optional context_id if provided
    if context_id:
        execution_config["contextId"] = context_id

    # Validate the configuration
    CommandExecutionConfig(
        cluster_id=cluster_id,
        language=language.lower(),
        command=command,
        context_id=context_id
    )

    logger.info(f"Executing {language} command on cluster: {cluster_id}")
    logger.debug(f"Command: {command[:100]}...")  # Log first 100 chars of command

    # Make the API request
    response = await make_databricks_request(
        endpoint=COMMAND_EXECUTION_ENDPOINT,
        method="POST",
        data=execution_config
    )

    logger.info(f"Successfully executed command on cluster {cluster_id}")
    return response

async def create_execution_context(
    cluster_id: str,
    language: str
) -> Dict[str, Any]:
    """
    Create an execution context on a running Databricks cluster.
    
    Documentation page: https://docs.databricks.com/api/workspace/commandexecution/create
    
    Args:
        cluster_id: ID of the cluster to create the context on
        language: Language for the execution context (python, scala, sql, or r)
        
    Returns:
        Dict containing the context creation response including the context ID
        
    Raises:
        DatabricksAPIError: If the context creation fails
        ValueError: If an invalid language is provided
        
    Example:
        >>> context = await create_context(
        ...     cluster_id="1234-567890-abcdef",
        ...     language="python"
        ... )
        >>> context_id = context["id"]
    """
    # Validate language
    valid_languages = ["python", "scala", "sql"]
    if language.lower() not in valid_languages:
        raise ValueError(f"Invalid language: {language}. Must be one of: {', '.join(valid_languages)}")
        
    # Construct the context creation request
    context_config = {
        "clusterId": cluster_id,
        "language": language.lower()
    }
    
    # Validate the configuration
    ContextCreationConfig(
        cluster_id=cluster_id,
        language=language.lower()
    )
    
    logger.info(f"Creating {language} execution context on cluster: {cluster_id}")
    
    # Make the API request
    response = await make_databricks_request(
        endpoint=CREATE_CONTEXT_ENDPOINT,
        method="POST",
        data=context_config
    )
    
    logger.info(f"Successfully created execution context on cluster {cluster_id}")
    return response

async def get_command_status(
    command_id: str,
    cluster_id: str,
    context_id: str
) -> Dict[str, Any]:
    """
    Get the status of a command execution on a Databricks cluster.
    
    Documentation page: https://docs.databricks.com/api/workspace/commandexecution/commandstatus
    
    Args:
        command_id: ID of the command to get status for
        cluster_id: ID of the cluster where the command was executed
        context_id: Context ID for the command execution
        
    Returns:
        Dict containing the command status information
        
    Raises:
        DatabricksAPIError: If the status retrieval fails
        
    Example:
        >>> status = await get_command_status(
        ...     command_id="abcd1234",
        ...     cluster_id="1234-567890-abcdef",
        ...     context_id="5678abcd"
        ... )
        >>> print(status["status"])
    """
    # Construct the status request
    status_config = {
        "commandId": command_id,
        "clusterId": cluster_id,
        "contextId": context_id
    }
    
    # Validate the configuration
    CommandStatusConfig(
        command_id=command_id,
        cluster_id=cluster_id,
        context_id=context_id
    )
    
    logger.info(f"Getting status for command: {command_id} on cluster: {cluster_id}")
    
    # Make the API request
    response = await make_databricks_request(
        endpoint=COMMAND_STATUS_ENDPOINT,
        method="GET",
        params=status_config
    )
    
    logger.info(f"Successfully retrieved status for command {command_id}")
    return response
