"""
Databricks SQL statement execution functionality.
"""

from typing import Any, Dict, Optional
from pydantic import BaseModel, Field

from src.core.http import make_databricks_request
from src.core.logging import create_logger

logger = create_logger(__name__)

STATEMENT_EXECUTE_ENDPOINT = "/api/2.0/sql/statements"

class StatementExecution(BaseModel):
    """Configuration for executing a SQL statement."""
    warehouse_id: str = Field(..., description="ID of the SQL warehouse to use")
    statement: str = Field(..., description="The SQL statement to execute")
    catalog: Optional[str] = Field(None, description="The catalog to use")
    schema: Optional[str] = Field(None, description="The schema to use")
    disposition: Optional[str] = Field("INLINE", description="How to return the result (INLINE or EXTERNAL_LINKS)")
    wait_timeout: Optional[int] = Field(None, description="Time in seconds to wait for completion")

    class Config:
        extra = "allow"  # Allow additional fields that might be supported by Databricks

async def execute_sql_statement(
    warehouse_id: str,
    statement: str,
    *,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    disposition: str = "INLINE",
    wait_timeout: Optional[int] = None,
    **additional_config: Any
) -> Dict[str, Any]:
    """
    Execute a SQL statement using a SQL warehouse.

    Documentation page: https://docs.databricks.com/api/workspace/statementexecution/executestatement

    Args:
        warehouse_id: ID of the SQL warehouse to use
        statement: The SQL statement to execute
        catalog: Optional catalog to use
        schema: Optional schema to use
        disposition: How to return the result (INLINE or EXTERNAL_LINKS) (default: INLINE)
        wait_timeout: Optional time in seconds to wait for completion
        **additional_config: Additional configuration parameters supported by Databricks

    Returns:
        Dict containing the statement execution results including:
        - statement_id: ID of the executed statement
        - status: Current status of the execution
        - result: Query results if available

    Example:
        >>> result = await execute_sql_statement(
        ...     warehouse_id="warehouse_id",
        ...     statement="SELECT * FROM my_table LIMIT 10",
        ...     schema="my_schema"
        ... )
        >>> print(f"Statement ID: {result['statement_id']}")
    """
    logger.info(f"Executing SQL statement on warehouse {warehouse_id}")
    
    # Construct the execution configuration
    execution_config = {
        "warehouse_id": warehouse_id,
        "statement": statement,
        "disposition": disposition
    }

    if catalog:
        execution_config["catalog"] = catalog
    if schema:
        execution_config["schema"] = schema
    if wait_timeout:
        execution_config["wait_timeout"] = wait_timeout

    # Add any additional configurations
    execution_config.update(additional_config)

    # Validate the configuration
    StatementExecution(**execution_config)

    logger.debug(f"Statement execution configuration: {execution_config}")

    # Make the API request
    response = await make_databricks_request(
        endpoint=STATEMENT_EXECUTE_ENDPOINT,
        method="POST",
        data=execution_config
    )

    logger.info(f"Successfully executed SQL statement with ID: {response.get('statement_id')}")
    return response
