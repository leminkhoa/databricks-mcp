"""
Databricks MCP prompts.

This module defines prompts for the Databricks MCP integration,
directly registering them with the mcp_app instance.
"""

from fastmcp.prompts.prompt import UserMessage, AssistantMessage, Message
from .. import mcp_app

@mcp_app.prompt(
    name="create-databricks-cluster-configurations",
    description="Generate configuration for creating a Databricks cluster",
)
def create_databricks_cluster_configurations_prompt(
    cluster_name: str,
    node_type_id: str,
    spark_version: str,
    purpose: str = "General Purpose",
    autoscaling: str = "yes",
    min_workers: str = "2",
    max_workers: str = "4",
    fixed_workers: str = "4",
    additional_config: str = "{}"
) -> list[Message]:
    """
    Generate a prompt for creating a Databricks cluster.

    Args:
        cluster_name: Name of the cluster (required)
        node_type_id: Type of nodes (required)
        spark_version: Spark version to use (required)
        purpose: Purpose of the cluster (optional)
        autoscaling: Whether to enable autoscaling (optional)
        min_workers: Minimum number of workers when autoscaling (optional)
        max_workers: Maximum number of workers when autoscaling (optional)
        fixed_workers: Fixed number of workers if not using autoscaling (optional)
        additional_config: Any additional configuration options as JSON (optional)
    Returns:
        Prompt result with messages for the AI assistant
    """
    # Generate prompt message
    prompt_text = f"""
        I need help creating a Databricks cluster with the following requirements:

        - Cluster name: {cluster_name}
        - Node type: {node_type_id}
        - Spark version: {spark_version}
        - Purpose: {purpose}
        - Autoscaling: {autoscaling}
        """

    if autoscaling.lower() == "yes":
        prompt_text += f"- Minimum workers: {min_workers}\n"
        prompt_text += f"- Maximum workers: {max_workers}\n"
    else:
        prompt_text += f"- Fixed number of workers: {fixed_workers}\n"

    if additional_config and additional_config != "{}":
        prompt_text += f"- Additional configuration: {additional_config}\n"

    prompt_text += """
        Based on these requirements, please provide:
        1. An explanation of the configuration choices, especially for any recommended settings
        2. Best practices for this type of cluster configuration
        """

    return [
        UserMessage(content=prompt_text),
    ]
