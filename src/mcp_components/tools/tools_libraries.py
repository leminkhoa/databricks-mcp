"""
Databricks MCP tools implementation for library management operations.
"""

import json
from typing import Any, Dict, List

from mcp.types import TextContent
from src.api.compute import libraries
from .. import mcp_app

@mcp_app.tool(
    name="install_libraries",
    description="""
    Install libraries on a running Databricks cluster.
    
    Required parameters:
    - cluster_id (str): ID of the cluster to install libraries on
    - libraries (list): List of library configurations. Each library should be a dict with one of:
        - jar: "s3://my-bucket/my-jar.jar"
        - egg: "s3://my-bucket/my.egg"
        - whl: "s3://my-bucket/my.whl"
        - pypi: {"package": "simplejson"}
        - maven: {"coordinates": "org.jsoup:jsoup:1.7.2", "repo": "..."}
        - cran: {"package": "forecast"}
    
    Example request:
    ```json
    {
    "cluster_id": "1234-56789-abcde",
    "libraries": [
        {
        "pypi": {
            "package": "numpy",
            "repo": "http://my-pypi-repo.com"
        }
        },
        {
        "jar": "/Workspace/path/to/library.jar"
        },
        {
        "whl": "/Workspace/path/to/library.whl"
        },
        {
        "maven": {
            "coordinates": "com.databricks:spark-csv_2.11:1.5.0",
            "exclusions": [
            "org.slf4j:slf4j-log4j12"
            ],
            "repo": "http://my-maven-repo.com"
        }
        },
        {
        "cran": {
            "package": "ggplot2",
            "repo": "http://cran.us.r-project.org"
        }
        },
        {
        "requirements": "/Workspace/path/to/requirements.txt"
        }
    ]
    }
    ```
    """
)
async def tool_install_libraries(params: Dict[str, Any]) -> List[TextContent]:
    """Install libraries on a running Databricks cluster."""
    try:
        # Validate required parameters
        required_params = ["cluster_id", "libraries"]
        missing_params = [p for p in required_params if p not in params]
        if missing_params:
            raise ValueError(f"Missing required parameters: {', '.join(missing_params)}")
        
        # Extract parameters
        cluster_id = params["cluster_id"]
        libraries_config = params["libraries"]
        
        # Validate libraries parameter is a list
        if not isinstance(libraries_config, list):
            raise ValueError("The 'libraries' parameter must be a list of library configurations")
        
        # Install the libraries
        result = await libraries.install_libraries(
            cluster_id=cluster_id,
            libraries=libraries_config
        )
        
        # Format the results nicely
        formatted_result = {
            "status": "submitted",
            "cluster_id": cluster_id,
            "libraries": libraries_config,
            "response": result
        }
        
        return [{"text": json.dumps(formatted_result, indent=2)}]
    except Exception as e:
        return [{"text": json.dumps({"error": str(e)})}]
