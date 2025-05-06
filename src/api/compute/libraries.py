"""
Databricks Libraries API functionality.
"""

from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field

from src.core.http import make_databricks_request
from src.core.logging import create_logger

logger = create_logger(__name__)

LIBRARIES_INSTALL_ENDPOINT = "/api/2.0/libraries/install"

class PyPiLibrary(BaseModel):
    """Configuration for PyPI library."""
    package: str
    repo: Optional[str] = None

class MavenLibrary(BaseModel):
    """Configuration for Maven library."""
    coordinates: str
    repo: Optional[str] = None
    exclusions: Optional[List[str]] = None

class CranLibrary(BaseModel):
    """Configuration for CRAN library."""
    package: str
    repo: Optional[str] = None

class Library(BaseModel):
    """Configuration for a single library."""
    pypi: Optional[PyPiLibrary] = None
    jar: Optional[str] = None
    egg: Optional[str] = None
    whl: Optional[str] = None
    maven: Optional[MavenLibrary] = None
    cran: Optional[CranLibrary] = None
    requirements: Optional[str] = None

class LibraryInstallConfig(BaseModel):
    """Configuration for installing libraries on a Databricks cluster."""
    cluster_id: str = Field(..., description="ID of the cluster to install libraries on")
    libraries: List[Library] = Field(..., description="List of libraries to install")

    class Config:
        extra = "allow"  # Allow additional fields that might be supported by Databricks

async def install_libraries(
    cluster_id: str,
    libraries: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Install libraries on a running Databricks cluster.

    Documentation page: https://docs.databricks.com/api/workspace/libraries/install

    Args:
        cluster_id: ID of the cluster to install libraries on
        libraries: List of library configurations. Each library should be a dict with one of:
            - jar: "s3://my-bucket/my-jar.jar"
            - egg: "s3://my-bucket/my.egg"
            - whl: "s3://my-bucket/my.whl"
            - pypi: {"package": "simplejson", "repo": "optional-repo-url"}
            - maven: {"coordinates": "org.jsoup:jsoup:1.7.2", "repo": "...", "exclusions": ["..."]}
            - cran: {"package": "forecast", "repo": "optional-repo-url"}
            - requirements: "/Workspace/path/to/requirements.txt"

    Returns:
        Dict containing the response from the library installation request

    Raises:
        DatabricksAPIError: If the library installation fails
        ValueError: If invalid library configuration is provided

    Example:
        >>> result = await install_libraries(
        ...     cluster_id="1234-567890-abcdef",
        ...     libraries=[
        ...         {"pypi": {"package": "pandas"}},
        ...         {"jar": "dbfs:/my/jar/file.jar"},
        ...         {"maven": {
        ...             "coordinates": "org.jsoup:jsoup:1.7.2",
        ...             "exclusions": ["org.slf4j:slf4j-log4j12"]
        ...         }}
        ...     ]
        ... )
    """
    # Construct the library installation request
    install_config = {
        "cluster_id": cluster_id,
        "libraries": libraries
    }

    # Validate the configuration
    LibraryInstallConfig(
        cluster_id=cluster_id,
        libraries=[Library(**lib) for lib in libraries]
    )

    logger.info(f"Installing libraries on cluster: {cluster_id}")
    logger.debug(f"Libraries to install: {libraries}")

    # Make the API request
    response = await make_databricks_request(
        endpoint=LIBRARIES_INSTALL_ENDPOINT,
        method="POST",
        data=install_config
    )

    logger.info(f"Successfully submitted library installation request for cluster {cluster_id}")
    return response
