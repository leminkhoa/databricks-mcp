"""
Async HTTP client for Databricks API requests.
"""

import json
from typing import Any, Dict, Optional

import httpx
from httpx import HTTPError, Response, TimeoutException

from .config import get_api_headers, get_databricks_api_url
from .logging import create_logger

logger = create_logger(__name__)

class DatabricksAPIError(Exception):
    """Custom exception for Databricks API errors."""
    
    def __init__(self, message: str, status_code: Optional[int] = None, response_body: Optional[str] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body

    def __str__(self) -> str:
        error_msg = f"Databricks API Error: {super().__str__()}"
        if self.status_code:
            error_msg += f" (Status: {self.status_code})"
        if self.response_body:
            error_msg += f"\nResponse: {self.response_body}"
        return error_msg

async def make_databricks_request(
    endpoint: str,
    method: str = "GET",
    data: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
    timeout: float = 30.0
) -> Dict[str, Any]:
    """
    Make an async request to the Databricks API with proper error handling.

    Args:
        endpoint: The API endpoint path (e.g., "/api/2.0/clusters/list")
        method: HTTP method to use (default: "GET")
        data: Request body data for POST/PUT requests (default: None)
        params: Query parameters for the request (default: None)
        timeout: Request timeout in seconds (default: 30.0)

    Returns:
        The JSON response from the API

    Raises:
        DatabricksAPIError: If the request fails or returns an error response
        TimeoutException: If the request times out
        HTTPError: For other HTTP-related errors

    Example:
        >>> response = await make_databricks_request("/api/2.0/clusters/list")
        >>> clusters = response.get("clusters", [])
    """
    url = get_databricks_api_url(endpoint)
    headers = get_api_headers()

    async with httpx.AsyncClient() as client:
        try:
            logger.debug(f"Making {method} request to {url}")
            
            response: Response = await client.request(
                method=method,
                url=url,
                headers=headers,
                json=data,
                params=params,
                timeout=timeout
            )

            # Log request details at debug level
            logger.debug(f"Request to {url} completed with status {response.status_code}")

            # Handle error responses
            if response.status_code >= 400:
                error_body = None
                try:
                    error_body = response.json()
                except json.JSONDecodeError:
                    error_body = response.text

                error_message = f"Request failed with status {response.status_code}"
                if isinstance(error_body, dict):
                    error_message = error_body.get("message", error_message)
                
                logger.error(f"Databricks API error: {error_message}")
                raise DatabricksAPIError(
                    message=error_message,
                    status_code=response.status_code,
                    response_body=str(error_body)
                )

            # Parse and return JSON response
            try:
                return response.json()
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON response: {e}")
                raise DatabricksAPIError(
                    message="Invalid JSON response from API",
                    status_code=response.status_code,
                    response_body=response.text
                )

        except TimeoutException as e:
            logger.error(f"Request to {url} timed out after {timeout} seconds")
            raise DatabricksAPIError(f"Request timed out: {str(e)}")
            
        except HTTPError as e:
            logger.error(f"HTTP error during request to {url}: {str(e)}")
            raise DatabricksAPIError(f"HTTP error: {str(e)}")
            
        except Exception as e:
            logger.error(f"Unexpected error during request to {url}: {str(e)}")
            raise DatabricksAPIError(f"Unexpected error: {str(e)}") 
