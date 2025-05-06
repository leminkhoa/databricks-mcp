"""
Configuration settings for the Databricks MCP server.
"""

import os
import sys
from typing import Dict, Optional
from urllib.parse import urlparse
from .logging import create_logger, SingletonLogger

logger = create_logger(__name__)

# Import dotenv if available, but don't require it
try:
    from dotenv import load_dotenv
    logger.info("Start loading .env if exists")
    if load_dotenv(override=False):
        logger.info("Successfully loaded .env")
    else:
        logger.warning("No .env file found")
except ImportError:
    logger.warning("python-dotenv not found, environment variables must be set manually")

# Check if required environment variables are set
if not os.environ.get("DATABRICKS_HOST") or not os.environ.get("DATABRICKS_TOKEN"):
    logger.error("DATABRICKS_HOST and DATABRICKS_TOKEN must be provided. Please set these environment variables and try again.")
    sys.exit(1)

# Version
VERSION = "0.1.0"

class ConfigurationError(Exception):
    """Raised when there is a configuration error."""
    pass

class Settings:
    """Settings for the MCP application."""
    
    def __init__(self):
        """Initialize settings from environment variables."""
        # Databricks API configuration
        self._databricks_host = self._get_env("DATABRICKS_HOST")
        self._databricks_token = self._get_env("DATABRICKS_TOKEN")
        
        # Server configuration
        self.server_host = self._get_env("SERVER_HOST", "0.0.0.0")
        self.server_port = int(self._get_env("SERVER_PORT", "8000"))
        self.debug = self._get_env("DEBUG", "false").lower() == "true"
        
        # Logging
        self.log_level = self._get_env("LOG_LEVEL", "INFO").upper()
        
        # Version
        self.version = VERSION

        # Transport
        self.transport = self._get_env("TRANSPORT", "stdio").lower()
        
        # Validate settings
        self._validate_settings()
        
        # Update logger level after settings are initialized
        SingletonLogger.update_log_level(self.log_level)

    @staticmethod
    def _get_env(key: str, default: Optional[str] = None) -> str:
        """
        Get environment variable with optional default.
        
        Args:
            key: Environment variable name
            default: Default value if not set
            
        Returns:
            Environment variable value
        """
        return os.environ.get(key, default)

    def _validate_settings(self) -> None:
        """Validate all settings."""
        self._validate_databricks_host()
        self._validate_databricks_token()
        self._validate_server_port()
        self._validate_log_level()

    def _validate_databricks_host(self) -> None:
        """Validate Databricks host URL."""
        if not self._databricks_host.startswith(("https://", "http://")):
            raise ConfigurationError("DATABRICKS_HOST must start with http:// or https://")
        
        try:
            parsed = urlparse(self._databricks_host)
            if not parsed.netloc:
                raise ValueError("Invalid URL")
        except Exception as e:
            raise ConfigurationError(f"Invalid DATABRICKS_HOST URL: {e}")

    def _validate_databricks_token(self) -> None:
        """Validate Databricks token format."""
        if not self._databricks_token:
            raise ConfigurationError("DATABRICKS_TOKEN cannot be empty")
            
        # Check if token starts with 'dapi_'
        if not self._databricks_token.startswith("dapi"):
            raise ConfigurationError("DATABRICKS_TOKEN must start with 'dapi_'")
            
        # Check minimum length (assuming reasonable minimum token length)
        if len(self._databricks_token) < 10:
            raise ConfigurationError("DATABRICKS_TOKEN is too short")

    def _validate_server_port(self) -> None:
        """Validate server port."""
        if not 0 <= self.server_port <= 65535:
            raise ConfigurationError("SERVER_PORT must be between 0 and 65535")

    def _validate_log_level(self) -> None:
        """Validate log level."""
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if self.log_level not in valid_levels:
            raise ConfigurationError(f"LOG_LEVEL must be one of: {', '.join(valid_levels)}")

    @property
    def databricks_host(self) -> str:
        """Get Databricks host URL."""
        return self._databricks_host

    @property
    def databricks_token(self) -> str:
        """Get Databricks token."""
        return self._databricks_token

def get_api_headers() -> Dict[str, str]:
    """Get headers for Databricks API requests."""
    return {
        "Authorization": f"Bearer {settings.databricks_token}",
        "Content-Type": "application/json",
    }

def get_databricks_api_url(endpoint: str) -> str:
    """
    Construct the full Databricks API URL.
    
    Args:
        endpoint: The API endpoint path, e.g., "/api/2.0/clusters/list"
    
    Returns:
        Full URL to the Databricks API endpoint
    """
    # Ensure endpoint starts with a slash
    if not endpoint.startswith("/"):
        endpoint = f"/{endpoint}"

    # Remove trailing slash from host if present
    host = settings.databricks_host.rstrip("/")
    
    return f"{host}{endpoint}"

# Create global settings instance
settings = Settings()
