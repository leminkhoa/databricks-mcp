"""
Logging configuration for the application.
"""

import logging
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional
import threading

class SingletonLogger:
    """
    A singleton logger class to ensure consistent logging configuration across the application.
    """
    _instance: Optional[logging.Logger] = None
    _initialized: bool = False
    _lock = threading.Lock()

    @classmethod
    def get_logger(cls, name: str = "mcp_databricks") -> logging.Logger:
        """
        Get or create a logger instance.

        Args:
            name: The name of the logger (default: "mcp_databricks")

        Returns:
            A configured logger instance
        """
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls._create_logger(name)
            return cls._instance

    @classmethod
    def _create_logger(cls, name: str) -> logging.Logger:
        """
        Create and configure a new logger instance.

        Args:
            name: The name of the logger

        Returns:
            A newly configured logger instance
        """
        if cls._initialized:
            return logging.getLogger(name)

        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)

        # Prevent adding handlers multiple times
        if logger.hasHandlers():
            logger.handlers.clear()

        # Create formatters
        console_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(pathname)s:%(lineno)d - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # Console handler
        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

        # File handler
        try:
            log_dir = Path("logs")
            log_dir.mkdir(exist_ok=True, parents=True)
            
            current_date = datetime.now().strftime("%Y-%m-%d")
            log_prefix = "app"
            log_file = log_dir / f"{log_prefix}_{current_date}.log"
            
            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=10 * 1024 * 1024,  # 10MB
                backupCount=5,
                encoding='utf-8'
            )
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)
        except Exception as e:
            logger.error(f"Failed to create file handler: {e}", exc_info=True)
            # Continue with console logging only

        cls._initialized = True
        return logger

    @classmethod
    def update_log_level(cls, level: str) -> None:
        """
        Update the log level for all loggers.

        Args:
            level: The new log level (e.g., "INFO", "DEBUG", etc.)
        """
        if cls._instance is not None:
            cls._instance.setLevel(level)
            for handler in cls._instance.handlers:
                handler.setLevel(level)

def create_logger(name: str = "mcp_databricks") -> logging.Logger:
    """
    Create or get a logger instance.

    This is the main function to be used by other modules to get a logger instance.
    It ensures that all loggers share the same configuration.

    Args:
        name: The name of the logger (default: "mcp_databricks")

    Returns:
        A configured logger instance
    """
    return SingletonLogger.get_logger(name)

__all__ = ['create_logger', 'SingletonLogger']
