"""
AQuA API - Centralized Logging Configuration
Supports dual output: Console (JSON) + Loki (optional)

This module provides a consistent logging interface across all AQuA API services,
with optional integration to Loki for centralized log aggregation.
"""

import logging
import os
import socket
from typing import Optional

from observability_library import LokiHandler, LokiLoggerLabels
from pythonjsonlogger import jsonlogger


def setup_logger(
    name: str, container_id: Optional[str] = None, enable_json: bool = True
) -> logging.Logger:
    """
    Set up logger with console (JSON format) and optional Loki handlers.

    Args:
        name: Logger name (typically __name__ from calling module)
        container_id: Optional container identifier (auto-detected if None)
        enable_json: Use JSON formatter for console output (default: True)

    Returns:
        Configured logger instance with console and optional Loki handlers

    Example:
        >>> from utils.logging_config import setup_logger
        >>> logger = setup_logger(__name__)
        >>> logger.info("Processing started", extra={"user_id": 123})
    """
    logger = logging.getLogger(name)

    # Prevent duplicate handlers if logger already configured
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    logger.propagate = False  # Prevent duplicate logs in parent loggers

    # Auto-detect container ID if not provided
    if container_id is None:
        container_id = get_container_id()

    # 1. Console Handler (stdout/stderr with JSON formatting)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    if enable_json:
        # JSON formatter - automatically includes all fields and extra data
        json_formatter = jsonlogger.JsonFormatter(
            "%(asctime)s %(name)s %(levelname)s %(message)s %(container_id)s",
            rename_fields={"levelname": "level", "asctime": "timestamp"},
        )
        console_handler.setFormatter(json_formatter)
    else:
        # Standard text formatter
        text_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - [%(container_id)s] - %(message)s"
        )
        console_handler.setFormatter(text_formatter)

    # Add default container_id to all logs
    logger.addFilter(
        lambda record: setattr(record, "container_id", container_id) or True
    )
    logger.addHandler(console_handler)

    # 2. Loki Handler (optional, controlled by feature flag)
    loki_enabled = os.getenv("LOKI_ENABLED", "false").lower() == "true"
    if loki_enabled:
        try:
            # Get configuration from environment
            loki_url = os.getenv("LOKI_URL")
            loki_auth_token = os.getenv("LOKI_AUTH_TOKEN")
            project_name = os.getenv("PROJECT_NAME", "aqua-api")
            environment_loki = os.getenv("ENVIRONMENT_LOKI", "local")

            # Define labels for log organization
            labels = LokiLoggerLabels(
                project=project_name,
                environment=environment_loki,
                container_id=container_id,
            )

            # Create and configure Loki handler
            loki_handler = LokiHandler(
                url=loki_url,
                labels=labels.to_loki_labels(),
                timeout=5,  # seconds
                auth_token=loki_auth_token,
            )
            loki_handler.setLevel(logging.INFO)

            logger.addHandler(loki_handler)
        except Exception as e:
            # Fail gracefully if Loki is unavailable
            logger.warning(f"Failed to initialize Loki handler: {e}")

    return logger


def get_container_id() -> str:
    """
    Get container ID from environment or hostname.

    Returns:
        Container identifier string

    """
    container_id = socket.gethostname()

    return container_id
