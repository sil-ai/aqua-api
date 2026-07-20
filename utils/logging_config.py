"""
AQuA API - Centralized Logging Configuration
Supports dual output: Console (JSON) + Loki (optional)

This module provides a consistent logging interface across all AQuA API services,
with optional integration to Loki for centralized log aggregation.
"""

import logging
import socket
from typing import Optional

from pythonjsonlogger import jsonlogger

from config import settings

# The Loki integration lives in the private observability-library, which
# external contributors can't install. Import it optionally so this module —
# and therefore the whole app, which imports setup_logger everywhere — loads
# cleanly without it. Loki logging is off by default (settings.loki_enabled),
# so the missing dependency only matters when Loki is explicitly turned on.
#
# Catch ImportError broadly (not just ModuleNotFoundError): observability is a
# best-effort feature that must never break app import — the same stance the
# `except Exception` around handler creation below takes. To avoid *silently*
# disabling Loki when the library is present but broken (e.g. a missing
# transitive dep), we keep the failure reason and surface it in the warning
# emitted when Loki is enabled.
_OBSERVABILITY_IMPORT_ERROR: Optional[str] = None
try:
    from observability_library import LokiHandler, LokiLoggerLabels

    OBSERVABILITY_AVAILABLE = True
except ImportError as exc:
    LokiHandler = None
    LokiLoggerLabels = None
    OBSERVABILITY_AVAILABLE = False
    _OBSERVABILITY_IMPORT_ERROR = str(exc)

# Guard so the "Loki enabled but library unavailable" warning is emitted once
# per process rather than once per logger name (setup_logger runs for ~15
# modules at startup).
_loki_unavailable_warned = False


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
    if settings.loki_enabled:
        if not OBSERVABILITY_AVAILABLE:
            # Loki was requested but the optional library is unavailable. Warn
            # (once per process) and carry on with console logging rather than
            # crashing. The captured reason distinguishes "not installed" from
            # a broken install so the latter isn't silently swallowed.
            global _loki_unavailable_warned
            if not _loki_unavailable_warned:
                logger.warning(
                    "observability-library unavailable; Loki logging disabled "
                    "(%s). Install the optional dependency to enable it "
                    "(pip install -r requirements-observability.txt).",
                    _OBSERVABILITY_IMPORT_ERROR,
                )
                _loki_unavailable_warned = True
            return logger
        try:
            # Define labels for log organization
            labels = LokiLoggerLabels(
                project=settings.project_name,
                environment=settings.environment_loki,
                container_id=container_id,
            )

            # Create and configure Loki handler
            loki_handler = LokiHandler(
                url=settings.loki_url,
                labels=labels.to_loki_labels(),
                timeout=5,  # seconds
                auth_token=settings.loki_auth_token,
            )
            loki_handler.setLevel(logging.INFO)

            logger.addHandler(loki_handler)
        except Exception as e:
            # Fail gracefully if Loki is unavailable. Log the exception *type*
            # rather than its str(): the message originates in the private
            # observability-library and could embed the Loki auth token or URL
            # (e.g. an auth/URL-validation error that echoes its input), which
            # would then land in plaintext in console logs. The type name is
            # enough to diagnose without risking credential exposure.
            logger.warning("Failed to initialize Loki handler: %s", type(e).__name__)

    return logger


def get_container_id() -> str:
    """
    Get container ID from environment or hostname.

    Returns:
        Container identifier string

    """
    container_id = socket.gethostname()

    return container_id
