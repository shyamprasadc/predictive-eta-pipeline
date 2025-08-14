"""
Logging utilities for the Predictive ETA Calculator pipeline.

Provides structured logging with JSON formatting for production environments
and human-readable formatting for development.
"""

import json
import logging
import sys
from datetime import datetime
from typing import Any, Dict, Optional
from pythonjsonlogger import jsonlogger

from .config import config


class StructuredFormatter(jsonlogger.JsonFormatter):
    """Custom JSON formatter that adds common fields to all log records."""

    def add_fields(
        self,
        log_record: Dict[str, Any],
        record: logging.LogRecord,
        message_dict: Dict[str, Any],
    ) -> None:
        """Add common fields to log records."""
        super().add_fields(log_record, record, message_dict)

        # Add timestamp in ISO format
        log_record["timestamp"] = datetime.utcnow().isoformat() + "Z"

        # Add environment and service info
        log_record["environment"] = config.environment
        log_record["service"] = "predictive-eta-calculator"

        # Add level name if not present
        if "level" not in log_record:
            log_record["level"] = record.levelname


def setup_logging(
    logger_name: Optional[str] = None,
    level: Optional[str] = None,
    enable_structured: Optional[bool] = None,
) -> logging.Logger:
    """
    Set up logging with configuration from environment.

    Args:
        logger_name: Name of the logger (defaults to root)
        level: Log level override
        enable_structured: Structured logging override

    Returns:
        Configured logger instance
    """

    # Use configuration values or provided overrides
    log_level = level or config.logging.level
    structured = (
        enable_structured
        if enable_structured is not None
        else config.logging.enable_structured_logging
    )

    # Get logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(getattr(logging, log_level.upper()))

    # Remove existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Create console handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(getattr(logging, log_level.upper()))

    # Set up formatter
    if structured:
        formatter = StructuredFormatter("%(timestamp)s %(level)s %(name)s %(message)s")
    else:
        formatter = logging.Formatter(config.logging.format_str)

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Prevent double logging
    logger.propagate = False

    return logger


def log_function_call(func_name: str, **kwargs) -> Dict[str, Any]:
    """
    Create a structured log entry for function calls.

    Args:
        func_name: Name of the function being called
        **kwargs: Additional context to log

    Returns:
        Log entry dictionary
    """
    return {"event": "function_call", "function": func_name, "context": kwargs}


def log_api_request(
    provider: str,
    endpoint: str,
    method: str = "GET",
    status_code: Optional[int] = None,
    duration_ms: Optional[float] = None,
    **kwargs,
) -> Dict[str, Any]:
    """
    Create a structured log entry for API requests.

    Args:
        provider: Name of the API provider (osrm, google, here)
        endpoint: API endpoint called
        method: HTTP method
        status_code: Response status code
        duration_ms: Request duration in milliseconds
        **kwargs: Additional context

    Returns:
        Log entry dictionary
    """
    entry = {
        "event": "api_request",
        "provider": provider,
        "endpoint": endpoint,
        "method": method,
    }

    if status_code is not None:
        entry["status_code"] = status_code
    if duration_ms is not None:
        entry["duration_ms"] = duration_ms

    entry.update(kwargs)
    return entry


def log_data_processing(
    stage: str,
    records_processed: int,
    records_failed: int = 0,
    duration_ms: Optional[float] = None,
    **kwargs,
) -> Dict[str, Any]:
    """
    Create a structured log entry for data processing stages.

    Args:
        stage: Processing stage name
        records_processed: Number of records successfully processed
        records_failed: Number of records that failed
        duration_ms: Processing duration in milliseconds
        **kwargs: Additional context

    Returns:
        Log entry dictionary
    """
    entry = {
        "event": "data_processing",
        "stage": stage,
        "records_processed": records_processed,
        "records_failed": records_failed,
        "success_rate": records_processed / (records_processed + records_failed)
        if (records_processed + records_failed) > 0
        else 0,
    }

    if duration_ms is not None:
        entry["duration_ms"] = duration_ms

    entry.update(kwargs)
    return entry


def log_database_operation(
    operation: str,
    table: str,
    rows_affected: Optional[int] = None,
    duration_ms: Optional[float] = None,
    **kwargs,
) -> Dict[str, Any]:
    """
    Create a structured log entry for database operations.

    Args:
        operation: Database operation (INSERT, UPDATE, DELETE, SELECT, MERGE)
        table: Target table name
        rows_affected: Number of rows affected
        duration_ms: Operation duration in milliseconds
        **kwargs: Additional context

    Returns:
        Log entry dictionary
    """
    entry = {"event": "database_operation", "operation": operation, "table": table}

    if rows_affected is not None:
        entry["rows_affected"] = rows_affected
    if duration_ms is not None:
        entry["duration_ms"] = duration_ms

    entry.update(kwargs)
    return entry


class TimedLogger:
    """Context manager for timing operations and logging results."""

    def __init__(self, logger: logging.Logger, operation: str, **context):
        self.logger = logger
        self.operation = operation
        self.context = context
        self.start_time = None

    def __enter__(self):
        self.start_time = datetime.utcnow()
        self.logger.info(
            f"Starting {self.operation}",
            extra={
                "event": "operation_start",
                "operation": self.operation,
                **self.context,
            },
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration_ms = (datetime.utcnow() - self.start_time).total_seconds() * 1000

        if exc_type is None:
            self.logger.info(
                f"Completed {self.operation}",
                extra={
                    "event": "operation_complete",
                    "operation": self.operation,
                    "duration_ms": duration_ms,
                    "success": True,
                    **self.context,
                },
            )
        else:
            self.logger.error(
                f"Failed {self.operation}: {exc_val}",
                extra={
                    "event": "operation_failed",
                    "operation": self.operation,
                    "duration_ms": duration_ms,
                    "success": False,
                    "error_type": exc_type.__name__,
                    "error_message": str(exc_val),
                    **self.context,
                },
            )


# Global logger instance
logger = setup_logging("predictive_eta")


def get_logger(name: str) -> logging.Logger:
    """Get a logger for a specific module or component."""
    return setup_logging(f"predictive_eta.{name}")
