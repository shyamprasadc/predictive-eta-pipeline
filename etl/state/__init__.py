"""
State management utilities for the Predictive ETA Calculator pipeline.

This package provides processing state tracking, backfill management,
and watermark persistence for reliable ETL operations.
"""

from .state_store import (
    StateStore,
    ProcessingState,
    BackfillJob,
    ProcessingStatus,
    get_state_store,
    create_processing_job,
    create_backfill_job,
)

__all__ = [
    "StateStore",
    "ProcessingState",
    "BackfillJob",
    "ProcessingStatus",
    "get_state_store",
    "create_processing_job",
    "create_backfill_job",
]
