"""
ETL package for the Predictive ETA Calculator pipeline.

This package provides comprehensive ETL capabilities including:
- Common utilities (config, logging, Snowflake connections)
- H3 hexagonal grid generation and neighbor discovery
- Multi-provider routing data ingestion (OSRM, Google, HERE)
- Data transformation (routing paths, time slab aggregation)
- Snowflake data loading with staging and MERGE operations
- State management for backfills and watermarks
"""

# Re-export key components for convenience
from .common import config, logger, get_logger
from .h3 import create_city_grid_from_config, get_neighbor_discovery
from .ingest import create_distance_matrix_client, RouteResult
from .transform import create_time_slab_aggregator, aggregate_routing_data_to_slabs
from .load import BatchLoader, load_route_results_to_snowflake
from .state import get_state_store, create_processing_job

__version__ = "1.0.0"

__all__ = [
    # Configuration and logging
    "config",
    "logger",
    "get_logger",
    # H3 utilities
    "create_city_grid_from_config",
    "get_neighbor_discovery",
    # Ingestion
    "create_distance_matrix_client",
    "RouteResult",
    # Transformation
    "create_time_slab_aggregator",
    "aggregate_routing_data_to_slabs",
    # Loading
    "BatchLoader",
    "load_route_results_to_snowflake",
    # State management
    "get_state_store",
    "create_processing_job",
]
