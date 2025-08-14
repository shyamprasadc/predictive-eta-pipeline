"""
Data transformation utilities for the Predictive ETA Calculator pipeline.

This package provides routing path mapping and time slab aggregation functionality
for transforming raw routing data into analytical datasets.
"""

from .routing_paths import (
    RoutingPathMapper,
    RoutingPath,
    PathSegment,
    create_routing_path_mapper,
    create_direct_routing_path,
    create_path_with_interpolation,
)

from .slab_agg import (
    TimeSlabAggregator,
    TimeSlabDefinition,
    ETAAggregation,
    WeekdayEnum,
    create_time_slab_aggregator,
    aggregate_routing_data_to_slabs,
)

__all__ = [
    # Routing paths
    "RoutingPathMapper",
    "RoutingPath",
    "PathSegment",
    "create_routing_path_mapper",
    "create_direct_routing_path",
    "create_path_with_interpolation",
    # Slab aggregation
    "TimeSlabAggregator",
    "TimeSlabDefinition",
    "ETAAggregation",
    "WeekdayEnum",
    "create_time_slab_aggregator",
    "aggregate_routing_data_to_slabs",
]
