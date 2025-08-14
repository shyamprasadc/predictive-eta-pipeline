"""
H3 hexagonal grid utilities for the Predictive ETA Calculator pipeline.

This package provides H3 grid generation and neighbor discovery functionality
for spatial analysis and routing operations.
"""

from .grid import (
    H3GridGenerator,
    CityBounds,
    create_dubai_grid,
    create_city_grid_from_config,
    get_grid_generator,
    latlng_to_hex,
)

from .neighbors import (
    H3NeighborDiscovery,
    NeighborConfig,
    get_neighbor_discovery,
    get_hex_neighbors,
)

__all__ = [
    "H3GridGenerator",
    "CityBounds",
    "create_dubai_grid",
    "create_city_grid_from_config",
    "get_grid_generator",
    "latlng_to_hex",
    "H3NeighborDiscovery",
    "NeighborConfig",
    "get_neighbor_discovery",
    "get_hex_neighbors",
]
