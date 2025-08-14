"""
Data ingestion utilities for the Predictive ETA Calculator pipeline.

This package provides routing provider clients and distance matrix calculation
capabilities for OSRM, Google Maps, and HERE Maps APIs.
"""

from .osrm_client import (
    OSRMClient,
    OSRMRoute,
    OSRMRequest,
    create_osrm_client,
    get_osrm_route,
)
from .google_client import (
    GoogleMapsClient,
    GoogleRoute,
    GoogleRequest,
    create_google_client,
    get_google_route,
)
from .here_client import (
    HereMapsClient,
    HereRoute,
    HereRequest,
    create_here_client,
    get_here_route,
)
from .distance_matrix import (
    DistanceMatrixClient,
    RouteResult,
    ProviderType,
    create_distance_matrix_client,
    get_route_between_points,
    get_routes_for_hex_pairs,
)

__all__ = [
    # OSRM
    "OSRMClient",
    "OSRMRoute",
    "OSRMRequest",
    "create_osrm_client",
    "get_osrm_route",
    # Google Maps
    "GoogleMapsClient",
    "GoogleRoute",
    "GoogleRequest",
    "create_google_client",
    "get_google_route",
    # HERE Maps
    "HereMapsClient",
    "HereRoute",
    "HereRequest",
    "create_here_client",
    "get_here_route",
    # Unified interface
    "DistanceMatrixClient",
    "RouteResult",
    "ProviderType",
    "create_distance_matrix_client",
    "get_route_between_points",
    "get_routes_for_hex_pairs",
]
