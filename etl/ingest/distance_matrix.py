"""
Provider-agnostic distance matrix client for the Predictive ETA Calculator pipeline.

Provides a unified interface to multiple routing providers (OSRM, Google Maps, HERE)
with automatic fallback, provider selection, and batch processing capabilities.
"""

import asyncio
import time
from typing import Dict, List, Tuple, Optional, Any, Union, Protocol
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

from .osrm_client import OSRMClient, OSRMRoute, OSRMRequest
from .google_client import GoogleMapsClient, GoogleRoute, GoogleRequest
from .here_client import HereMapsClient, HereRoute, HereRequest
from ..common import config, get_logger, TimedLogger, log_data_processing

logger = get_logger("ingest.distance_matrix")


class ProviderType(Enum):
    """Supported routing providers."""

    OSRM = "osrm"
    GOOGLE = "google"
    HERE = "here"


@dataclass
class RouteResult:
    """Unified route result from any provider."""

    from_lat: float
    from_lng: float
    to_lat: float
    to_lng: float
    distance_m: float
    duration_s: float
    provider: str
    depart_time: Optional[datetime] = None
    traffic_duration_s: Optional[float] = None
    request_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "from_lat": self.from_lat,
            "from_lng": self.from_lng,
            "to_lat": self.to_lat,
            "to_lng": self.to_lng,
            "distance_m": self.distance_m,
            "duration_s": self.duration_s,
            "provider": self.provider,
            "depart_time": self.depart_time.isoformat() if self.depart_time else None,
            "traffic_duration_s": self.traffic_duration_s,
            "request_id": self.request_id,
        }


class ProviderClient(Protocol):
    """Protocol for routing provider clients."""

    def get_route(self, request: Any) -> Any:
        """Get single route."""
        ...

    def get_distance_matrix(
        self,
        origins: List[Tuple[float, float]],
        destinations: List[Tuple[float, float]],
        **kwargs,
    ) -> List[List[Optional[Any]]]:
        """Get distance matrix."""
        ...

    def health_check(self) -> bool:
        """Check if provider is healthy."""
        ...

    def get_service_info(self) -> Dict[str, Any]:
        """Get provider service information."""
        ...


class DistanceMatrixClient:
    """Provider-agnostic distance matrix client."""

    def __init__(
        self,
        primary_provider: Optional[str] = None,
        fallback_providers: Optional[List[str]] = None,
        max_concurrent_requests: Optional[int] = None,
        batch_size: Optional[int] = None,
    ):
        """
        Initialize distance matrix client.

        Args:
            primary_provider: Primary provider to use
            fallback_providers: List of fallback providers
            max_concurrent_requests: Maximum concurrent requests
            batch_size: Batch size for processing
        """
        self.primary_provider = primary_provider or config.provider.provider
        self.fallback_providers = fallback_providers or []
        self.max_concurrent_requests = (
            max_concurrent_requests or config.eta.max_concurrent_requests
        )
        self.batch_size = batch_size or config.eta.batch_size

        # Initialize provider clients
        self.clients: Dict[str, ProviderClient] = {}
        self._initialize_clients()

        # Provider precedence for fallback
        self.provider_precedence = [self.primary_provider] + self.fallback_providers

        self.logger = logger

    def _initialize_clients(self):
        """Initialize all available provider clients."""
        # OSRM (always available)
        try:
            self.clients[ProviderType.OSRM.value] = OSRMClient()
            self.logger.info("Initialized OSRM client")
        except Exception as e:
            self.logger.warning(f"Failed to initialize OSRM client: {e}")

        # Google Maps (requires API key)
        if config.provider.google_api_key:
            try:
                self.clients[ProviderType.GOOGLE.value] = GoogleMapsClient()
                self.logger.info("Initialized Google Maps client")
            except Exception as e:
                self.logger.warning(f"Failed to initialize Google Maps client: {e}")

        # HERE Maps (requires API key)
        if config.provider.here_api_key:
            try:
                self.clients[ProviderType.HERE.value] = HereMapsClient()
                self.logger.info("Initialized HERE Maps client")
            except Exception as e:
                self.logger.warning(f"Failed to initialize HERE Maps client: {e}")

        if not self.clients:
            raise ValueError("No routing providers could be initialized")

    def _convert_provider_route(
        self,
        route: Union[OSRMRoute, GoogleRoute, HereRoute],
        provider: str,
        from_lat: float,
        from_lng: float,
        to_lat: float,
        to_lng: float,
        depart_time: Optional[datetime] = None,
        request_id: Optional[str] = None,
    ) -> RouteResult:
        """Convert provider-specific route to unified RouteResult."""

        traffic_duration = None

        if isinstance(route, OSRMRoute):
            distance = route.distance_m
            duration = route.duration_s
        elif isinstance(route, GoogleRoute):
            distance = route.distance_m
            duration = route.duration_s
            traffic_duration = route.duration_in_traffic_s
        elif isinstance(route, HereRoute):
            distance = route.distance_m
            duration = route.duration_s
            traffic_duration = route.traffic_duration_s
        else:
            raise ValueError(f"Unknown route type: {type(route)}")

        return RouteResult(
            from_lat=from_lat,
            from_lng=from_lng,
            to_lat=to_lat,
            to_lng=to_lng,
            distance_m=distance,
            duration_s=duration,
            provider=provider,
            depart_time=depart_time,
            traffic_duration_s=traffic_duration,
            request_id=request_id,
        )

    def get_route(
        self,
        from_lat: float,
        from_lng: float,
        to_lat: float,
        to_lng: float,
        depart_time: Optional[datetime] = None,
        preferred_provider: Optional[str] = None,
    ) -> RouteResult:
        """
        Get route between two points with fallback.

        Args:
            from_lat: Origin latitude
            from_lng: Origin longitude
            to_lat: Destination latitude
            to_lng: Destination longitude
            depart_time: Departure time
            preferred_provider: Preferred provider override

        Returns:
            RouteResult with unified route data
        """
        providers_to_try = (
            [preferred_provider] if preferred_provider else self.provider_precedence
        )

        for provider in providers_to_try:
            if provider not in self.clients:
                self.logger.warning(f"Provider {provider} not available, skipping")
                continue

            try:
                client = self.clients[provider]

                # Create provider-specific request
                if provider == ProviderType.OSRM.value:
                    request = OSRMRequest(
                        from_lat, from_lng, to_lat, to_lng, depart_time
                    )
                    route = client.get_route(request)
                    request_id = request.get_request_id()
                elif provider == ProviderType.GOOGLE.value:
                    request = GoogleRequest(
                        from_lat, from_lng, to_lat, to_lng, depart_time
                    )
                    route = client.get_route(request)
                    request_id = request.get_request_id()
                elif provider == ProviderType.HERE.value:
                    request = HereRequest(
                        from_lat, from_lng, to_lat, to_lng, depart_time
                    )
                    route = client.get_route(request)
                    request_id = request.get_request_id()
                else:
                    raise ValueError(f"Unknown provider: {provider}")

                # Convert to unified format
                return self._convert_provider_route(
                    route,
                    provider,
                    from_lat,
                    from_lng,
                    to_lat,
                    to_lng,
                    depart_time,
                    request_id,
                )

            except Exception as e:
                self.logger.warning(f"Provider {provider} failed: {e}")
                if provider == providers_to_try[-1]:  # Last provider
                    raise
                continue

        raise ValueError("All providers failed")

    def get_distance_matrix_batch(
        self,
        origin_destination_pairs: List[Tuple[Tuple[float, float], Tuple[float, float]]],
        depart_time: Optional[datetime] = None,
        preferred_provider: Optional[str] = None,
    ) -> List[RouteResult]:
        """
        Get routes for multiple origin-destination pairs in batches.

        Args:
            origin_destination_pairs: List of ((from_lat, from_lng), (to_lat, to_lng)) tuples
            depart_time: Departure time for all routes
            preferred_provider: Preferred provider override

        Returns:
            List of RouteResult objects
        """
        with TimedLogger(
            self.logger,
            f"get_distance_matrix_batch: {len(origin_destination_pairs)} pairs",
        ):
            results = []
            failed_count = 0

            # Process in batches
            for i in range(0, len(origin_destination_pairs), self.batch_size):
                batch = origin_destination_pairs[i : i + self.batch_size]

                # Use ThreadPoolExecutor for concurrent requests
                with ThreadPoolExecutor(
                    max_workers=self.max_concurrent_requests
                ) as executor:
                    future_to_pair = {
                        executor.submit(
                            self.get_route,
                            from_lat,
                            from_lng,
                            to_lat,
                            to_lng,
                            depart_time,
                            preferred_provider,
                        ): ((from_lat, from_lng), (to_lat, to_lng))
                        for (from_lat, from_lng), (to_lat, to_lng) in batch
                    }

                    for future in as_completed(future_to_pair):
                        try:
                            result = future.result()
                            results.append(result)
                        except Exception as e:
                            failed_count += 1
                            pair = future_to_pair[future]
                            self.logger.error(f"Failed to get route for {pair}: {e}")

                # Small delay between batches to be respectful
                if i + self.batch_size < len(origin_destination_pairs):
                    time.sleep(0.1)

            self.logger.info(
                "Batch processing complete",
                extra=log_data_processing(
                    stage="distance_matrix_batch",
                    records_processed=len(results),
                    records_failed=failed_count,
                ),
            )

            return results

    def get_hex_pair_routes(
        self,
        hex_pairs: List[Tuple[str, str]],
        hex_to_latlng: Dict[str, Tuple[float, float]],
        depart_time: Optional[datetime] = None,
        preferred_provider: Optional[str] = None,
    ) -> List[RouteResult]:
        """
        Get routes for H3 hex pairs.

        Args:
            hex_pairs: List of (from_hex, to_hex) tuples
            hex_to_latlng: Mapping from hex ID to (lat, lng)
            depart_time: Departure time for all routes
            preferred_provider: Preferred provider override

        Returns:
            List of RouteResult objects
        """
        # Convert hex pairs to coordinate pairs
        coordinate_pairs = []
        for from_hex, to_hex in hex_pairs:
            if from_hex not in hex_to_latlng or to_hex not in hex_to_latlng:
                self.logger.warning(
                    f"Missing coordinates for hex pair: {from_hex} -> {to_hex}"
                )
                continue

            from_coords = hex_to_latlng[from_hex]
            to_coords = hex_to_latlng[to_hex]
            coordinate_pairs.append((from_coords, to_coords))

        return self.get_distance_matrix_batch(
            coordinate_pairs, depart_time, preferred_provider
        )

    def to_dataframe(self, results: List[RouteResult]) -> pd.DataFrame:
        """Convert RouteResult list to pandas DataFrame."""
        if not results:
            return pd.DataFrame()

        data = [result.to_dict() for result in results]
        df = pd.DataFrame(data)

        # Convert datetime strings back to datetime objects
        if "depart_time" in df.columns:
            df["depart_time"] = pd.to_datetime(df["depart_time"])

        return df

    def health_check_all_providers(self) -> Dict[str, bool]:
        """Check health of all available providers."""
        health_status = {}

        for provider, client in self.clients.items():
            try:
                health_status[provider] = client.health_check()
            except Exception as e:
                self.logger.error(f"Health check failed for {provider}: {e}")
                health_status[provider] = False

        return health_status

    def get_provider_info(self) -> Dict[str, Dict[str, Any]]:
        """Get information about all providers."""
        provider_info = {}

        for provider, client in self.clients.items():
            try:
                provider_info[provider] = client.get_service_info()
            except Exception as e:
                self.logger.error(f"Failed to get info for {provider}: {e}")
                provider_info[provider] = {"error": str(e)}

        return provider_info


# Convenience functions
def create_distance_matrix_client() -> DistanceMatrixClient:
    """Create distance matrix client with default configuration."""
    return DistanceMatrixClient()


def get_route_between_points(
    from_lat: float,
    from_lng: float,
    to_lat: float,
    to_lng: float,
    depart_time: Optional[datetime] = None,
    provider: Optional[str] = None,
) -> RouteResult:
    """Get route between two points using default client."""
    client = create_distance_matrix_client()
    return client.get_route(from_lat, from_lng, to_lat, to_lng, depart_time, provider)


def get_routes_for_hex_pairs(
    hex_pairs: List[Tuple[str, str]],
    hex_to_latlng: Dict[str, Tuple[float, float]],
    depart_time: Optional[datetime] = None,
    provider: Optional[str] = None,
) -> List[RouteResult]:
    """Get routes for H3 hex pairs using default client."""
    client = create_distance_matrix_client()
    return client.get_hex_pair_routes(hex_pairs, hex_to_latlng, depart_time, provider)
