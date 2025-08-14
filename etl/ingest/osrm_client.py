"""
OSRM (Open Source Routing Machine) client for the Predictive ETA Calculator pipeline.

Provides interface to OSRM routing services for distance and duration calculations
between geographic coordinates with retry logic and rate limiting.
"""

import time
import requests
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from datetime import datetime
import hashlib
import json

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ..common import config, get_logger, TimedLogger, log_api_request

logger = get_logger("ingest.osrm")


@dataclass
class OSRMRoute:
    """OSRM route response data."""

    distance_m: float
    duration_s: float
    geometry: Optional[str] = None  # Polyline geometry
    legs: Optional[List[Dict]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "distance_m": self.distance_m,
            "duration_s": self.duration_s,
            "geometry": self.geometry,
            "legs": self.legs,
        }


@dataclass
class OSRMRequest:
    """OSRM routing request parameters."""

    from_lat: float
    from_lng: float
    to_lat: float
    to_lng: float
    depart_time: Optional[datetime] = None

    def get_coordinates_string(self) -> str:
        """Get coordinates in OSRM format: lng,lat;lng,lat"""
        return f"{self.from_lng},{self.from_lat};{self.to_lng},{self.to_lat}"

    def get_request_id(self) -> str:
        """Generate deterministic request ID for idempotency."""
        content = f"{self.from_lat:.6f},{self.from_lng:.6f}-{self.to_lat:.6f},{self.to_lng:.6f}"
        if self.depart_time:
            content += f"-{self.depart_time.isoformat()}"
        return hashlib.md5(content.encode()).hexdigest()


class OSRMClient:
    """Client for OSRM routing API."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        timeout: Optional[int] = None,
        max_retries: Optional[int] = None,
        rate_limit_rps: Optional[float] = None,
    ):
        """
        Initialize OSRM client.

        Args:
            base_url: OSRM server base URL
            timeout: Request timeout in seconds
            max_retries: Maximum retry attempts
            rate_limit_rps: Rate limit in requests per second
        """
        self.base_url = base_url or config.provider.osrm_base_url
        self.timeout = timeout or config.provider.timeout_seconds
        self.max_retries = max_retries or config.provider.max_retries
        self.rate_limit_rps = rate_limit_rps or config.provider.requests_per_second

        # Rate limiting
        self._last_request_time = 0.0
        self._min_request_interval = (
            1.0 / self.rate_limit_rps if self.rate_limit_rps > 0 else 0
        )

        # Session with retry strategy
        self.session = self._create_session()

        self.logger = logger

        # Validate base URL
        if not self.base_url.startswith(("http://", "https://")):
            raise ValueError(f"Invalid OSRM base URL: {self.base_url}")

    def _create_session(self) -> requests.Session:
        """Create requests session with retry strategy."""
        session = requests.Session()

        retry_strategy = Retry(
            total=self.max_retries,
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=["GET", "POST"],
            backoff_factor=config.provider.backoff_factor,
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def _rate_limit(self):
        """Apply rate limiting between requests."""
        if self._min_request_interval > 0:
            current_time = time.time()
            time_since_last = current_time - self._last_request_time

            if time_since_last < self._min_request_interval:
                sleep_time = self._min_request_interval - time_since_last
                time.sleep(sleep_time)

            self._last_request_time = time.time()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((requests.RequestException, requests.Timeout)),
    )
    def _make_request(self, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Make HTTP request with retry logic.

        Args:
            url: Request URL
            params: Query parameters

        Returns:
            JSON response data
        """
        self._rate_limit()

        start_time = time.time()

        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            duration_ms = (time.time() - start_time) * 1000

            response.raise_for_status()

            self.logger.debug(
                "OSRM request successful",
                extra=log_api_request(
                    provider="osrm",
                    endpoint=url,
                    method="GET",
                    status_code=response.status_code,
                    duration_ms=duration_ms,
                ),
            )

            return response.json()

        except requests.exceptions.RequestException as e:
            duration_ms = (time.time() - start_time) * 1000

            self.logger.error(
                f"OSRM request failed: {e}",
                extra=log_api_request(
                    provider="osrm",
                    endpoint=url,
                    method="GET",
                    status_code=getattr(e.response, "status_code", None)
                    if hasattr(e, "response")
                    else None,
                    duration_ms=duration_ms,
                    error=str(e),
                ),
            )
            raise

    def get_route(
        self, request: OSRMRequest, include_geometry: bool = False
    ) -> OSRMRoute:
        """
        Get route between two points.

        Args:
            request: OSRM request parameters
            include_geometry: Include route geometry in response

        Returns:
            OSRMRoute object with distance and duration
        """
        url = f"{self.base_url}/route/v1/driving/{request.get_coordinates_string()}"

        params = {
            "overview": "full" if include_geometry else "false",
            "geometries": "polyline",
            "annotations": "false",
        }

        # OSRM doesn't natively support departure time, but we log it for context
        if request.depart_time:
            self.logger.debug(
                f"OSRM request with departure time context: {request.depart_time}"
            )

        with TimedLogger(
            self.logger,
            f"get_route {request.from_lat:.3f},{request.from_lng:.3f} -> {request.to_lat:.3f},{request.to_lng:.3f}",
        ):
            response_data = self._make_request(url, params)

            if response_data.get("code") != "Ok":
                error_msg = response_data.get("message", "Unknown OSRM error")
                raise ValueError(f"OSRM routing failed: {error_msg}")

            routes = response_data.get("routes", [])
            if not routes:
                raise ValueError("No routes found in OSRM response")

            # Use the first (best) route
            route_data = routes[0]

            osrm_route = OSRMRoute(
                distance_m=route_data.get("distance", 0.0),
                duration_s=route_data.get("duration", 0.0),
                geometry=route_data.get("geometry") if include_geometry else None,
                legs=route_data.get("legs", []) if include_geometry else None,
            )

            self.logger.info(
                f"Retrieved OSRM route",
                extra={
                    "distance_m": osrm_route.distance_m,
                    "duration_s": osrm_route.duration_s,
                    "request_id": request.get_request_id(),
                },
            )

            return osrm_route

    def get_distance_matrix(
        self,
        origins: List[Tuple[float, float]],
        destinations: List[Tuple[float, float]],
    ) -> List[List[Optional[OSRMRoute]]]:
        """
        Get distance matrix between multiple origins and destinations.

        Args:
            origins: List of (lat, lng) tuples for origins
            destinations: List of (lat, lng) tuples for destinations

        Returns:
            Matrix of OSRMRoute objects (None for unreachable routes)
        """
        # OSRM table service requires all coordinates in one list
        all_coords = origins + destinations
        coordinates_str = ";".join([f"{lng},{lat}" for lat, lng in all_coords])

        url = f"{self.base_url}/table/v1/driving/{coordinates_str}"

        # Sources are indices 0 to len(origins)-1
        # Destinations are indices len(origins) to len(all_coords)-1
        sources = list(range(len(origins)))
        destinations_indices = list(range(len(origins), len(all_coords)))

        params = {
            "sources": ";".join(map(str, sources)),
            "destinations": ";".join(map(str, destinations_indices)),
            "annotations": "duration,distance",
        }

        with TimedLogger(
            self.logger, f"get_distance_matrix {len(origins)}x{len(destinations)}"
        ):
            response_data = self._make_request(url, params)

            if response_data.get("code") != "Ok":
                error_msg = response_data.get("message", "Unknown OSRM error")
                raise ValueError(f"OSRM distance matrix failed: {error_msg}")

            durations = response_data.get("durations", [])
            distances = response_data.get("distances", [])

            if not durations or not distances:
                raise ValueError("No distance/duration data in OSRM response")

            # Convert to OSRMRoute matrix
            matrix = []
            for i in range(len(origins)):
                row = []
                for j in range(len(destinations)):
                    duration = (
                        durations[i][j]
                        if i < len(durations) and j < len(durations[i])
                        else None
                    )
                    distance = (
                        distances[i][j]
                        if i < len(distances) and j < len(distances[i])
                        else None
                    )

                    if duration is not None and distance is not None:
                        route = OSRMRoute(distance_m=distance, duration_s=duration)
                    else:
                        route = None

                    row.append(route)
                matrix.append(row)

            self.logger.info(
                f"Retrieved OSRM distance matrix",
                extra={
                    "origins_count": len(origins),
                    "destinations_count": len(destinations),
                    "total_routes": len(origins) * len(destinations),
                },
            )

            return matrix

    def health_check(self) -> bool:
        """
        Check if OSRM service is available.

        Returns:
            True if service is healthy
        """
        try:
            # Simple test route (short distance to minimize load)
            test_request = OSRMRequest(
                from_lat=25.2048,
                from_lng=55.2708,  # Dubai
                to_lat=25.2049,
                to_lng=55.2709,
            )

            route = self.get_route(test_request)

            # Basic validation
            if route.distance_m > 0 and route.duration_s > 0:
                self.logger.info("OSRM health check passed")
                return True
            else:
                self.logger.warning("OSRM health check failed: invalid route data")
                return False

        except Exception as e:
            self.logger.error(f"OSRM health check failed: {e}")
            return False

    def get_service_info(self) -> Dict[str, Any]:
        """
        Get OSRM service information.

        Returns:
            Service metadata
        """
        return {
            "provider": "osrm",
            "base_url": self.base_url,
            "timeout": self.timeout,
            "max_retries": self.max_retries,
            "rate_limit_rps": self.rate_limit_rps,
        }


# Convenience functions
def create_osrm_client() -> OSRMClient:
    """Create OSRM client with default configuration."""
    return OSRMClient()


def get_osrm_route(
    from_lat: float,
    from_lng: float,
    to_lat: float,
    to_lng: float,
    depart_time: Optional[datetime] = None,
    include_geometry: bool = False,
) -> OSRMRoute:
    """Get route using OSRM with default client."""
    client = create_osrm_client()
    request = OSRMRequest(from_lat, from_lng, to_lat, to_lng, depart_time)
    return client.get_route(request, include_geometry)
