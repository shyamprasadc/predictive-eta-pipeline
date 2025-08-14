"""
HERE Maps client for the Predictive ETA Calculator pipeline.

Provides interface to HERE Routing API for distance and duration calculations
with traffic-aware routing and departure time support.
"""

import time
import requests
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from datetime import datetime
import hashlib

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ..common import config, get_logger, TimedLogger, log_api_request

logger = get_logger("ingest.here")


@dataclass
class HereRoute:
    """HERE Maps route response data."""

    distance_m: float
    duration_s: float
    traffic_duration_s: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "distance_m": self.distance_m,
            "duration_s": self.duration_s,
            "traffic_duration_s": self.traffic_duration_s,
        }


@dataclass
class HereRequest:
    """HERE Maps routing request parameters."""

    from_lat: float
    from_lng: float
    to_lat: float
    to_lng: float
    depart_time: Optional[datetime] = None

    def get_origin_string(self) -> str:
        """Get origin in HERE Maps format."""
        return f"{self.from_lat},{self.from_lng}"

    def get_destination_string(self) -> str:
        """Get destination in HERE Maps format."""
        return f"{self.to_lat},{self.to_lng}"

    def get_request_id(self) -> str:
        """Generate deterministic request ID for idempotency."""
        content = f"here-{self.from_lat:.6f},{self.from_lng:.6f}-{self.to_lat:.6f},{self.to_lng:.6f}"
        if self.depart_time:
            content += f"-{self.depart_time.isoformat()}"
        return hashlib.md5(content.encode()).hexdigest()


class HereMapsClient:
    """Client for HERE Maps Routing API."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        timeout: Optional[int] = None,
        max_retries: Optional[int] = None,
        rate_limit_rps: Optional[float] = None,
    ):
        """
        Initialize HERE Maps client.

        Args:
            api_key: HERE Maps API key
            timeout: Request timeout in seconds
            max_retries: Maximum retry attempts
            rate_limit_rps: Rate limit in requests per second
        """
        self.api_key = api_key or config.provider.here_api_key
        if not self.api_key:
            raise ValueError("HERE Maps API key is required")

        self.timeout = timeout or config.provider.timeout_seconds
        self.max_retries = max_retries or config.provider.max_retries
        self.rate_limit_rps = rate_limit_rps or config.provider.requests_per_second

        # Rate limiting
        self._last_request_time = 0.0
        self._min_request_interval = (
            1.0 / self.rate_limit_rps if self.rate_limit_rps > 0 else 0
        )

        # Base URLs for different services
        self.routing_url = "https://router.hereapi.com/v8/routes"
        self.matrix_url = "https://matrix.router.hereapi.com/v8/matrix"

        # Session with retry strategy
        self.session = self._create_session()

        self.logger = logger

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
    def _make_request(
        self, url: str, params: Dict[str, Any], method: str = "GET"
    ) -> Dict[str, Any]:
        """
        Make HTTP request with retry logic.

        Args:
            url: Request URL
            params: Query parameters or JSON body
            method: HTTP method

        Returns:
            JSON response data
        """
        self._rate_limit()

        start_time = time.time()

        try:
            if method.upper() == "GET":
                response = self.session.get(url, params=params, timeout=self.timeout)
            else:
                response = self.session.post(url, json=params, timeout=self.timeout)

            duration_ms = (time.time() - start_time) * 1000

            response.raise_for_status()

            response_data = response.json()

            self.logger.debug(
                "HERE Maps request successful",
                extra=log_api_request(
                    provider="here",
                    endpoint=url,
                    method=method,
                    status_code=response.status_code,
                    duration_ms=duration_ms,
                ),
            )

            return response_data

        except requests.exceptions.RequestException as e:
            duration_ms = (time.time() - start_time) * 1000

            self.logger.error(
                f"HERE Maps request failed: {e}",
                extra=log_api_request(
                    provider="here",
                    endpoint=url,
                    method=method,
                    status_code=getattr(e.response, "status_code", None)
                    if hasattr(e, "response")
                    else None,
                    duration_ms=duration_ms,
                    error=str(e),
                ),
            )
            raise

    def get_route(self, request: HereRequest) -> HereRoute:
        """
        Get route between two points.

        Args:
            request: HERE Maps request parameters

        Returns:
            HereRoute object with distance and duration
        """
        params = {
            "apikey": self.api_key,
            "origin": request.get_origin_string(),
            "destination": request.get_destination_string(),
            "transportMode": "car",
            "return": "summary",
        }

        # Add departure time if specified
        if request.depart_time:
            # HERE uses ISO 8601 format
            params["departureTime"] = request.depart_time.strftime("%Y-%m-%dT%H:%M:%S")

        with TimedLogger(
            self.logger,
            f"get_route {request.from_lat:.3f},{request.from_lng:.3f} -> {request.to_lat:.3f},{request.to_lng:.3f}",
        ):
            response_data = self._make_request(self.routing_url, params)

            routes = response_data.get("routes", [])
            if not routes:
                raise ValueError("No routes found in HERE response")

            # Use the first route
            route_data = routes[0]
            sections = route_data.get("sections", [])

            if not sections:
                raise ValueError("No route sections found in HERE response")

            # Aggregate distance and duration from all sections
            total_distance = 0
            total_duration = 0
            total_traffic_duration = 0

            for section in sections:
                summary = section.get("summary", {})
                total_distance += summary.get("length", 0)
                total_duration += summary.get("baseDuration", 0)

                # Traffic duration might be in different field
                traffic_duration = summary.get(
                    "duration", summary.get("baseDuration", 0)
                )
                total_traffic_duration += traffic_duration

            here_route = HereRoute(
                distance_m=total_distance,
                duration_s=total_duration,
                traffic_duration_s=total_traffic_duration
                if total_traffic_duration != total_duration
                else None,
            )

            self.logger.info(
                f"Retrieved HERE route",
                extra={
                    "distance_m": here_route.distance_m,
                    "duration_s": here_route.duration_s,
                    "traffic_duration_s": here_route.traffic_duration_s,
                    "request_id": request.get_request_id(),
                },
            )

            return here_route

    def get_distance_matrix(
        self,
        origins: List[Tuple[float, float]],
        destinations: List[Tuple[float, float]],
        depart_time: Optional[datetime] = None,
    ) -> List[List[Optional[HereRoute]]]:
        """
        Get distance matrix between multiple origins and destinations.

        Args:
            origins: List of (lat, lng) tuples for origins
            destinations: List of (lat, lng) tuples for destinations
            depart_time: Departure time for traffic-aware routing

        Returns:
            Matrix of HereRoute objects (None for unreachable routes)
        """
        # HERE Matrix API has limits (100x100 for basic plan)
        max_elements = 100
        if len(origins) > max_elements or len(destinations) > max_elements:
            self.logger.warning(
                f"Large matrix request: {len(origins)}x{len(destinations)}, consider batching"
            )

        # Prepare request body for POST request
        request_body = {
            "origins": [{"lat": lat, "lng": lng} for lat, lng in origins],
            "destinations": [{"lat": lat, "lng": lng} for lat, lng in destinations],
            "regionDefinition": {"type": "world"},
            "matrixAttributes": ["distances", "travelTimes"],
        }

        # Add departure time if specified
        if depart_time:
            request_body["departureTime"] = depart_time.strftime("%Y-%m-%dT%H:%M:%S")

        params = {"apikey": self.api_key}

        with TimedLogger(
            self.logger, f"get_distance_matrix {len(origins)}x{len(destinations)}"
        ):
            # Use POST for matrix requests
            response_data = self._make_request(
                f"{self.matrix_url}?{self.session.request('GET', '', params=params).url.split('?')[1]}",
                request_body,
                "POST",
            )

            matrix_data = response_data.get("matrix", {})
            num_origins = matrix_data.get("numOrigins", 0)
            num_destinations = matrix_data.get("numDestinations", 0)

            if num_origins != len(origins) or num_destinations != len(destinations):
                raise ValueError(
                    f"Matrix size mismatch: expected {len(origins)}x{len(destinations)}, got {num_origins}x{num_destinations}"
                )

            # Extract distances and travel times
            distances = matrix_data.get("distances", [])
            travel_times = matrix_data.get("travelTimes", [])

            if len(distances) != len(origins) * len(destinations):
                raise ValueError(
                    f"Expected {len(origins) * len(destinations)} distance values, got {len(distances)}"
                )

            if len(travel_times) != len(origins) * len(destinations):
                raise ValueError(
                    f"Expected {len(origins) * len(destinations)} travel time values, got {len(travel_times)}"
                )

            # Convert flat arrays to matrix format
            matrix = []
            for i in range(len(origins)):
                row = []
                for j in range(len(destinations)):
                    idx = i * len(destinations) + j

                    distance = distances[idx] if idx < len(distances) else None
                    duration = travel_times[idx] if idx < len(travel_times) else None

                    if (
                        distance is not None
                        and duration is not None
                        and distance >= 0
                        and duration >= 0
                    ):
                        route = HereRoute(distance_m=distance, duration_s=duration)
                    else:
                        route = None

                    row.append(route)
                matrix.append(row)

            self.logger.info(
                f"Retrieved HERE distance matrix",
                extra={
                    "origins_count": len(origins),
                    "destinations_count": len(destinations),
                    "total_routes": len(origins) * len(destinations),
                    "successful_routes": sum(
                        1 for row in matrix for route in row if route is not None
                    ),
                },
            )

            return matrix

    def health_check(self) -> bool:
        """
        Check if HERE Maps service is available.

        Returns:
            True if service is healthy
        """
        try:
            # Simple test route (short distance to minimize API usage)
            test_request = HereRequest(
                from_lat=25.2048,
                from_lng=55.2708,  # Dubai
                to_lat=25.2049,
                to_lng=55.2709,
            )

            route = self.get_route(test_request)

            # Basic validation
            if route.distance_m > 0 and route.duration_s > 0:
                self.logger.info("HERE Maps health check passed")
                return True
            else:
                self.logger.warning("HERE Maps health check failed: invalid route data")
                return False

        except Exception as e:
            self.logger.error(f"HERE Maps health check failed: {e}")
            return False

    def get_service_info(self) -> Dict[str, Any]:
        """
        Get HERE Maps service information.

        Returns:
            Service metadata
        """
        return {
            "provider": "here",
            "routing_url": self.routing_url,
            "matrix_url": self.matrix_url,
            "timeout": self.timeout,
            "max_retries": self.max_retries,
            "rate_limit_rps": self.rate_limit_rps,
            "has_api_key": bool(self.api_key),
        }


# Convenience functions
def create_here_client() -> HereMapsClient:
    """Create HERE Maps client with default configuration."""
    return HereMapsClient()


def get_here_route(
    from_lat: float,
    from_lng: float,
    to_lat: float,
    to_lng: float,
    depart_time: Optional[datetime] = None,
) -> HereRoute:
    """Get route using HERE Maps with default client."""
    client = create_here_client()
    request = HereRequest(from_lat, from_lng, to_lat, to_lng, depart_time)
    return client.get_route(request)
