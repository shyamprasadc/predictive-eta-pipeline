"""
Google Maps client for the Predictive ETA Calculator pipeline.

Provides interface to Google Maps Distance Matrix API for distance and duration
calculations with traffic-aware routing and departure time support.
"""

import time
import requests
from typing import Dict, List, Tuple, Optional, Any, Union
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

logger = get_logger("ingest.google")


@dataclass
class GoogleRoute:
    """Google Maps route response data."""

    distance_m: float
    duration_s: float
    duration_in_traffic_s: Optional[float] = None
    status: str = "OK"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "distance_m": self.distance_m,
            "duration_s": self.duration_s,
            "duration_in_traffic_s": self.duration_in_traffic_s,
            "status": self.status,
        }


@dataclass
class GoogleRequest:
    """Google Maps routing request parameters."""

    from_lat: float
    from_lng: float
    to_lat: float
    to_lng: float
    depart_time: Optional[datetime] = None
    traffic_model: str = "best_guess"  # best_guess, pessimistic, optimistic

    def get_origin_string(self) -> str:
        """Get origin in Google Maps format."""
        return f"{self.from_lat},{self.from_lng}"

    def get_destination_string(self) -> str:
        """Get destination in Google Maps format."""
        return f"{self.to_lat},{self.to_lng}"

    def get_request_id(self) -> str:
        """Generate deterministic request ID for idempotency."""
        content = f"google-{self.from_lat:.6f},{self.from_lng:.6f}-{self.to_lat:.6f},{self.to_lng:.6f}"
        if self.depart_time:
            content += f"-{self.depart_time.isoformat()}"
        content += f"-{self.traffic_model}"
        return hashlib.md5(content.encode()).hexdigest()


class GoogleMapsClient:
    """Client for Google Maps Distance Matrix API."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        timeout: Optional[int] = None,
        max_retries: Optional[int] = None,
        rate_limit_rps: Optional[float] = None,
    ):
        """
        Initialize Google Maps client.

        Args:
            api_key: Google Maps API key
            timeout: Request timeout in seconds
            max_retries: Maximum retry attempts
            rate_limit_rps: Rate limit in requests per second
        """
        self.api_key = api_key or config.provider.google_api_key
        if not self.api_key:
            raise ValueError("Google Maps API key is required")

        self.timeout = timeout or config.provider.timeout_seconds
        self.max_retries = max_retries or config.provider.max_retries
        self.rate_limit_rps = rate_limit_rps or config.provider.requests_per_second

        # Rate limiting
        self._last_request_time = 0.0
        self._min_request_interval = (
            1.0 / self.rate_limit_rps if self.rate_limit_rps > 0 else 0
        )

        # Base URL
        self.base_url = "https://maps.googleapis.com/maps/api/distancematrix/json"

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
    def _make_request(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Make HTTP request with retry logic.

        Args:
            params: Query parameters

        Returns:
            JSON response data
        """
        self._rate_limit()

        start_time = time.time()

        try:
            response = self.session.get(
                self.base_url, params=params, timeout=self.timeout
            )
            duration_ms = (time.time() - start_time) * 1000

            response.raise_for_status()

            response_data = response.json()

            self.logger.debug(
                "Google Maps request successful",
                extra=log_api_request(
                    provider="google",
                    endpoint=self.base_url,
                    method="GET",
                    status_code=response.status_code,
                    duration_ms=duration_ms,
                    api_status=response_data.get("status"),
                ),
            )

            # Check API-level status
            if response_data.get("status") != "OK":
                raise ValueError(
                    f"Google Maps API error: {response_data.get('status')} - {response_data.get('error_message', '')}"
                )

            return response_data

        except requests.exceptions.RequestException as e:
            duration_ms = (time.time() - start_time) * 1000

            self.logger.error(
                f"Google Maps request failed: {e}",
                extra=log_api_request(
                    provider="google",
                    endpoint=self.base_url,
                    method="GET",
                    status_code=getattr(e.response, "status_code", None)
                    if hasattr(e, "response")
                    else None,
                    duration_ms=duration_ms,
                    error=str(e),
                ),
            )
            raise

    def get_route(self, request: GoogleRequest) -> GoogleRoute:
        """
        Get route between two points.

        Args:
            request: Google Maps request parameters

        Returns:
            GoogleRoute object with distance and duration
        """
        params = {
            "key": self.api_key,
            "origins": request.get_origin_string(),
            "destinations": request.get_destination_string(),
            "mode": "driving",
            "language": "en",
            "units": "metric",
        }

        # Add departure time and traffic model if specified
        if request.depart_time:
            # Convert to Unix timestamp
            departure_timestamp = int(request.depart_time.timestamp())
            params["departure_time"] = departure_timestamp
            params["traffic_model"] = request.traffic_model

        with TimedLogger(
            self.logger,
            f"get_route {request.from_lat:.3f},{request.from_lng:.3f} -> {request.to_lat:.3f},{request.to_lng:.3f}",
        ):
            response_data = self._make_request(params)

            rows = response_data.get("rows", [])
            if not rows or not rows[0].get("elements"):
                raise ValueError("No route data in Google Maps response")

            element = rows[0]["elements"][0]
            status = element.get("status", "UNKNOWN")

            if status != "OK":
                if status == "ZERO_RESULTS":
                    raise ValueError("No route found between the specified points")
                elif status == "NOT_FOUND":
                    raise ValueError("One or more locations could not be geocoded")
                else:
                    raise ValueError(f"Google Maps routing failed: {status}")

            # Extract distance and duration
            distance_info = element.get("distance", {})
            duration_info = element.get("duration", {})
            duration_in_traffic_info = element.get("duration_in_traffic", {})

            google_route = GoogleRoute(
                distance_m=distance_info.get("value", 0.0),
                duration_s=duration_info.get("value", 0.0),
                duration_in_traffic_s=duration_in_traffic_info.get("value")
                if duration_in_traffic_info
                else None,
                status=status,
            )

            self.logger.info(
                f"Retrieved Google Maps route",
                extra={
                    "distance_m": google_route.distance_m,
                    "duration_s": google_route.duration_s,
                    "duration_in_traffic_s": google_route.duration_in_traffic_s,
                    "request_id": request.get_request_id(),
                },
            )

            return google_route

    def get_distance_matrix(
        self,
        origins: List[Tuple[float, float]],
        destinations: List[Tuple[float, float]],
        depart_time: Optional[datetime] = None,
        traffic_model: str = "best_guess",
    ) -> List[List[Optional[GoogleRoute]]]:
        """
        Get distance matrix between multiple origins and destinations.

        Args:
            origins: List of (lat, lng) tuples for origins
            destinations: List of (lat, lng) tuples for destinations
            depart_time: Departure time for traffic-aware routing
            traffic_model: Traffic model (best_guess, pessimistic, optimistic)

        Returns:
            Matrix of GoogleRoute objects (None for unreachable routes)
        """
        # Google Maps API has limits on matrix size (25 origins x 25 destinations)
        max_elements = 25
        if len(origins) > max_elements or len(destinations) > max_elements:
            self.logger.warning(
                f"Large matrix request: {len(origins)}x{len(destinations)}, consider batching"
            )

        # Format coordinates
        origins_str = "|".join([f"{lat},{lng}" for lat, lng in origins])
        destinations_str = "|".join([f"{lat},{lng}" for lat, lng in destinations])

        params = {
            "key": self.api_key,
            "origins": origins_str,
            "destinations": destinations_str,
            "mode": "driving",
            "language": "en",
            "units": "metric",
        }

        # Add departure time and traffic model if specified
        if depart_time:
            departure_timestamp = int(depart_time.timestamp())
            params["departure_time"] = departure_timestamp
            params["traffic_model"] = traffic_model

        with TimedLogger(
            self.logger, f"get_distance_matrix {len(origins)}x{len(destinations)}"
        ):
            response_data = self._make_request(params)

            rows = response_data.get("rows", [])
            if len(rows) != len(origins):
                raise ValueError(f"Expected {len(origins)} rows, got {len(rows)}")

            # Convert to GoogleRoute matrix
            matrix = []
            for i, row in enumerate(rows):
                elements = row.get("elements", [])
                if len(elements) != len(destinations):
                    raise ValueError(
                        f"Expected {len(destinations)} elements in row {i}, got {len(elements)}"
                    )

                route_row = []
                for element in elements:
                    status = element.get("status", "UNKNOWN")

                    if status == "OK":
                        distance_info = element.get("distance", {})
                        duration_info = element.get("duration", {})
                        duration_in_traffic_info = element.get(
                            "duration_in_traffic", {}
                        )

                        route = GoogleRoute(
                            distance_m=distance_info.get("value", 0.0),
                            duration_s=duration_info.get("value", 0.0),
                            duration_in_traffic_s=duration_in_traffic_info.get("value")
                            if duration_in_traffic_info
                            else None,
                            status=status,
                        )
                    else:
                        # Log the failure but include None in matrix
                        self.logger.warning(f"Route failed with status: {status}")
                        route = None

                    route_row.append(route)
                matrix.append(route_row)

            self.logger.info(
                f"Retrieved Google Maps distance matrix",
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
        Check if Google Maps service is available.

        Returns:
            True if service is healthy
        """
        try:
            # Simple test route (short distance to minimize API usage)
            test_request = GoogleRequest(
                from_lat=25.2048,
                from_lng=55.2708,  # Dubai
                to_lat=25.2049,
                to_lng=55.2709,
            )

            route = self.get_route(test_request)

            # Basic validation
            if route.distance_m > 0 and route.duration_s > 0 and route.status == "OK":
                self.logger.info("Google Maps health check passed")
                return True
            else:
                self.logger.warning(
                    f"Google Maps health check failed: invalid route data (status: {route.status})"
                )
                return False

        except Exception as e:
            self.logger.error(f"Google Maps health check failed: {e}")
            return False

    def get_service_info(self) -> Dict[str, Any]:
        """
        Get Google Maps service information.

        Returns:
            Service metadata
        """
        return {
            "provider": "google",
            "base_url": self.base_url,
            "timeout": self.timeout,
            "max_retries": self.max_retries,
            "rate_limit_rps": self.rate_limit_rps,
            "has_api_key": bool(self.api_key),
        }


# Convenience functions
def create_google_client() -> GoogleMapsClient:
    """Create Google Maps client with default configuration."""
    return GoogleMapsClient()


def get_google_route(
    from_lat: float,
    from_lng: float,
    to_lat: float,
    to_lng: float,
    depart_time: Optional[datetime] = None,
    traffic_model: str = "best_guess",
) -> GoogleRoute:
    """Get route using Google Maps with default client."""
    client = create_google_client()
    request = GoogleRequest(
        from_lat, from_lng, to_lat, to_lng, depart_time, traffic_model
    )
    return client.get_route(request)
