"""
H3 hexagonal grid generation for the Predictive ETA Calculator pipeline.

Provides utilities to generate city grids at H3 resolution 7 and map
geographic coordinates to hexagonal cells.
"""

import h3
import pandas as pd
from typing import List, Tuple, Set, Dict, Optional
from shapely.geometry import Polygon, Point
from dataclasses import dataclass

from ..common import config, get_logger, TimedLogger

logger = get_logger("h3.grid")


@dataclass
class CityBounds:
    """City boundary definition."""

    min_lat: float
    min_lng: float
    max_lat: float
    max_lng: float

    @classmethod
    def from_bbox(cls, bbox: List[float]) -> "CityBounds":
        """Create CityBounds from bounding box list."""
        if len(bbox) != 4:
            raise ValueError(
                "Bounding box must have 4 values: [min_lat, min_lng, max_lat, max_lng]"
            )
        return cls(min_lat=bbox[0], min_lng=bbox[1], max_lat=bbox[2], max_lng=bbox[3])

    def to_polygon(self) -> Polygon:
        """Convert bounds to Shapely polygon."""
        return Polygon(
            [
                (self.min_lng, self.min_lat),
                (self.max_lng, self.min_lat),
                (self.max_lng, self.max_lat),
                (self.min_lng, self.max_lat),
                (self.min_lng, self.min_lat),
            ]
        )

    def center(self) -> Tuple[float, float]:
        """Get center point of the bounds."""
        center_lat = (self.min_lat + self.max_lat) / 2
        center_lng = (self.min_lng + self.max_lng) / 2
        return center_lat, center_lng


class H3GridGenerator:
    """Generates H3 hexagonal grids for cities."""

    def __init__(self, resolution: int = 7):
        """
        Initialize H3 grid generator.

        Args:
            resolution: H3 resolution level (default: 7)
        """
        self.resolution = resolution
        self.logger = logger

        # Validate resolution
        if not (0 <= resolution <= 15):
            raise ValueError(
                f"H3 resolution must be between 0 and 15, got {resolution}"
            )

    def generate_city_grid(
        self, city_bounds: CityBounds, city_name: str, include_border_hexes: bool = True
    ) -> pd.DataFrame:
        """
        Generate H3 grid for a city within specified bounds.

        Args:
            city_bounds: City boundary definition
            city_name: Name of the city
            include_border_hexes: Include hexes that partially overlap bounds

        Returns:
            DataFrame with columns: hex_id, city, resolution, centroid_lat, centroid_lng
        """
        with TimedLogger(self.logger, f"generate_city_grid for {city_name}"):
            # Get H3 hexes that cover the bounding box
            hexes = self._get_hexes_in_bounds(city_bounds, include_border_hexes)

            # Create DataFrame with hex information
            grid_data = []
            for hex_id in hexes:
                centroid_lat, centroid_lng = h3.h3_to_geo(hex_id)
                grid_data.append(
                    {
                        "hex_id": hex_id,
                        "city": city_name,
                        "resolution": self.resolution,
                        "centroid_lat": centroid_lat,
                        "centroid_lng": centroid_lng,
                    }
                )

            df = pd.DataFrame(grid_data)

            self.logger.info(
                f"Generated grid for {city_name}",
                extra={
                    "city": city_name,
                    "resolution": self.resolution,
                    "hex_count": len(df),
                    "bounds": {
                        "min_lat": city_bounds.min_lat,
                        "min_lng": city_bounds.min_lng,
                        "max_lat": city_bounds.max_lat,
                        "max_lng": city_bounds.max_lng,
                    },
                },
            )

            return df

    def _get_hexes_in_bounds(
        self, city_bounds: CityBounds, include_border_hexes: bool
    ) -> Set[str]:
        """
        Get H3 hexes that intersect with city bounds.

        Args:
            city_bounds: City boundary definition
            include_border_hexes: Include hexes that partially overlap bounds

        Returns:
            Set of H3 hex IDs
        """
        # Create polygon from bounds
        bounds_polygon = city_bounds.to_polygon()

        # Get hex at center point to start search
        center_lat, center_lng = city_bounds.center()
        center_hex = h3.geo_to_h3(center_lat, center_lng, self.resolution)

        # Use polyfill to get hexes covering the polygon
        # Convert polygon to H3 compatible format (list of lat/lng tuples)
        exterior_coords = list(bounds_polygon.exterior.coords)
        # H3 expects [lat, lng] format
        h3_polygon = [[coord[1], coord[0]] for coord in exterior_coords]

        # Get hexes that cover the polygon
        hexes = h3.polyfill(h3_polygon, self.resolution)

        if not include_border_hexes:
            # Filter out hexes that don't have their centroid within bounds
            filtered_hexes = set()
            for hex_id in hexes:
                centroid_lat, centroid_lng = h3.h3_to_geo(hex_id)
                centroid_point = Point(centroid_lng, centroid_lat)
                if bounds_polygon.contains(centroid_point):
                    filtered_hexes.add(hex_id)
            hexes = filtered_hexes

        return hexes

    def latlng_to_hex(self, lat: float, lng: float) -> str:
        """
        Convert latitude/longitude to H3 hex ID.

        Args:
            lat: Latitude
            lng: Longitude

        Returns:
            H3 hex ID string
        """
        return h3.geo_to_h3(lat, lng, self.resolution)

    def hex_to_latlng(self, hex_id: str) -> Tuple[float, float]:
        """
        Convert H3 hex ID to latitude/longitude centroid.

        Args:
            hex_id: H3 hex ID string

        Returns:
            Tuple of (latitude, longitude)
        """
        return h3.h3_to_geo(hex_id)

    def hex_to_boundary(self, hex_id: str) -> List[Tuple[float, float]]:
        """
        Get boundary coordinates for an H3 hex.

        Args:
            hex_id: H3 hex ID string

        Returns:
            List of (latitude, longitude) tuples defining the hex boundary
        """
        return h3.h3_to_geo_boundary(hex_id)

    def get_hex_area_km2(self, hex_id: str) -> float:
        """
        Get the area of an H3 hex in square kilometers.

        Args:
            hex_id: H3 hex ID string

        Returns:
            Area in square kilometers
        """
        return h3.hex_area(self.resolution, "km^2")

    def get_hex_edge_length_km(self, hex_id: str) -> float:
        """
        Get the edge length of an H3 hex in kilometers.

        Args:
            hex_id: H3 hex ID string

        Returns:
            Edge length in kilometers
        """
        return h3.edge_length(self.resolution, "km")

    def validate_hex_id(self, hex_id: str) -> bool:
        """
        Validate that a string is a valid H3 hex ID.

        Args:
            hex_id: String to validate

        Returns:
            True if valid H3 hex ID
        """
        try:
            return (
                h3.h3_is_valid(hex_id)
                and h3.h3_get_resolution(hex_id) == self.resolution
            )
        except:
            return False

    def get_distance_between_hexes(self, hex1: str, hex2: str) -> float:
        """
        Get approximate distance between two H3 hexes in kilometers.

        Args:
            hex1: First H3 hex ID
            hex2: Second H3 hex ID

        Returns:
            Distance in kilometers
        """
        lat1, lng1 = self.hex_to_latlng(hex1)
        lat2, lng2 = self.hex_to_latlng(hex2)

        # Use H3's point distance function for accuracy
        return h3.point_dist((lat1, lng1), (lat2, lng2), "km")


def create_dubai_grid() -> pd.DataFrame:
    """
    Create H3 grid for Dubai as an example.

    Returns:
        DataFrame with Dubai H3 grid
    """
    # Dubai approximate bounds
    dubai_bounds = CityBounds(min_lat=24.9, min_lng=54.8, max_lat=25.4, max_lng=55.6)

    generator = H3GridGenerator(resolution=config.h3.resolution)
    return generator.generate_city_grid(dubai_bounds, "Dubai")


def create_city_grid_from_config() -> pd.DataFrame:
    """
    Create H3 grid using configuration values.

    Returns:
        DataFrame with city H3 grid from config
    """
    city_bounds = CityBounds.from_bbox(config.h3.city_bbox)
    generator = H3GridGenerator(resolution=config.h3.resolution)
    return generator.generate_city_grid(city_bounds, config.h3.city_name)


# Convenience functions
def get_grid_generator(resolution: Optional[int] = None) -> H3GridGenerator:
    """Get H3 grid generator with default or specified resolution."""
    res = resolution or config.h3.resolution
    return H3GridGenerator(resolution=res)


def latlng_to_hex(lat: float, lng: float, resolution: Optional[int] = None) -> str:
    """Convert lat/lng to H3 hex using default resolution."""
    res = resolution or config.h3.resolution
    return h3.geo_to_h3(lat, lng, res)
