"""
Routing path transformation for the Predictive ETA Calculator pipeline.

Provides utilities to map routes between H3 hexes, including both direct routing
and path-based routing through intermediate hexes using line interpolation or
provider polylines.
"""

import h3
import numpy as np
from typing import List, Tuple, Dict, Optional, Any
from dataclasses import dataclass
from geographiclib.geodesic import Geodesic
import polyline
from shapely.geometry import LineString, Point

from ..common import config, get_logger, TimedLogger
from ..h3 import get_grid_generator, latlng_to_hex

logger = get_logger("transform.routing_paths")


@dataclass
class PathSegment:
    """Represents a segment of a routing path between two hexes."""

    from_hex: str
    to_hex: str
    distance_m: float
    duration_s: float
    sequence_order: int


@dataclass
class RoutingPath:
    """Complete routing path from origin to destination."""

    origin_hex: str
    destination_hex: str
    segments: List[PathSegment]
    total_distance_m: float
    total_duration_s: float
    method: str  # 'direct' or 'path_sum'

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "origin_hex": self.origin_hex,
            "destination_hex": self.destination_hex,
            "segments": [
                {
                    "from_hex": seg.from_hex,
                    "to_hex": seg.to_hex,
                    "distance_m": seg.distance_m,
                    "duration_s": seg.duration_s,
                    "sequence_order": seg.sequence_order,
                }
                for seg in self.segments
            ],
            "total_distance_m": self.total_distance_m,
            "total_duration_s": self.total_duration_s,
            "method": self.method,
        }


class RoutingPathMapper:
    """Maps routes between H3 hexes using different strategies."""

    def __init__(self, resolution: int = 7):
        """
        Initialize routing path mapper.

        Args:
            resolution: H3 resolution level
        """
        self.resolution = resolution
        self.grid_generator = get_grid_generator(resolution)
        self.logger = logger

        # Geodesic calculator for accurate distance calculations
        self.geod = Geodesic.WGS84

    def create_direct_path(
        self, from_hex: str, to_hex: str, distance_m: float, duration_s: float
    ) -> RoutingPath:
        """
        Create a direct routing path between two hexes.

        Args:
            from_hex: Origin H3 hex ID
            to_hex: Destination H3 hex ID
            distance_m: Direct distance in meters
            duration_s: Direct duration in seconds

        Returns:
            RoutingPath with single direct segment
        """
        segment = PathSegment(
            from_hex=from_hex,
            to_hex=to_hex,
            distance_m=distance_m,
            duration_s=duration_s,
            sequence_order=0,
        )

        return RoutingPath(
            origin_hex=from_hex,
            destination_hex=to_hex,
            segments=[segment],
            total_distance_m=distance_m,
            total_duration_s=duration_s,
            method="direct",
        )

    def create_interpolated_path(
        self,
        from_hex: str,
        to_hex: str,
        total_distance_m: float,
        total_duration_s: float,
        num_intermediate_points: int = 10,
    ) -> RoutingPath:
        """
        Create a routing path with intermediate hexes using line interpolation.

        Args:
            from_hex: Origin H3 hex ID
            to_hex: Destination H3 hex ID
            total_distance_m: Total route distance
            total_duration_s: Total route duration
            num_intermediate_points: Number of intermediate points to interpolate

        Returns:
            RoutingPath with segments through intermediate hexes
        """
        # Get coordinates for origin and destination
        from_lat, from_lng = self.grid_generator.hex_to_latlng(from_hex)
        to_lat, to_lng = self.grid_generator.hex_to_latlng(to_hex)

        # Generate intermediate points along the great circle path
        intermediate_coords = self._interpolate_great_circle(
            from_lat, from_lng, to_lat, to_lng, num_intermediate_points
        )

        # Convert coordinates to H3 hexes
        hex_sequence = [from_hex]
        for lat, lng in intermediate_coords:
            hex_id = latlng_to_hex(lat, lng, self.resolution)
            if hex_id != hex_sequence[-1]:  # Avoid duplicates
                hex_sequence.append(hex_id)

        if hex_sequence[-1] != to_hex:
            hex_sequence.append(to_hex)

        # Create segments between consecutive hexes
        segments = []
        for i in range(len(hex_sequence) - 1):
            segment_from = hex_sequence[i]
            segment_to = hex_sequence[i + 1]

            # Calculate segment distance and duration (proportional)
            segment_distance = self._calculate_hex_distance(segment_from, segment_to)
            segment_duration = (
                (segment_distance / total_distance_m) * total_duration_s
                if total_distance_m > 0
                else 0
            )

            segments.append(
                PathSegment(
                    from_hex=segment_from,
                    to_hex=segment_to,
                    distance_m=segment_distance,
                    duration_s=segment_duration,
                    sequence_order=i,
                )
            )

        return RoutingPath(
            origin_hex=from_hex,
            destination_hex=to_hex,
            segments=segments,
            total_distance_m=total_distance_m,
            total_duration_s=total_duration_s,
            method="path_sum",
        )

    def create_polyline_path(
        self,
        from_hex: str,
        to_hex: str,
        polyline_geometry: str,
        total_distance_m: float,
        total_duration_s: float,
        polyline_precision: int = 5,
    ) -> RoutingPath:
        """
        Create a routing path using provider polyline geometry.

        Args:
            from_hex: Origin H3 hex ID
            to_hex: Destination H3 hex ID
            polyline_geometry: Encoded polyline string from routing provider
            total_distance_m: Total route distance
            total_duration_s: Total route duration
            polyline_precision: Polyline precision (5 for most providers)

        Returns:
            RoutingPath with segments following the polyline
        """
        try:
            # Decode polyline to coordinates
            coordinates = polyline.decode(
                polyline_geometry, precision=polyline_precision
            )

            # Convert coordinates to H3 hexes
            hex_sequence = [from_hex]
            for lat, lng in coordinates:
                hex_id = latlng_to_hex(lat, lng, self.resolution)
                if hex_id != hex_sequence[-1]:  # Avoid duplicates
                    hex_sequence.append(hex_id)

            if hex_sequence[-1] != to_hex:
                hex_sequence.append(to_hex)

            # Create segments between consecutive hexes
            segments = []
            cumulative_distance = 0

            for i in range(len(hex_sequence) - 1):
                segment_from = hex_sequence[i]
                segment_to = hex_sequence[i + 1]

                # Calculate segment distance
                segment_distance = self._calculate_hex_distance(
                    segment_from, segment_to
                )
                cumulative_distance += segment_distance

                # Estimate duration proportionally
                segment_duration = (
                    (segment_distance / total_distance_m) * total_duration_s
                    if total_distance_m > 0
                    else 0
                )

                segments.append(
                    PathSegment(
                        from_hex=segment_from,
                        to_hex=segment_to,
                        distance_m=segment_distance,
                        duration_s=segment_duration,
                        sequence_order=i,
                    )
                )

            return RoutingPath(
                origin_hex=from_hex,
                destination_hex=to_hex,
                segments=segments,
                total_distance_m=total_distance_m,
                total_duration_s=total_duration_s,
                method="path_sum",
            )

        except Exception as e:
            self.logger.warning(
                f"Failed to decode polyline, falling back to interpolation: {e}"
            )
            return self.create_interpolated_path(
                from_hex, to_hex, total_distance_m, total_duration_s
            )

    def _interpolate_great_circle(
        self,
        from_lat: float,
        from_lng: float,
        to_lat: float,
        to_lng: float,
        num_points: int,
    ) -> List[Tuple[float, float]]:
        """
        Interpolate points along a great circle path.

        Args:
            from_lat: Origin latitude
            from_lng: Origin longitude
            to_lat: Destination latitude
            to_lng: Destination longitude
            num_points: Number of intermediate points

        Returns:
            List of (lat, lng) tuples for intermediate points
        """
        if num_points <= 0:
            return []

        # Calculate total distance
        line = self.geod.InverseLine(from_lat, from_lng, to_lat, to_lng)
        total_distance = line.s13

        # Generate intermediate points
        points = []
        for i in range(1, num_points + 1):
            distance = (i / (num_points + 1)) * total_distance
            point = line.Position(distance)
            points.append((point["lat2"], point["lon2"]))

        return points

    def _calculate_hex_distance(self, from_hex: str, to_hex: str) -> float:
        """Calculate distance between two H3 hexes in meters."""
        from_lat, from_lng = self.grid_generator.hex_to_latlng(from_hex)
        to_lat, to_lng = self.grid_generator.hex_to_latlng(to_hex)

        result = self.geod.Inverse(from_lat, from_lng, to_lat, to_lng)
        return result["s12"]  # Distance in meters

    def optimize_path_segments(
        self, path: RoutingPath, segment_durations: Dict[Tuple[str, str], float]
    ) -> RoutingPath:
        """
        Optimize path using actual segment durations if available.

        Args:
            path: Original routing path
            segment_durations: Dictionary mapping (from_hex, to_hex) to actual duration

        Returns:
            Optimized routing path with updated durations
        """
        optimized_segments = []
        total_optimized_duration = 0

        for segment in path.segments:
            segment_key = (segment.from_hex, segment.to_hex)

            if segment_key in segment_durations:
                # Use actual duration
                optimized_duration = segment_durations[segment_key]
            else:
                # Keep estimated duration
                optimized_duration = segment.duration_s

            optimized_segments.append(
                PathSegment(
                    from_hex=segment.from_hex,
                    to_hex=segment.to_hex,
                    distance_m=segment.distance_m,
                    duration_s=optimized_duration,
                    sequence_order=segment.sequence_order,
                )
            )

            total_optimized_duration += optimized_duration

        return RoutingPath(
            origin_hex=path.origin_hex,
            destination_hex=path.destination_hex,
            segments=optimized_segments,
            total_distance_m=path.total_distance_m,
            total_duration_s=total_optimized_duration,
            method=path.method,
        )

    def validate_path(self, path: RoutingPath) -> bool:
        """
        Validate that a routing path is consistent.

        Args:
            path: Routing path to validate

        Returns:
            True if path is valid
        """
        if not path.segments:
            return False

        # Check sequence continuity
        for i in range(len(path.segments) - 1):
            if path.segments[i].to_hex != path.segments[i + 1].from_hex:
                self.logger.error(f"Path discontinuity at segment {i}")
                return False

        # Check origin and destination
        if path.segments[0].from_hex != path.origin_hex:
            self.logger.error("Path origin mismatch")
            return False

        if path.segments[-1].to_hex != path.destination_hex:
            self.logger.error("Path destination mismatch")
            return False

        # Check total distance and duration
        segment_distance = sum(seg.distance_m for seg in path.segments)
        segment_duration = sum(seg.duration_s for seg in path.segments)

        if path.method == "path_sum":
            # For path_sum, totals should match segment sums
            if abs(segment_distance - path.total_distance_m) > 1000:  # 1km tolerance
                self.logger.warning(
                    f"Distance mismatch: {segment_distance} vs {path.total_distance_m}"
                )

            if abs(segment_duration - path.total_duration_s) > 60:  # 60s tolerance
                self.logger.warning(
                    f"Duration mismatch: {segment_duration} vs {path.total_duration_s}"
                )

        return True


# Convenience functions
def create_routing_path_mapper(resolution: Optional[int] = None) -> RoutingPathMapper:
    """Create routing path mapper with default or specified resolution."""
    res = resolution or config.h3.resolution
    return RoutingPathMapper(resolution=res)


def create_direct_routing_path(
    from_hex: str, to_hex: str, distance_m: float, duration_s: float
) -> RoutingPath:
    """Create direct routing path using default mapper."""
    mapper = create_routing_path_mapper()
    return mapper.create_direct_path(from_hex, to_hex, distance_m, duration_s)


def create_path_with_interpolation(
    from_hex: str,
    to_hex: str,
    distance_m: float,
    duration_s: float,
    num_intermediate_points: int = 10,
) -> RoutingPath:
    """Create interpolated routing path using default mapper."""
    mapper = create_routing_path_mapper()
    return mapper.create_interpolated_path(
        from_hex, to_hex, distance_m, duration_s, num_intermediate_points
    )
