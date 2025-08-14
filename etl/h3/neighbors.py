"""
H3 neighbor discovery for the Predictive ETA Calculator pipeline.

Provides utilities to find neighboring H3 hexes within specified distances
using k-ring algorithms. Supports approximate distance-based neighbor discovery.
"""

import h3
import pandas as pd
from typing import Set, List, Dict, Tuple, Optional
from dataclasses import dataclass
from collections import defaultdict

from ..common import config, get_logger, TimedLogger

logger = get_logger("h3.neighbors")


@dataclass
class NeighborConfig:
    """Configuration for neighbor discovery distances."""

    distance_1km: int  # K-ring distance for ~1km
    distance_3km: int  # K-ring distance for ~3km
    distance_8km: int  # K-ring distance for ~8km

    @classmethod
    def from_config(cls) -> "NeighborConfig":
        """Create NeighborConfig from global configuration."""
        return cls(
            distance_1km=config.h3.neighbor_distance_1km,
            distance_3km=config.h3.neighbor_distance_3km,
            distance_8km=config.h3.neighbor_distance_8km,
        )


class H3NeighborDiscovery:
    """Discovers H3 hex neighbors within specified distances."""

    def __init__(self, resolution: int = 7):
        """
        Initialize neighbor discovery.

        Args:
            resolution: H3 resolution level
        """
        self.resolution = resolution
        self.logger = logger
        self.neighbor_config = NeighborConfig.from_config()

        # Cache for performance
        self._neighbor_cache: Dict[Tuple[str, int], Set[str]] = {}

        # Resolution-specific distance calibration
        self._distance_calibration = self._get_distance_calibration()

    def _get_distance_calibration(self) -> Dict[str, Dict[int, float]]:
        """
        Get distance calibration for different H3 resolutions.

        Returns actual average distances for k-ring values at different resolutions.
        This helps map from desired km distances to appropriate k-ring values.
        """
        # These are approximate values based on H3 documentation
        # In production, you might want to calibrate these empirically
        calibration = {
            "6": {1: 1.8, 2: 3.6, 3: 5.4, 4: 7.2, 5: 9.0},
            "7": {
                1: 0.9,
                2: 1.8,
                3: 2.7,
                4: 3.6,
                5: 4.5,
                6: 5.4,
                7: 6.3,
                8: 7.2,
                9: 8.1,
            },
            "8": {
                1: 0.45,
                2: 0.9,
                3: 1.35,
                4: 1.8,
                5: 2.25,
                6: 2.7,
                7: 3.15,
                8: 3.6,
                16: 7.2,
                18: 8.1,
            },
            "9": {
                1: 0.225,
                2: 0.45,
                3: 0.675,
                4: 0.9,
                8: 1.8,
                12: 2.7,
                16: 3.6,
                32: 7.2,
                36: 8.1,
            },
        }
        return calibration

    def get_k_ring_for_distance(self, target_distance_km: float) -> int:
        """
        Get appropriate k-ring value for target distance in kilometers.

        Args:
            target_distance_km: Target distance in kilometers

        Returns:
            K-ring value that approximates the target distance
        """
        resolution_str = str(self.resolution)
        if resolution_str not in self._distance_calibration:
            # Fallback for unsupported resolutions
            self.logger.warning(
                f"No calibration for resolution {self.resolution}, using estimation"
            )
            edge_length_km = h3.edge_length(self.resolution, "km")
            return max(1, int(target_distance_km / edge_length_km))

        calibration = self._distance_calibration[resolution_str]

        # Find closest k-ring value
        best_k = 1
        best_diff = float("inf")

        for k, distance in calibration.items():
            diff = abs(distance - target_distance_km)
            if diff < best_diff:
                best_diff = diff
                best_k = k

        return best_k

    def get_neighbors_within_distance(
        self, hex_id: str, distance_km: float, include_origin: bool = False
    ) -> Set[str]:
        """
        Get all H3 hexes within specified distance of the origin hex.

        Args:
            hex_id: Origin H3 hex ID
            distance_km: Distance in kilometers
            include_origin: Whether to include the origin hex in results

        Returns:
            Set of H3 hex IDs within the specified distance
        """
        cache_key = (hex_id, distance_km, include_origin)
        if cache_key in self._neighbor_cache:
            return self._neighbor_cache[cache_key]

        k_ring = self.get_k_ring_for_distance(distance_km)
        neighbors = self.get_k_ring_neighbors(hex_id, k_ring, include_origin)

        # Filter by actual distance to be more precise
        if distance_km > 0:
            neighbors = self._filter_by_actual_distance(hex_id, neighbors, distance_km)

        self._neighbor_cache[cache_key] = neighbors
        return neighbors

    def get_k_ring_neighbors(
        self, hex_id: str, k: int, include_origin: bool = False
    ) -> Set[str]:
        """
        Get all H3 hexes within k-ring distance of the origin hex.

        Args:
            hex_id: Origin H3 hex ID
            k: K-ring distance
            include_origin: Whether to include the origin hex in results

        Returns:
            Set of H3 hex IDs within k-ring distance
        """
        cache_key = (hex_id, k)
        if cache_key in self._neighbor_cache:
            neighbors = self._neighbor_cache[cache_key]
            if include_origin:
                return neighbors | {hex_id}
            else:
                return neighbors - {hex_id}

        try:
            # Get k-ring neighbors (includes origin by default)
            neighbors = set(h3.k_ring(hex_id, k))

            # Cache the result (including origin)
            self._neighbor_cache[cache_key] = neighbors

            if include_origin:
                return neighbors
            else:
                return neighbors - {hex_id}

        except Exception as e:
            self.logger.error(
                f"Error getting k-ring neighbors for {hex_id} at k={k}: {e}"
            )
            return set()

    def _filter_by_actual_distance(
        self, origin_hex: str, candidate_hexes: Set[str], max_distance_km: float
    ) -> Set[str]:
        """
        Filter hexes by actual geometric distance.

        Args:
            origin_hex: Origin H3 hex ID
            candidate_hexes: Set of candidate hex IDs to filter
            max_distance_km: Maximum distance in kilometers

        Returns:
            Filtered set of hex IDs within actual distance
        """
        origin_lat, origin_lng = h3.h3_to_geo(origin_hex)
        filtered_hexes = set()

        for hex_id in candidate_hexes:
            hex_lat, hex_lng = h3.h3_to_geo(hex_id)
            distance = h3.point_dist((origin_lat, origin_lng), (hex_lat, hex_lng), "km")

            if distance <= max_distance_km:
                filtered_hexes.add(hex_id)

        return filtered_hexes

    def get_neighbors_1km(self, hex_id: str, include_origin: bool = False) -> Set[str]:
        """Get neighbors within ~1km distance."""
        return self.get_k_ring_neighbors(
            hex_id, self.neighbor_config.distance_1km, include_origin
        )

    def get_neighbors_3km(self, hex_id: str, include_origin: bool = False) -> Set[str]:
        """Get neighbors within ~3km distance."""
        return self.get_k_ring_neighbors(
            hex_id, self.neighbor_config.distance_3km, include_origin
        )

    def get_neighbors_8km(self, hex_id: str, include_origin: bool = False) -> Set[str]:
        """Get neighbors within ~8km distance."""
        return self.get_k_ring_neighbors(
            hex_id, self.neighbor_config.distance_8km, include_origin
        )

    def get_all_neighbor_pairs(
        self, hex_ids: List[str], max_distance_km: float, bidirectional: bool = True
    ) -> List[Tuple[str, str]]:
        """
        Get all neighbor pairs within specified distance.

        Args:
            hex_ids: List of H3 hex IDs to check
            max_distance_km: Maximum distance for neighbor relationship
            bidirectional: If True, include both (A,B) and (B,A) pairs

        Returns:
            List of (from_hex, to_hex) tuples
        """
        with TimedLogger(
            self.logger, f"get_all_neighbor_pairs for {len(hex_ids)} hexes"
        ):
            pairs = []
            processed_pairs = set()

            for i, hex_id in enumerate(hex_ids):
                neighbors = self.get_neighbors_within_distance(hex_id, max_distance_km)

                for neighbor_id in neighbors:
                    if neighbor_id in hex_ids:  # Only include hexes from our input list
                        pair = (hex_id, neighbor_id)
                        reverse_pair = (neighbor_id, hex_id)

                        if bidirectional:
                            pairs.append(pair)
                        else:
                            # For unidirectional, avoid duplicates
                            if (
                                pair not in processed_pairs
                                and reverse_pair not in processed_pairs
                            ):
                                pairs.append(pair)
                                processed_pairs.add(pair)

            self.logger.info(
                f"Generated {len(pairs)} neighbor pairs",
                extra={
                    "hex_count": len(hex_ids),
                    "max_distance_km": max_distance_km,
                    "pair_count": len(pairs),
                    "bidirectional": bidirectional,
                },
            )

            return pairs

    def create_neighbor_matrix(
        self, hex_ids: List[str], max_distance_km: float
    ) -> pd.DataFrame:
        """
        Create a neighbor adjacency matrix.

        Args:
            hex_ids: List of H3 hex IDs
            max_distance_km: Maximum distance for neighbor relationship

        Returns:
            DataFrame with from_hex, to_hex, distance_km columns
        """
        with TimedLogger(
            self.logger, f"create_neighbor_matrix for {len(hex_ids)} hexes"
        ):
            matrix_data = []

            for from_hex in hex_ids:
                neighbors = self.get_neighbors_within_distance(
                    from_hex, max_distance_km
                )

                for to_hex in neighbors:
                    if to_hex in hex_ids:  # Only include hexes from our input list
                        # Calculate actual distance
                        from_lat, from_lng = h3.h3_to_geo(from_hex)
                        to_lat, to_lng = h3.h3_to_geo(to_hex)
                        distance_km = h3.point_dist(
                            (from_lat, from_lng), (to_lat, to_lng), "km"
                        )

                        matrix_data.append(
                            {
                                "from_hex": from_hex,
                                "to_hex": to_hex,
                                "distance_km": round(distance_km, 3),
                            }
                        )

            df = pd.DataFrame(matrix_data)

            self.logger.info(
                f"Created neighbor matrix",
                extra={
                    "hex_count": len(hex_ids),
                    "neighbor_pairs": len(df),
                    "max_distance_km": max_distance_km,
                },
            )

            return df

    def get_neighbor_statistics(self, hex_ids: List[str]) -> Dict[str, any]:
        """
        Get statistics about neighbor relationships for a set of hexes.

        Args:
            hex_ids: List of H3 hex IDs

        Returns:
            Dictionary with neighbor statistics
        """
        stats = {
            "total_hexes": len(hex_ids),
            "neighbor_counts": defaultdict(int),
            "distance_stats": {},
        }

        # Count neighbors at different distances
        for hex_id in hex_ids[:10]:  # Sample first 10 for performance
            neighbors_1km = len(self.get_neighbors_1km(hex_id))
            neighbors_3km = len(self.get_neighbors_3km(hex_id))
            neighbors_8km = len(self.get_neighbors_8km(hex_id))

            stats["neighbor_counts"]["1km"] += neighbors_1km
            stats["neighbor_counts"]["3km"] += neighbors_3km
            stats["neighbor_counts"]["8km"] += neighbors_8km

        # Average neighbor counts
        sample_size = min(10, len(hex_ids))
        for distance in ["1km", "3km", "8km"]:
            stats["neighbor_counts"][f"{distance}_avg"] = (
                stats["neighbor_counts"][distance] / sample_size
            )

        return stats

    def clear_cache(self):
        """Clear the neighbor cache."""
        self._neighbor_cache.clear()
        self.logger.info("Cleared neighbor cache")


# Convenience functions
def get_neighbor_discovery(resolution: Optional[int] = None) -> H3NeighborDiscovery:
    """Get neighbor discovery instance with default or specified resolution."""
    res = resolution or config.h3.resolution
    return H3NeighborDiscovery(resolution=res)


def get_hex_neighbors(
    hex_id: str,
    distance_km: float,
    resolution: Optional[int] = None,
    include_origin: bool = False,
) -> Set[str]:
    """Get neighbors for a hex using default resolution."""
    discovery = get_neighbor_discovery(resolution)
    return discovery.get_neighbors_within_distance(hex_id, distance_km, include_origin)
