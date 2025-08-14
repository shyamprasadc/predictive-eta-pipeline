"""
Snowflake data loading utilities for the Predictive ETA Calculator pipeline.

Provides specialized loaders for routing data with staged loads, MERGE upserts,
and batch processing optimized for the ETL pipeline requirements.
"""

import pandas as pd
from typing import List, Dict, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime
import tempfile
import json

from ..common import (
    config,
    get_logger,
    TimedLogger,
    log_database_operation,
    get_raw_connection,
    get_core_connection,
)
from ..ingest import RouteResult
from ..transform import ETAAggregation

logger = get_logger("load.snowflake")


@dataclass
class LoadResult:
    """Result of a Snowflake load operation."""

    table_name: str
    records_loaded: int
    records_failed: int
    load_duration_ms: float
    operation_type: str  # INSERT, MERGE, UPSERT
    batch_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


class RoutesRawLoader:
    """Loader for RAW.ROUTES_RAW table with routing data."""

    def __init__(self):
        """Initialize routes raw loader."""
        self.connection = get_raw_connection()
        self.table_name = "ROUTES_RAW"
        self.logger = logger

    def load_route_results(
        self,
        route_results: List[RouteResult],
        batch_id: Optional[str] = None,
        use_merge: bool = True,
    ) -> LoadResult:
        """
        Load route results into ROUTES_RAW table.

        Args:
            route_results: List of RouteResult objects
            batch_id: Optional batch identifier
            use_merge: Whether to use MERGE for upsert behavior

        Returns:
            LoadResult with operation statistics
        """
        if not route_results:
            self.logger.warning("No route results to load")
            return LoadResult(
                table_name=self.table_name,
                records_loaded=0,
                records_failed=0,
                load_duration_ms=0.0,
                operation_type="SKIP",
                batch_id=batch_id,
            )

        with TimedLogger(
            self.logger, f"load_route_results: {len(route_results)} records"
        ) as timer:
            # Convert to DataFrame
            df = self._route_results_to_dataframe(route_results, batch_id)

            try:
                if use_merge:
                    success = self._merge_route_data(df)
                    operation_type = "MERGE"
                else:
                    success = self.connection.bulk_insert_dataframe(
                        df, self.table_name, if_exists="append"
                    )
                    operation_type = "INSERT"

                if success:
                    records_loaded = len(df)
                    records_failed = 0
                else:
                    records_loaded = 0
                    records_failed = len(df)

            except Exception as e:
                self.logger.error(f"Failed to load route results: {e}")
                records_loaded = 0
                records_failed = len(df)
                operation_type = "FAILED"

            return LoadResult(
                table_name=self.table_name,
                records_loaded=records_loaded,
                records_failed=records_failed,
                load_duration_ms=timer.start_time
                and (datetime.utcnow() - timer.start_time).total_seconds() * 1000
                or 0,
                operation_type=operation_type,
                batch_id=batch_id,
            )

    def _route_results_to_dataframe(
        self, route_results: List[RouteResult], batch_id: Optional[str] = None
    ) -> pd.DataFrame:
        """Convert route results to DataFrame with proper schema."""
        data = []

        for route in route_results:
            # Extract hour and weekday from depart_time if available
            if route.depart_time:
                hour = route.depart_time.hour
                weekday = route.depart_time.strftime("%A")
                depart_ts = route.depart_time
            else:
                hour = None
                weekday = None
                depart_ts = None

            # Map from/to coordinates to hex IDs (if not already hex)
            from_hex = getattr(route, "from_hex", None)
            to_hex = getattr(route, "to_hex", None)

            # If we don't have hex IDs, we'd need to convert from lat/lng
            # For now, assume RouteResult has been enhanced with hex IDs
            if (
                not from_hex
                and hasattr(route, "from_lat")
                and hasattr(route, "from_lng")
            ):
                from etl.h3 import latlng_to_hex

                from_hex = latlng_to_hex(route.from_lat, route.from_lng)
                to_hex = latlng_to_hex(route.to_lat, route.to_lng)

            record = {
                "from_hex": from_hex,
                "to_hex": to_hex,
                "provider": route.provider,
                "distance_m": route.distance_m,
                "duration_s": route.duration_s,
                "depart_ts": depart_ts,
                "weekday": weekday,
                "hour": hour,
                "ingested_at": datetime.utcnow(),
                "request_id": route.request_id
                or f"{route.provider}_{from_hex}_{to_hex}_{int(datetime.utcnow().timestamp())}",
                "batch_id": batch_id,
                "traffic_duration_s": route.traffic_duration_s,
            }

            data.append(record)

        df = pd.DataFrame(data)

        # Ensure proper data types
        df["distance_m"] = df["distance_m"].astype(float)
        df["duration_s"] = df["duration_s"].astype(float)
        df["hour"] = df["hour"].astype("Int64")  # Nullable integer

        return df

    def _merge_route_data(self, df: pd.DataFrame) -> bool:
        """
        Merge route data using MERGE statement for upsert behavior.

        Args:
            df: DataFrame with route data

        Returns:
            True if successful
        """
        if df.empty:
            return True

        # Use stage and copy with MERGE
        merge_keys = ["from_hex", "to_hex", "provider", "depart_ts"]

        try:
            return self.connection.stage_and_copy(
                df=df,
                table_name=self.table_name,
                stage_name="@~/routes_raw_stage",
                file_format="CSV",
                merge_keys=merge_keys,
            )
        except Exception as e:
            self.logger.error(f"Merge operation failed: {e}")
            return False


class H3LookupLoader:
    """Loader for CORE.H3_LOOKUP table with H3 grid data."""

    def __init__(self):
        """Initialize H3 lookup loader."""
        self.connection = get_core_connection()
        self.table_name = "H3_LOOKUP"
        self.logger = logger

    def load_h3_grid(
        self, grid_df: pd.DataFrame, replace_existing: bool = False
    ) -> LoadResult:
        """
        Load H3 grid data into H3_LOOKUP table.

        Args:
            grid_df: DataFrame with H3 grid data
            replace_existing: Whether to replace existing data

        Returns:
            LoadResult with operation statistics
        """
        if grid_df.empty:
            self.logger.warning("No H3 grid data to load")
            return LoadResult(
                table_name=self.table_name,
                records_loaded=0,
                records_failed=0,
                load_duration_ms=0.0,
                operation_type="SKIP",
            )

        with TimedLogger(self.logger, f"load_h3_grid: {len(grid_df)} records") as timer:
            # Prepare DataFrame
            df = grid_df.copy()

            # Ensure proper schema
            required_columns = [
                "hex_id",
                "city",
                "resolution",
                "centroid_lat",
                "centroid_lng",
            ]
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")

            try:
                if replace_existing:
                    # Delete existing data for this city/resolution first
                    city = df["city"].iloc[0]
                    resolution = df["resolution"].iloc[0]

                    delete_sql = f"""
                    DELETE FROM {self.table_name} 
                    WHERE city = '{city}' AND resolution = {resolution}
                    """
                    self.connection.execute_query(delete_sql)

                # Insert new data
                success = self.connection.bulk_insert_dataframe(
                    df, self.table_name, if_exists="append"
                )

                if success:
                    records_loaded = len(df)
                    records_failed = 0
                    operation_type = "REPLACE" if replace_existing else "INSERT"
                else:
                    records_loaded = 0
                    records_failed = len(df)
                    operation_type = "FAILED"

            except Exception as e:
                self.logger.error(f"Failed to load H3 grid: {e}")
                records_loaded = 0
                records_failed = len(df)
                operation_type = "FAILED"

            return LoadResult(
                table_name=self.table_name,
                records_loaded=records_loaded,
                records_failed=records_failed,
                load_duration_ms=timer.start_time
                and (datetime.utcnow() - timer.start_time).total_seconds() * 1000
                or 0,
                operation_type=operation_type,
            )


class ETASlabsLoader:
    """Loader for CORE.ETA_SLABS table with final ETA aggregations."""

    def __init__(self):
        """Initialize ETA slabs loader."""
        self.connection = get_core_connection()
        self.table_name = "ETA_SLABS"
        self.logger = logger

    def load_eta_aggregations(
        self, aggregations: List[ETAAggregation], use_merge: bool = True
    ) -> LoadResult:
        """
        Load ETA aggregations into ETA_SLABS table.

        Args:
            aggregations: List of ETAAggregation objects
            use_merge: Whether to use MERGE for upsert behavior

        Returns:
            LoadResult with operation statistics
        """
        if not aggregations:
            self.logger.warning("No ETA aggregations to load")
            return LoadResult(
                table_name=self.table_name,
                records_loaded=0,
                records_failed=0,
                load_duration_ms=0.0,
                operation_type="SKIP",
            )

        with TimedLogger(
            self.logger, f"load_eta_aggregations: {len(aggregations)} records"
        ) as timer:
            # Convert to DataFrame
            df = self._aggregations_to_dataframe(aggregations)

            try:
                if use_merge:
                    success = self._merge_eta_data(df)
                    operation_type = "MERGE"
                else:
                    success = self.connection.bulk_insert_dataframe(
                        df, self.table_name, if_exists="append"
                    )
                    operation_type = "INSERT"

                if success:
                    records_loaded = len(df)
                    records_failed = 0
                else:
                    records_loaded = 0
                    records_failed = len(df)

            except Exception as e:
                self.logger.error(f"Failed to load ETA aggregations: {e}")
                records_loaded = 0
                records_failed = len(df)
                operation_type = "FAILED"

            return LoadResult(
                table_name=self.table_name,
                records_loaded=records_loaded,
                records_failed=records_failed,
                load_duration_ms=timer.start_time
                and (datetime.utcnow() - timer.start_time).total_seconds() * 1000
                or 0,
                operation_type=operation_type,
            )

    def _aggregations_to_dataframe(
        self, aggregations: List[ETAAggregation]
    ) -> pd.DataFrame:
        """Convert ETA aggregations to DataFrame."""
        data = [agg.to_dict() for agg in aggregations]
        df = pd.DataFrame(data)

        # Add updated_at timestamp
        df["updated_at"] = datetime.utcnow()

        # Ensure proper data types
        df["min_eta_s"] = df["min_eta_s"].astype(int)
        df["max_eta_s"] = df["max_eta_s"].astype(int)
        df["sample_count"] = df["sample_count"].astype(int)
        df["rain_eta_s"] = df["rain_eta_s"].astype("Int64")  # Nullable integer

        return df

    def _merge_eta_data(self, df: pd.DataFrame) -> bool:
        """
        Merge ETA data using MERGE statement for upsert behavior.

        Args:
            df: DataFrame with ETA data

        Returns:
            True if successful
        """
        if df.empty:
            return True

        # MERGE on unique key: from_hex, to_hex, weekday, slab
        merge_keys = ["from_hex", "to_hex", "weekday", "slab"]

        try:
            return self.connection.stage_and_copy(
                df=df,
                table_name=self.table_name,
                stage_name="@~/eta_slabs_stage",
                file_format="CSV",
                merge_keys=merge_keys,
            )
        except Exception as e:
            self.logger.error(f"Merge operation failed: {e}")
            return False


class BatchLoader:
    """Coordinated batch loader for multiple tables."""

    def __init__(self):
        """Initialize batch loader."""
        self.routes_loader = RoutesRawLoader()
        self.h3_loader = H3LookupLoader()
        self.eta_loader = ETASlabsLoader()
        self.logger = logger

    def load_complete_batch(
        self,
        route_results: Optional[List[RouteResult]] = None,
        h3_grid: Optional[pd.DataFrame] = None,
        eta_aggregations: Optional[List[ETAAggregation]] = None,
        batch_id: Optional[str] = None,
    ) -> Dict[str, LoadResult]:
        """
        Load a complete batch of data across all tables.

        Args:
            route_results: Route results for ROUTES_RAW
            h3_grid: H3 grid data for H3_LOOKUP
            eta_aggregations: ETA aggregations for ETA_SLABS
            batch_id: Batch identifier

        Returns:
            Dictionary mapping table names to LoadResult objects
        """
        results = {}

        # Load H3 grid first (foundational data)
        if h3_grid is not None:
            self.logger.info("Loading H3 grid data")
            results["H3_LOOKUP"] = self.h3_loader.load_h3_grid(h3_grid)

        # Load raw route data
        if route_results:
            self.logger.info("Loading route results")
            results["ROUTES_RAW"] = self.routes_loader.load_route_results(
                route_results, batch_id=batch_id
            )

        # Load ETA aggregations
        if eta_aggregations:
            self.logger.info("Loading ETA aggregations")
            results["ETA_SLABS"] = self.eta_loader.load_eta_aggregations(
                eta_aggregations
            )

        # Log summary
        total_loaded = sum(result.records_loaded for result in results.values())
        total_failed = sum(result.records_failed for result in results.values())

        self.logger.info(
            f"Batch load complete",
            extra={
                "batch_id": batch_id,
                "tables_loaded": len(results),
                "total_records_loaded": total_loaded,
                "total_records_failed": total_failed,
                "load_results": {
                    table: result.to_dict() for table, result in results.items()
                },
            },
        )

        return results

    def get_load_summary(self, results: Dict[str, LoadResult]) -> Dict[str, Any]:
        """Get summary statistics for a batch load."""
        return {
            "tables_processed": len(results),
            "total_records_loaded": sum(r.records_loaded for r in results.values()),
            "total_records_failed": sum(r.records_failed for r in results.values()),
            "total_duration_ms": sum(r.load_duration_ms for r in results.values()),
            "success_rate": sum(r.records_loaded for r in results.values())
            / (
                sum(r.records_loaded for r in results.values())
                + sum(r.records_failed for r in results.values())
            )
            if any(r.records_loaded + r.records_failed > 0 for r in results.values())
            else 0,
            "results_by_table": {
                table: result.to_dict() for table, result in results.items()
            },
        }


# Convenience functions
def load_route_results_to_snowflake(
    route_results: List[RouteResult], batch_id: Optional[str] = None
) -> LoadResult:
    """Load route results using default loader."""
    loader = RoutesRawLoader()
    return loader.load_route_results(route_results, batch_id)


def load_h3_grid_to_snowflake(grid_df: pd.DataFrame) -> LoadResult:
    """Load H3 grid using default loader."""
    loader = H3LookupLoader()
    return loader.load_h3_grid(grid_df)


def load_eta_aggregations_to_snowflake(
    aggregations: List[ETAAggregation],
) -> LoadResult:
    """Load ETA aggregations using default loader."""
    loader = ETASlabsLoader()
    return loader.load_eta_aggregations(aggregations)
