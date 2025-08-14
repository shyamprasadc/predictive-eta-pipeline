"""
Time slab aggregation for the Predictive ETA Calculator pipeline.

Provides utilities to group routing data by time slabs (0-4, 4-8, etc.) and weekdays,
computing min/max ETA values with optional weather adjustments.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from datetime import datetime, time
from enum import Enum

from ..common import config, get_logger, TimedLogger, log_data_processing

logger = get_logger("transform.slab_agg")


class WeekdayEnum(Enum):
    """Weekday enumeration."""

    MONDAY = "Monday"
    TUESDAY = "Tuesday"
    WEDNESDAY = "Wednesday"
    THURSDAY = "Thursday"
    FRIDAY = "Friday"
    SATURDAY = "Saturday"
    SUNDAY = "Sunday"


@dataclass
class TimeSlabDefinition:
    """Definition of a time slab."""

    slab_name: str
    start_hour: int
    end_hour: int

    def contains_hour(self, hour: int) -> bool:
        """Check if hour falls within this slab."""
        return self.start_hour <= hour < self.end_hour

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "slab_name": self.slab_name,
            "start_hour": self.start_hour,
            "end_hour": self.end_hour,
        }


@dataclass
class ETAAggregation:
    """Aggregated ETA statistics for a hex pair and time period."""

    from_hex: str
    to_hex: str
    weekday: str
    slab: str
    min_eta_s: int
    max_eta_s: int
    avg_eta_s: float
    median_eta_s: float
    sample_count: int
    rain_eta_s: Optional[int] = None
    std_eta_s: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "from_hex": self.from_hex,
            "to_hex": self.to_hex,
            "weekday": self.weekday,
            "slab": self.slab,
            "min_eta_s": self.min_eta_s,
            "max_eta_s": self.max_eta_s,
            "avg_eta_s": self.avg_eta_s,
            "median_eta_s": self.median_eta_s,
            "sample_count": self.sample_count,
            "rain_eta_s": self.rain_eta_s,
            "std_eta_s": self.std_eta_s,
        }


class TimeSlabAggregator:
    """Aggregates routing data into time slabs with min/max ETA calculations."""

    def __init__(
        self,
        time_slabs: Optional[List[str]] = None,
        rain_uplift_pct: Optional[float] = None,
    ):
        """
        Initialize time slab aggregator.

        Args:
            time_slabs: List of time slab definitions (e.g., ["0-4", "4-8"])
            rain_uplift_pct: Rain weather uplift percentage
        """
        self.time_slabs = time_slabs or config.eta.time_slabs
        self.rain_uplift_pct = rain_uplift_pct or config.eta.rain_uplift_pct

        # Parse time slab definitions
        self.slab_definitions = self._parse_time_slabs()

        self.logger = logger

    def _parse_time_slabs(self) -> List[TimeSlabDefinition]:
        """Parse time slab strings into TimeSlabDefinition objects."""
        definitions = []

        for slab_str in self.time_slabs:
            try:
                if "-" in slab_str:
                    start_str, end_str = slab_str.split("-", 1)
                    start_hour = int(start_str)
                    end_hour = int(end_str)

                    # Handle edge case for 24-hour format
                    if end_hour == 24:
                        end_hour = 0  # Treat as next day

                    definitions.append(
                        TimeSlabDefinition(
                            slab_name=slab_str,
                            start_hour=start_hour,
                            end_hour=end_hour if end_hour != 0 else 24,
                        )
                    )
                else:
                    raise ValueError(f"Invalid slab format: {slab_str}")

            except ValueError as e:
                self.logger.error(f"Error parsing time slab '{slab_str}': {e}")
                raise

        return definitions

    def get_slab_for_hour(self, hour: int) -> Optional[str]:
        """
        Get time slab name for a given hour.

        Args:
            hour: Hour of day (0-23)

        Returns:
            Slab name or None if hour doesn't fit any slab
        """
        for slab_def in self.slab_definitions:
            if slab_def.contains_hour(hour):
                return slab_def.slab_name

        return None

    def get_weekday_name(self, date_obj: datetime) -> str:
        """
        Get weekday name from datetime object.

        Args:
            date_obj: Datetime object

        Returns:
            Weekday name (e.g., "Monday")
        """
        weekday_names = [
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
            "Sunday",
        ]
        return weekday_names[date_obj.weekday()]

    def prepare_aggregation_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare DataFrame for aggregation by adding time slab and weekday columns.

        Args:
            df: DataFrame with routing data (must have 'depart_ts' or 'hour' column)

        Returns:
            DataFrame with added 'weekday' and 'slab' columns
        """
        df = df.copy()

        # Ensure we have datetime information
        if "depart_ts" in df.columns:
            df["depart_ts"] = pd.to_datetime(df["depart_ts"])
            df["hour"] = df["depart_ts"].dt.hour
            df["weekday"] = df["depart_ts"].dt.day_name()
        elif "hour" in df.columns and "weekday" in df.columns:
            # Already have hour and weekday
            pass
        else:
            raise ValueError(
                "DataFrame must have either 'depart_ts' or both 'hour' and 'weekday' columns"
            )

        # Add time slab
        df["slab"] = df["hour"].apply(self.get_slab_for_hour)

        # Filter out rows without valid slab assignment
        valid_slab_mask = df["slab"].notna()
        if not valid_slab_mask.all():
            filtered_count = (~valid_slab_mask).sum()
            self.logger.warning(
                f"Filtered out {filtered_count} records without valid time slab"
            )
            df = df[valid_slab_mask]

        return df

    def aggregate_eta_by_hex_pair_slab(
        self,
        df: pd.DataFrame,
        duration_column: str = "duration_s",
        groupby_columns: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Aggregate ETA data by hex pair, weekday, and time slab.

        Args:
            df: DataFrame with routing data
            duration_column: Column name containing duration in seconds
            groupby_columns: Additional columns to group by

        Returns:
            DataFrame with aggregated ETA statistics
        """
        # Prepare data for aggregation
        df_prepared = self.prepare_aggregation_data(df)

        # Default groupby columns
        base_groupby = ["from_hex", "to_hex", "weekday", "slab"]
        if groupby_columns:
            base_groupby.extend(groupby_columns)

        with TimedLogger(
            self.logger, f"aggregate_eta_by_hex_pair_slab: {len(df_prepared)} records"
        ):
            # Perform aggregation
            agg_functions = {
                duration_column: [
                    ("min_eta_s", "min"),
                    ("max_eta_s", "max"),
                    ("avg_eta_s", "mean"),
                    ("median_eta_s", "median"),
                    ("std_eta_s", "std"),
                    ("sample_count", "count"),
                ]
            }

            # Add distance aggregation if available
            if "distance_m" in df_prepared.columns:
                agg_functions["distance_m"] = [
                    ("avg_distance_m", "mean"),
                    ("min_distance_m", "min"),
                    ("max_distance_m", "max"),
                ]

            # Add provider precedence aggregation if available
            if "provider" in df_prepared.columns:
                agg_functions["provider"] = [
                    (
                        "provider_pref",
                        lambda x: x.mode().iloc[0] if not x.mode().empty else x.iloc[0],
                    )
                ]

            result_df = df_prepared.groupby(base_groupby).agg(agg_functions)

            # Flatten column names
            result_df.columns = [
                col[1] if col[1] else col[0] for col in result_df.columns
            ]
            result_df = result_df.reset_index()

            # Convert float columns to appropriate types
            result_df["min_eta_s"] = result_df["min_eta_s"].astype(int)
            result_df["max_eta_s"] = result_df["max_eta_s"].astype(int)
            result_df["sample_count"] = result_df["sample_count"].astype(int)

            # Add rain-adjusted ETA if uplift is configured
            if self.rain_uplift_pct > 0:
                result_df["rain_eta_s"] = (
                    result_df["max_eta_s"] * (1 + self.rain_uplift_pct / 100)
                ).astype(int)
            else:
                result_df["rain_eta_s"] = None

            # Add updated timestamp
            result_df["updated_at"] = datetime.utcnow()

            self.logger.info(
                "ETA aggregation complete",
                extra=log_data_processing(
                    stage="eta_aggregation",
                    records_processed=len(result_df),
                    input_records=len(df_prepared),
                ),
            )

            return result_df

    def aggregate_hourly_to_slabs(self, hourly_df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate hourly ETA data into time slabs.

        Args:
            hourly_df: DataFrame with hourly ETA aggregations

        Returns:
            DataFrame with slab-level aggregations
        """
        # Add slab information to hourly data
        hourly_df = hourly_df.copy()
        hourly_df["slab"] = hourly_df["hour"].apply(self.get_slab_for_hour)

        # Filter valid slabs
        hourly_df = hourly_df[hourly_df["slab"].notna()]

        with TimedLogger(
            self.logger, f"aggregate_hourly_to_slabs: {len(hourly_df)} hourly records"
        ):
            # Aggregate by slab
            groupby_cols = ["from_hex", "to_hex", "weekday", "slab"]

            # Weighted aggregation based on sample counts
            def weighted_average(group, value_col, weight_col):
                return np.average(group[value_col], weights=group[weight_col])

            # Aggregate with appropriate functions for each metric
            agg_result = (
                hourly_df.groupby(groupby_cols)
                .agg(
                    {
                        "min_eta_s": "min",  # Minimum of minimums
                        "max_eta_s": "max",  # Maximum of maximums
                        "avg_eta_s": lambda x: weighted_average(
                            hourly_df.loc[x.index], "avg_eta_s", "sample_count"
                        ),
                        "sample_count": "sum",  # Sum of all samples
                        "provider_pref": lambda x: x.mode().iloc[0]
                        if not x.mode().empty
                        else x.iloc[0],
                    }
                )
                .reset_index()
            )

            # Add rain-adjusted ETA if uplift is configured
            if self.rain_uplift_pct > 0:
                agg_result["rain_eta_s"] = (
                    agg_result["max_eta_s"] * (1 + self.rain_uplift_pct / 100)
                ).astype(int)
            else:
                agg_result["rain_eta_s"] = None

            # Add updated timestamp
            agg_result["updated_at"] = datetime.utcnow()

            self.logger.info(
                "Hourly to slab aggregation complete",
                extra=log_data_processing(
                    stage="hourly_to_slab",
                    records_processed=len(agg_result),
                    input_records=len(hourly_df),
                ),
            )

            return agg_result

    def create_eta_aggregations(
        self, aggregated_df: pd.DataFrame
    ) -> List[ETAAggregation]:
        """
        Convert aggregated DataFrame to ETAAggregation objects.

        Args:
            aggregated_df: DataFrame with aggregated ETA data

        Returns:
            List of ETAAggregation objects
        """
        aggregations = []

        for _, row in aggregated_df.iterrows():
            aggregation = ETAAggregation(
                from_hex=row["from_hex"],
                to_hex=row["to_hex"],
                weekday=row["weekday"],
                slab=row["slab"],
                min_eta_s=int(row["min_eta_s"]),
                max_eta_s=int(row["max_eta_s"]),
                avg_eta_s=float(row["avg_eta_s"]),
                median_eta_s=float(row.get("median_eta_s", row["avg_eta_s"])),
                sample_count=int(row["sample_count"]),
                rain_eta_s=int(row["rain_eta_s"])
                if pd.notna(row.get("rain_eta_s"))
                else None,
                std_eta_s=float(row["std_eta_s"])
                if pd.notna(row.get("std_eta_s"))
                else None,
            )
            aggregations.append(aggregation)

        return aggregations

    def validate_aggregations(
        self, aggregations: List[ETAAggregation]
    ) -> List[ETAAggregation]:
        """
        Validate and filter aggregations for data quality.

        Args:
            aggregations: List of ETAAggregation objects

        Returns:
            Filtered list of valid aggregations
        """
        valid_aggregations = []

        for agg in aggregations:
            # Basic validation checks
            if agg.min_eta_s <= 0:
                self.logger.warning(
                    f"Invalid min_eta_s: {agg.min_eta_s} for {agg.from_hex}->{agg.to_hex}"
                )
                continue

            if agg.max_eta_s < agg.min_eta_s:
                self.logger.warning(
                    f"max_eta_s < min_eta_s for {agg.from_hex}->{agg.to_hex}"
                )
                continue

            if agg.sample_count < 1:
                self.logger.warning(
                    f"Insufficient samples: {agg.sample_count} for {agg.from_hex}->{agg.to_hex}"
                )
                continue

            # Reasonable bounds check (configurable)
            max_reasonable_eta = 7200  # 2 hours
            if agg.max_eta_s > max_reasonable_eta:
                self.logger.warning(
                    f"Suspiciously high ETA: {agg.max_eta_s}s for {agg.from_hex}->{agg.to_hex}"
                )
                # Could optionally continue or cap the value

            valid_aggregations.append(agg)

        self.logger.info(
            f"Validation complete: {len(valid_aggregations)}/{len(aggregations)} aggregations passed"
        )
        return valid_aggregations

    def get_slab_statistics(self, aggregated_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Get statistics about the aggregated data.

        Args:
            aggregated_df: DataFrame with aggregated ETA data

        Returns:
            Dictionary with statistics
        """
        stats = {
            "total_aggregations": len(aggregated_df),
            "unique_hex_pairs": aggregated_df[["from_hex", "to_hex"]]
            .drop_duplicates()
            .shape[0],
            "weekdays_covered": sorted(aggregated_df["weekday"].unique().tolist()),
            "slabs_covered": sorted(aggregated_df["slab"].unique().tolist()),
            "total_samples": aggregated_df["sample_count"].sum(),
            "avg_samples_per_aggregation": aggregated_df["sample_count"].mean(),
            "eta_statistics": {
                "min_eta_overall": aggregated_df["min_eta_s"].min(),
                "max_eta_overall": aggregated_df["max_eta_s"].max(),
                "avg_eta_overall": aggregated_df["avg_eta_s"].mean(),
                "median_eta_overall": aggregated_df["avg_eta_s"].median(),
            },
        }

        return stats


# Convenience functions
def create_time_slab_aggregator(
    time_slabs: Optional[List[str]] = None, rain_uplift_pct: Optional[float] = None
) -> TimeSlabAggregator:
    """Create time slab aggregator with configuration."""
    return TimeSlabAggregator(time_slabs, rain_uplift_pct)


def aggregate_routing_data_to_slabs(
    df: pd.DataFrame, duration_column: str = "duration_s"
) -> pd.DataFrame:
    """Aggregate routing data to time slabs using default aggregator."""
    aggregator = create_time_slab_aggregator()
    return aggregator.aggregate_eta_by_hex_pair_slab(df, duration_column)
