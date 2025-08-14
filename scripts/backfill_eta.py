#!/usr/bin/env python3
"""
Backfill script for historical ETA data processing.

This script allows backfilling ETA data for specified date ranges, hex pairs,
and routing providers. It's designed for historical analysis and data recovery.

Usage:
    python backfill_eta.py --start-date 2024-01-01 --end-date 2024-01-07
    python backfill_eta.py --start-date 2024-01-01 --end-date 2024-01-31 --providers osrm,google
    python backfill_eta.py --config /path/to/config/.env --hex-pairs hex1,hex2 hex3,hex4
"""

import sys
import argparse
import os
from pathlib import Path
from datetime import datetime, timedelta, time
from typing import Dict, Any, List, Tuple, Optional
import concurrent.futures
from dataclasses import asdict

# Add ETL package to path
sys.path.insert(0, str(Path(__file__).parent.parent / "etl"))

from etl.common import config, get_logger, TimedLogger
from etl.h3 import get_neighbor_discovery
from etl.ingest import create_distance_matrix_client, RouteResult
from etl.transform import create_time_slab_aggregator
from etl.load import BatchLoader
from etl.state import (
    get_state_store,
    create_backfill_job,
    create_processing_job,
    ProcessingStatus,
)

logger = get_logger("backfill_eta")


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Backfill historical ETA data for Predictive ETA Calculator",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--start-date", type=str, required=True, help="Start date (YYYY-MM-DD)"
    )

    parser.add_argument(
        "--end-date", type=str, required=True, help="End date (YYYY-MM-DD)"
    )

    parser.add_argument(
        "--providers",
        type=str,
        default="osrm",
        help="Comma-separated list of providers (osrm,google,here)",
    )

    parser.add_argument(
        "--hex-pairs",
        type=str,
        nargs="*",
        help='Specific hex pairs to process as "from_hex,to_hex"',
    )

    parser.add_argument(
        "--sample-rate",
        type=float,
        default=1.0,
        help="Sample rate for hex pairs (0.0-1.0) to limit processing",
    )

    parser.add_argument(
        "--hours",
        type=str,
        help='Specific hours to process (e.g., "8,12,18" for 8am, 12pm, 6pm)',
    )

    parser.add_argument(
        "--max-distance-km",
        type=float,
        default=8.0,
        help="Maximum distance for neighbor pairs in kilometers",
    )

    parser.add_argument(
        "--batch-size", type=int, default=100, help="Batch size for processing"
    )

    parser.add_argument(
        "--max-workers", type=int, default=4, help="Maximum concurrent workers"
    )

    parser.add_argument("--config", type=str, help="Path to .env configuration file")

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate processing without loading data",
    )

    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")

    parser.add_argument(
        "--resume-job", type=str, help="Resume a previous backfill job by job ID"
    )

    return parser.parse_args()


def validate_date_range(
    start_date_str: str, end_date_str: str
) -> Tuple[datetime, datetime]:
    """Validate and parse date range."""

    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"Invalid date format. Use YYYY-MM-DD: {e}")

    if start_date > end_date:
        raise ValueError("Start date must be before end date")

    if end_date > datetime.now():
        raise ValueError("End date cannot be in the future")

    # Limit backfill range to prevent excessive processing
    max_days = 90
    if (end_date - start_date).days > max_days:
        raise ValueError(f"Date range too large. Maximum {max_days} days allowed")

    return start_date, end_date


def get_hex_pairs_to_process(
    specific_pairs: Optional[List[str]] = None,
    sample_rate: float = 1.0,
    max_distance_km: float = 8.0,
) -> List[Tuple[str, str]]:
    """Get list of hex pairs to process."""

    if specific_pairs:
        # Parse specific hex pairs
        hex_pairs = []
        for pair_str in specific_pairs:
            try:
                from_hex, to_hex = pair_str.split(",")
                hex_pairs.append((from_hex.strip(), to_hex.strip()))
            except ValueError:
                raise ValueError(
                    f"Invalid hex pair format: {pair_str}. Use 'from_hex,to_hex'"
                )

        logger.info(f"Using {len(hex_pairs)} specific hex pairs")
        return hex_pairs

    else:
        # Get hex pairs from city grid
        from etl.common import get_core_connection

        connection = get_core_connection()
        grid_query = """
            SELECT hex_id, centroid_lat, centroid_lng 
            FROM h3_lookup 
            WHERE city = %s AND resolution = %s
        """

        grid_results = connection.execute_query(
            grid_query,
            params={"city": config.h3.city_name, "resolution": config.h3.resolution},
            fetch=True,
        )

        if not grid_results:
            raise ValueError(f"No H3 grid found for {config.h3.city_name}")

        hex_ids = [row["HEX_ID"] for row in grid_results]

        # Generate neighbor pairs
        neighbor_discovery = get_neighbor_discovery()
        all_pairs = neighbor_discovery.get_all_neighbor_pairs(
            hex_ids=hex_ids, max_distance_km=max_distance_km, bidirectional=True
        )

        # Apply sampling
        if sample_rate < 1.0:
            import random

            random.seed(42)  # Deterministic sampling
            sample_size = int(len(all_pairs) * sample_rate)
            all_pairs = random.sample(all_pairs, sample_size)

        logger.info(f"Generated {len(all_pairs)} hex pairs (sample_rate={sample_rate})")
        return all_pairs


def generate_time_schedule(
    start_date: datetime, end_date: datetime, specific_hours: Optional[List[int]] = None
) -> List[datetime]:
    """Generate schedule of timestamps to process."""

    if specific_hours:
        hours = specific_hours
    else:
        # Default: sample key hours throughout the day
        hours = [6, 9, 12, 15, 18, 21]  # 6am, 9am, 12pm, 3pm, 6pm, 9pm

    schedule = []
    current_date = start_date

    while current_date <= end_date:
        for hour in hours:
            timestamp = current_date.replace(
                hour=hour, minute=0, second=0, microsecond=0
            )
            schedule.append(timestamp)

        current_date += timedelta(days=1)

    return schedule


def process_batch(
    hex_pairs_batch: List[Tuple[str, str]],
    timestamp: datetime,
    providers: List[str],
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Process a batch of hex pairs for a specific timestamp."""

    batch_id = f"backfill_{timestamp.strftime('%Y%m%d_%H')}_{len(hex_pairs_batch)}pairs"

    try:
        with TimedLogger(logger, f"process_batch: {batch_id}"):
            # Get hex-to-coordinate mapping
            from etl.common import get_core_connection

            connection = get_core_connection()
            unique_hexes = list(
                set([hex_id for pair in hex_pairs_batch for hex_id in pair])
            )

            if len(unique_hexes) > 1000:  # Batch query for efficiency
                placeholders = ",".join(["%s"] * len(unique_hexes))
                grid_query = f"""
                    SELECT hex_id, centroid_lat, centroid_lng 
                    FROM h3_lookup 
                    WHERE hex_id IN ({placeholders})
                """
                grid_results = connection.execute_query(
                    grid_query, unique_hexes, fetch=True
                )
            else:
                grid_results = []
                for hex_id in unique_hexes:
                    result = connection.execute_query(
                        "SELECT hex_id, centroid_lat, centroid_lng FROM h3_lookup WHERE hex_id = %s",
                        [hex_id],
                        fetch=True,
                    )
                    grid_results.extend(result)

            hex_to_latlng = {
                row["HEX_ID"]: (row["CENTROID_LAT"], row["CENTROID_LNG"])
                for row in grid_results
            }

            # Get routes from providers
            route_results = []

            for provider in providers:
                try:
                    client = create_distance_matrix_client()

                    # Override provider preference
                    client.primary_provider = provider
                    client.fallback_providers = []

                    batch_routes = client.get_hex_pair_routes(
                        hex_pairs=hex_pairs_batch,
                        hex_to_latlng=hex_to_latlng,
                        depart_time=timestamp,
                        preferred_provider=provider,
                    )

                    route_results.extend(batch_routes)

                except Exception as e:
                    logger.warning(
                        f"Provider {provider} failed for batch {batch_id}: {e}"
                    )
                    continue

            # Load to Snowflake
            if route_results and not dry_run:
                from etl.load import RoutesRawLoader

                loader = RoutesRawLoader()
                load_result = loader.load_route_results(
                    route_results, batch_id=batch_id
                )

                return {
                    "batch_id": batch_id,
                    "timestamp": timestamp.isoformat(),
                    "hex_pairs": len(hex_pairs_batch),
                    "routes_retrieved": len(route_results),
                    "routes_loaded": load_result.records_loaded,
                    "routes_failed": load_result.records_failed,
                    "success": True,
                }
            else:
                return {
                    "batch_id": batch_id,
                    "timestamp": timestamp.isoformat(),
                    "hex_pairs": len(hex_pairs_batch),
                    "routes_retrieved": len(route_results),
                    "routes_loaded": 0 if dry_run else len(route_results),
                    "routes_failed": 0,
                    "success": True,
                    "dry_run": dry_run,
                }

    except Exception as e:
        logger.error(f"Batch processing failed for {batch_id}: {e}")
        return {
            "batch_id": batch_id,
            "timestamp": timestamp.isoformat(),
            "hex_pairs": len(hex_pairs_batch),
            "routes_retrieved": 0,
            "routes_loaded": 0,
            "routes_failed": len(hex_pairs_batch),
            "success": False,
            "error": str(e),
        }


def run_backfill(
    start_date: datetime,
    end_date: datetime,
    hex_pairs: List[Tuple[str, str]],
    providers: List[str],
    batch_size: int = 100,
    max_workers: int = 4,
    specific_hours: Optional[List[int]] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Run the backfill process."""

    # Create backfill job
    backfill_job = create_backfill_job(
        start_date=start_date,
        end_date=end_date,
        hex_pairs=hex_pairs[:10]
        if len(hex_pairs) > 10
        else hex_pairs,  # Sample for metadata
        providers=providers,
    )

    if not dry_run:
        state_store = get_state_store()
        state_store.save_backfill_job(backfill_job)

    # Generate processing schedule
    schedule = generate_time_schedule(start_date, end_date, specific_hours)

    # Create batches
    batches = [
        hex_pairs[i : i + batch_size] for i in range(0, len(hex_pairs), batch_size)
    ]

    total_tasks = len(schedule) * len(batches)
    logger.info(
        f"Starting backfill: {len(schedule)} timestamps × {len(batches)} batches = {total_tasks} tasks"
    )

    # Process with concurrent workers
    results = []
    completed_tasks = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_task = {}

        for timestamp in schedule:
            for batch_idx, hex_pairs_batch in enumerate(batches):
                future = executor.submit(
                    process_batch,
                    hex_pairs_batch=hex_pairs_batch,
                    timestamp=timestamp,
                    providers=providers,
                    dry_run=dry_run,
                )
                future_to_task[future] = (timestamp, batch_idx)

        # Collect results
        for future in concurrent.futures.as_completed(future_to_task):
            try:
                result = future.get()
                results.append(result)
                completed_tasks += 1

                # Progress reporting
                if completed_tasks % 10 == 0 or completed_tasks == total_tasks:
                    progress = (completed_tasks / total_tasks) * 100
                    logger.info(
                        f"Progress: {completed_tasks}/{total_tasks} ({progress:.1f}%)"
                    )

            except Exception as e:
                timestamp, batch_idx = future_to_task[future]
                logger.error(f"Task failed for {timestamp} batch {batch_idx}: {e}")
                results.append(
                    {
                        "timestamp": timestamp.isoformat(),
                        "batch_idx": batch_idx,
                        "success": False,
                        "error": str(e),
                    }
                )

    # Calculate summary statistics
    successful_results = [r for r in results if r.get("success", False)]
    total_routes_retrieved = sum(
        r.get("routes_retrieved", 0) for r in successful_results
    )
    total_routes_loaded = sum(r.get("routes_loaded", 0) for r in successful_results)
    total_routes_failed = sum(r.get("routes_failed", 0) for r in successful_results)

    summary = {
        "backfill_job_id": backfill_job.job_id,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "providers": providers,
        "total_tasks": total_tasks,
        "successful_tasks": len(successful_results),
        "failed_tasks": total_tasks - len(successful_results),
        "total_hex_pairs": len(hex_pairs),
        "total_routes_retrieved": total_routes_retrieved,
        "total_routes_loaded": total_routes_loaded,
        "total_routes_failed": total_routes_failed,
        "success_rate": len(successful_results) / total_tasks if total_tasks > 0 else 0,
        "dry_run": dry_run,
    }

    return summary


def main():
    """Main execution function."""

    try:
        # Parse arguments
        args = parse_arguments()

        # Configure logging
        if args.verbose:
            import logging

            logging.getLogger().setLevel(logging.DEBUG)

        # Load custom config if provided
        if args.config:
            from dotenv import load_dotenv

            load_dotenv(args.config)
            logger.info(f"Loaded configuration from {args.config}")

        logger.info("Starting ETA data backfill")

        # Validate date range
        start_date, end_date = validate_date_range(args.start_date, args.end_date)

        # Parse providers
        providers = [p.strip() for p in args.providers.split(",")]
        valid_providers = ["osrm", "google", "here"]
        invalid_providers = [p for p in providers if p not in valid_providers]
        if invalid_providers:
            raise ValueError(
                f"Invalid providers: {invalid_providers}. Valid options: {valid_providers}"
            )

        # Parse specific hours if provided
        specific_hours = None
        if args.hours:
            try:
                specific_hours = [int(h.strip()) for h in args.hours.split(",")]
                invalid_hours = [h for h in specific_hours if not (0 <= h <= 23)]
                if invalid_hours:
                    raise ValueError(f"Invalid hours: {invalid_hours}. Must be 0-23")
            except ValueError as e:
                raise ValueError(f"Invalid hours format: {e}")

        # Get hex pairs to process
        hex_pairs = get_hex_pairs_to_process(
            specific_pairs=args.hex_pairs,
            sample_rate=args.sample_rate,
            max_distance_km=args.max_distance_km,
        )

        # Print configuration summary
        print(f"\n📋 Backfill Configuration:")
        print(f"   Date Range: {start_date.date()} to {end_date.date()}")
        print(f"   Providers: {', '.join(providers)}")
        print(f"   Hex Pairs: {len(hex_pairs):,}")
        print(f"   Hours: {specific_hours or 'Default (6,9,12,15,18,21)'}")
        print(f"   Batch Size: {args.batch_size}")
        print(f"   Max Workers: {args.max_workers}")
        print(f"   Dry Run: {args.dry_run}")

        # Confirm before proceeding
        if not args.dry_run:
            response = input("\nProceed with backfill? (y/N): ")
            if response.lower() != "y":
                print("Backfill cancelled")
                return

        # Run backfill
        summary = run_backfill(
            start_date=start_date,
            end_date=end_date,
            hex_pairs=hex_pairs,
            providers=providers,
            batch_size=args.batch_size,
            max_workers=args.max_workers,
            specific_hours=specific_hours,
            dry_run=args.dry_run,
        )

        # Print results
        print(f"\n📊 Backfill Results:")
        print(f"   Job ID: {summary['backfill_job_id']}")
        print(f"   Total Tasks: {summary['total_tasks']:,}")
        print(f"   Successful: {summary['successful_tasks']:,}")
        print(f"   Failed: {summary['failed_tasks']:,}")
        print(f"   Success Rate: {summary['success_rate']:.1%}")
        print(f"   Routes Retrieved: {summary['total_routes_retrieved']:,}")
        print(f"   Routes Loaded: {summary['total_routes_loaded']:,}")

        if summary["failed_tasks"] > 0:
            print(
                f"\n⚠️  {summary['failed_tasks']} tasks failed. Check logs for details."
            )

        if args.dry_run:
            print(f"\n✅ DRY RUN completed successfully")
        else:
            print(f"\n✅ Backfill completed successfully")

        logger.info("ETA backfill completed", extra=summary)

    except KeyboardInterrupt:
        print("\n⚠️  Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Backfill failed: {e}")
        print(f"\n❌ ERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
