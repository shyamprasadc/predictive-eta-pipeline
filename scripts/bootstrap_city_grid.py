#!/usr/bin/env python3
"""
Bootstrap script for setting up H3 city grid in Snowflake.

This script generates the H3 hexagonal grid for a specified city and loads it
into the CORE.H3_LOOKUP table. It's designed to be idempotent and can be run
multiple times safely.

Usage:
    python bootstrap_city_grid.py --city Dubai
    python bootstrap_city_grid.py --city "New York" --resolution 6
    python bootstrap_city_grid.py --config /path/to/config/.env
"""

import sys
import argparse
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

# Add ETL package to path
sys.path.insert(0, str(Path(__file__).parent.parent / "etl"))

from etl.common import config, get_logger, TimedLogger
from etl.h3 import H3GridGenerator, CityBounds, create_city_grid_from_config
from etl.load import H3LookupLoader
from etl.state import get_state_store, create_processing_job, ProcessingStatus

logger = get_logger("bootstrap_city_grid")


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Bootstrap H3 city grid for Predictive ETA Calculator",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument("--city", type=str, help="City name (overrides config)")

    parser.add_argument(
        "--bbox",
        type=str,
        help='Bounding box as "min_lat,min_lng,max_lat,max_lng" (overrides config)',
    )

    parser.add_argument(
        "--resolution", type=int, default=7, help="H3 resolution level (0-15)"
    )

    parser.add_argument("--config", type=str, help="Path to .env configuration file")

    parser.add_argument(
        "--replace", action="store_true", help="Replace existing grid data for the city"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate grid but do not load to Snowflake",
    )

    parser.add_argument("--output", type=str, help="Save grid to CSV file")

    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")

    return parser.parse_args()


def get_predefined_city_bounds(city_name: str) -> Optional[CityBounds]:
    """Get predefined bounding boxes for common cities."""

    city_bounds = {
        "dubai": CityBounds(min_lat=24.9, min_lng=54.8, max_lat=25.4, max_lng=55.6),
        "new york": CityBounds(
            min_lat=40.4774, min_lng=-74.2591, max_lat=40.9176, max_lng=-73.7004
        ),
        "london": CityBounds(
            min_lat=51.2868, min_lng=-0.5103, max_lat=51.6918, max_lng=0.3340
        ),
        "singapore": CityBounds(
            min_lat=1.1304, min_lng=103.6026, max_lat=1.4784, max_lng=104.0120
        ),
        "paris": CityBounds(
            min_lat=48.8155, min_lng=2.2241, max_lat=48.9021, max_lng=2.4699
        ),
        "tokyo": CityBounds(
            min_lat=35.5322, min_lng=139.3796, max_lat=35.8177, max_lng=139.9190
        ),
        "san francisco": CityBounds(
            min_lat=37.7081, min_lng=-122.5125, max_lat=37.8199, max_lng=-122.3574
        ),
    }

    return city_bounds.get(city_name.lower())


def validate_inputs(args) -> Dict[str, Any]:
    """Validate and prepare inputs for grid generation."""

    # Determine city and bounds
    if args.city:
        city_name = args.city

        if args.bbox:
            # Parse custom bounding box
            try:
                bbox_values = [float(x.strip()) for x in args.bbox.split(",")]
                if len(bbox_values) != 4:
                    raise ValueError("Bounding box must have 4 values")
                city_bounds = CityBounds.from_bbox(bbox_values)
            except Exception as e:
                raise ValueError(f"Invalid bounding box format: {e}")
        else:
            # Try predefined city bounds
            city_bounds = get_predefined_city_bounds(city_name)
            if not city_bounds:
                raise ValueError(
                    f"No predefined bounds for city '{city_name}'. Please provide --bbox"
                )
    else:
        # Use configuration
        city_name = config.h3.city_name
        city_bounds = CityBounds.from_bbox(config.h3.city_bbox)

    # Validate resolution
    if not (0 <= args.resolution <= 15):
        raise ValueError(
            f"H3 resolution must be between 0 and 15, got {args.resolution}"
        )

    return {
        "city_name": city_name,
        "city_bounds": city_bounds,
        "resolution": args.resolution,
        "replace_existing": args.replace,
        "dry_run": args.dry_run,
        "output_file": args.output,
    }


def generate_city_grid(
    city_name: str, city_bounds: CityBounds, resolution: int
) -> Dict[str, Any]:
    """Generate H3 grid for the specified city."""

    with TimedLogger(logger, f"generate_city_grid for {city_name}"):
        # Create grid generator
        generator = H3GridGenerator(resolution=resolution)

        # Generate grid
        grid_df = generator.generate_city_grid(
            city_bounds=city_bounds, city_name=city_name, include_border_hexes=True
        )

        # Get some statistics
        center_lat, center_lng = city_bounds.center()
        center_hex = generator.latlng_to_hex(center_lat, center_lng)

        stats = {
            "city_name": city_name,
            "resolution": resolution,
            "total_hexes": len(grid_df),
            "bounds": {
                "min_lat": city_bounds.min_lat,
                "min_lng": city_bounds.min_lng,
                "max_lat": city_bounds.max_lat,
                "max_lng": city_bounds.max_lng,
            },
            "center_hex": center_hex,
            "hex_area_km2": generator.get_hex_area_km2(center_hex),
            "hex_edge_length_km": generator.get_hex_edge_length_km(center_hex),
        }

        logger.info("Grid generation complete", extra=stats)

        return {"grid_df": grid_df, "stats": stats}


def save_grid_to_csv(grid_df, output_file: str):
    """Save grid DataFrame to CSV file."""

    with TimedLogger(logger, f"save_grid_to_csv: {output_file}"):
        # Ensure output directory exists
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Save to CSV
        grid_df.to_csv(output_path, index=False)

        logger.info(f"Grid saved to {output_path}")


def load_grid_to_snowflake(
    grid_df, city_name: str, replace_existing: bool
) -> Dict[str, Any]:
    """Load grid DataFrame to Snowflake."""

    with TimedLogger(logger, f"load_grid_to_snowflake: {city_name}"):
        try:
            # Create loader
            loader = H3LookupLoader()

            # Load grid
            result = loader.load_h3_grid(
                grid_df=grid_df, replace_existing=replace_existing
            )

            logger.info(
                "Grid loaded to Snowflake",
                extra={
                    "records_loaded": result.records_loaded,
                    "records_failed": result.records_failed,
                    "operation_type": result.operation_type,
                },
            )

            return {
                "success": True,
                "records_loaded": result.records_loaded,
                "records_failed": result.records_failed,
                "operation_type": result.operation_type,
            }

        except Exception as e:
            logger.error(f"Failed to load grid to Snowflake: {e}")
            return {"success": False, "error": str(e)}


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

        logger.info("Starting city grid bootstrap")

        # Validate inputs
        inputs = validate_inputs(args)

        # Create processing job for tracking
        job_id = f"bootstrap_grid_{inputs['city_name']}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

        if not args.dry_run:
            state_store = get_state_store()
            job_state = create_processing_job(
                job_id=job_id,
                job_type="bootstrap_grid",
                metadata={
                    "city_name": inputs["city_name"],
                    "resolution": inputs["resolution"],
                    "replace_existing": inputs["replace_existing"],
                    "script_args": vars(args),
                },
            )
            job_state.status = ProcessingStatus.RUNNING
            state_store.save_processing_state(job_state)

        # Generate grid
        grid_result = generate_city_grid(
            city_name=inputs["city_name"],
            city_bounds=inputs["city_bounds"],
            resolution=inputs["resolution"],
        )

        grid_df = grid_result["grid_df"]
        stats = grid_result["stats"]

        # Save to CSV if requested
        if inputs["output_file"]:
            save_grid_to_csv(grid_df, inputs["output_file"])

        # Load to Snowflake unless dry run
        if not inputs["dry_run"]:
            load_result = load_grid_to_snowflake(
                grid_df=grid_df,
                city_name=inputs["city_name"],
                replace_existing=inputs["replace_existing"],
            )

            # Update job state
            if load_result["success"]:
                job_state.status = ProcessingStatus.COMPLETED
                job_state.processed_records = load_result["records_loaded"]
                job_state.failed_records = load_result["records_failed"]
            else:
                job_state.status = ProcessingStatus.FAILED
                job_state.error_message = load_result["error"]

            job_state.end_time = datetime.utcnow()
            state_store.save_processing_state(job_state)

            # Print results
            if load_result["success"]:
                print(
                    f"\n✅ SUCCESS: Loaded {load_result['records_loaded']} hexes for {inputs['city_name']}"
                )
            else:
                print(f"\n❌ FAILED: {load_result['error']}")
                sys.exit(1)
        else:
            print(
                f"\n✅ DRY RUN: Generated {len(grid_df)} hexes for {inputs['city_name']}"
            )

        # Print statistics
        print(f"\n📊 Grid Statistics:")
        print(f"   City: {stats['city_name']}")
        print(f"   Resolution: {stats['resolution']}")
        print(f"   Total Hexes: {stats['total_hexes']:,}")
        print(f"   Hex Area: {stats['hex_area_km2']:.2f} km²")
        print(f"   Hex Edge Length: {stats['hex_edge_length_km']:.2f} km")
        print(f"   Center Hex: {stats['center_hex']}")

        bounds = stats["bounds"]
        print(
            f"   Bounds: ({bounds['min_lat']:.4f}, {bounds['min_lng']:.4f}) to ({bounds['max_lat']:.4f}, {bounds['max_lng']:.4f})"
        )

        logger.info("City grid bootstrap completed successfully")

    except KeyboardInterrupt:
        print("\n⚠️  Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Bootstrap failed: {e}")
        print(f"\n❌ ERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
