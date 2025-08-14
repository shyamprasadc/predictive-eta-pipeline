"""
Airflow DAG for Predictive ETA Calculator ETL Pipeline.

This DAG orchestrates the complete ETL process including:
1. Bootstrap city grid (idempotent)
2. Ingest distance matrix data from routing providers
3. Transform and aggregate data by time slabs
4. Run dbt models for staging and marts
5. Publish final ETA data
6. Data quality tests and reporting

Default schedule: Daily at 2 AM UTC
Supports manual triggers and backfill operations.
"""

import os
import sys
from datetime import datetime, timedelta
from typing import Dict, Any, List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.configuration import conf

# Add ETL package to Python path
sys.path.insert(0, "/opt/airflow/dags/repo/etl")

# ETL imports
from etl.common import config, get_logger, TimedLogger
from etl.h3 import create_city_grid_from_config, get_neighbor_discovery
from etl.ingest import create_distance_matrix_client, RouteResult
from etl.transform import create_time_slab_aggregator
from etl.load import BatchLoader
from etl.state import get_state_store, create_processing_job, ProcessingStatus

# Initialize logger
logger = get_logger("airflow.dag_predictive_eta")

# DAG configuration
DAG_ID = "dag_predictive_eta"
DESCRIPTION = "Predictive ETA Calculator ETL Pipeline"

# Default arguments
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

# Schedule interval - daily at 2 AM UTC
SCHEDULE_INTERVAL = "0 2 * * *"

# Task timeout
TASK_TIMEOUT = timedelta(hours=4)


def bootstrap_city_grid(**context) -> Dict[str, Any]:
    """
    Bootstrap H3 city grid (idempotent operation).

    Returns:
        Dict with operation results
    """
    execution_date = context["execution_date"]
    job_id = f"bootstrap_grid_{execution_date.strftime('%Y%m%d_%H%M%S')}"

    with TimedLogger(logger, "bootstrap_city_grid"):
        try:
            # Create processing job state
            state_store = get_state_store()
            job_state = create_processing_job(job_id, "bootstrap_grid")
            job_state.status = ProcessingStatus.RUNNING
            state_store.save_processing_state(job_state)

            # Generate city grid
            grid_df = create_city_grid_from_config()
            logger.info(f"Generated grid with {len(grid_df)} hexes")

            # Load to Snowflake
            from etl.load import H3LookupLoader

            loader = H3LookupLoader()
            result = loader.load_h3_grid(grid_df, replace_existing=True)

            # Update job state
            job_state.status = ProcessingStatus.COMPLETED
            job_state.processed_records = result.records_loaded
            job_state.failed_records = result.records_failed
            job_state.end_time = datetime.utcnow()
            state_store.save_processing_state(job_state)

            return {
                "job_id": job_id,
                "hexes_loaded": result.records_loaded,
                "success": True,
            }

        except Exception as e:
            logger.error(f"Bootstrap failed: {e}")
            job_state.status = ProcessingStatus.FAILED
            job_state.error_message = str(e)
            job_state.end_time = datetime.utcnow()
            state_store.save_processing_state(job_state)
            raise


def ingest_distance_matrix(**context) -> Dict[str, Any]:
    """
    Ingest distance matrix data from routing providers.

    Returns:
        Dict with ingestion results
    """
    execution_date = context["execution_date"]
    job_id = f"ingest_matrix_{execution_date.strftime('%Y%m%d_%H%M%S')}"

    with TimedLogger(logger, "ingest_distance_matrix"):
        try:
            # Get configuration
            batch_size = Variable.get(
                "eta_batch_size", default_var=config.eta.batch_size
            )
            max_concurrent = Variable.get(
                "eta_max_concurrent", default_var=config.eta.max_concurrent_requests
            )

            # Create processing job state
            state_store = get_state_store()
            job_state = create_processing_job(
                job_id,
                "distance_matrix_ingestion",
                metadata={
                    "execution_date": execution_date.isoformat(),
                    "batch_size": batch_size,
                    "max_concurrent": max_concurrent,
                },
            )
            job_state.status = ProcessingStatus.RUNNING
            state_store.save_processing_state(job_state)

            # Get H3 grid for the city
            from etl.common import get_core_connection

            connection = get_core_connection()
            grid_query = """
                SELECT hex_id, centroid_lat, centroid_lng 
                FROM h3_lookup 
                WHERE city = %s AND resolution = %s
            """
            grid_results = connection.execute_query(
                grid_query,
                params={
                    "city": config.h3.city_name,
                    "resolution": config.h3.resolution,
                },
                fetch=True,
            )

            if not grid_results:
                raise ValueError(f"No H3 grid found for {config.h3.city_name}")

            # Create hex-to-coordinate mapping
            hex_to_latlng = {
                row["HEX_ID"]: (row["CENTROID_LAT"], row["CENTROID_LNG"])
                for row in grid_results
            }

            # Generate neighbor pairs (sample for demo - customize based on needs)
            neighbor_discovery = get_neighbor_discovery()
            hex_ids = list(hex_to_latlng.keys())

            # For production, you might want to:
            # 1. Use a subset for daily updates
            # 2. Implement intelligent sampling
            # 3. Focus on high-traffic routes
            sample_hex_pairs = []
            for i, from_hex in enumerate(hex_ids[:50]):  # Sample first 50 hexes
                neighbors = neighbor_discovery.get_neighbors_3km(from_hex)
                for to_hex in list(neighbors)[:10]:  # Sample first 10 neighbors
                    if to_hex in hex_to_latlng:
                        sample_hex_pairs.append((from_hex, to_hex))

            logger.info(f"Processing {len(sample_hex_pairs)} hex pairs")

            # Get routes from providers
            client = create_distance_matrix_client()
            depart_time = execution_date.replace(
                hour=8, minute=0, second=0, microsecond=0
            )  # 8 AM

            route_results = client.get_hex_pair_routes(
                hex_pairs=sample_hex_pairs,
                hex_to_latlng=hex_to_latlng,
                depart_time=depart_time,
            )

            logger.info(f"Retrieved {len(route_results)} route results")

            # Load to Snowflake
            if route_results:
                from etl.load import RoutesRawLoader

                loader = RoutesRawLoader()
                load_result = loader.load_route_results(route_results, batch_id=job_id)

                # Update job state
                job_state.status = ProcessingStatus.COMPLETED
                job_state.processed_records = load_result.records_loaded
                job_state.failed_records = load_result.records_failed
                job_state.end_time = datetime.utcnow()
                state_store.save_processing_state(job_state)

                return {
                    "job_id": job_id,
                    "routes_processed": len(route_results),
                    "routes_loaded": load_result.records_loaded,
                    "success": True,
                }
            else:
                job_state.status = ProcessingStatus.COMPLETED
                job_state.end_time = datetime.utcnow()
                state_store.save_processing_state(job_state)

                return {
                    "job_id": job_id,
                    "routes_processed": 0,
                    "routes_loaded": 0,
                    "success": True,
                }

        except Exception as e:
            logger.error(f"Distance matrix ingestion failed: {e}")
            job_state.status = ProcessingStatus.FAILED
            job_state.error_message = str(e)
            job_state.end_time = datetime.utcnow()
            state_store.save_processing_state(job_state)
            raise


def transform_hourly_agg(**context) -> Dict[str, Any]:
    """
    Transform raw routes into hourly aggregations.

    Returns:
        Dict with transformation results
    """
    execution_date = context["execution_date"]
    job_id = f"transform_hourly_{execution_date.strftime('%Y%m%d_%H%M%S')}"

    with TimedLogger(logger, "transform_hourly_agg"):
        try:
            # Create processing job state
            state_store = get_state_store()
            job_state = create_processing_job(job_id, "hourly_aggregation")
            job_state.status = ProcessingStatus.RUNNING
            state_store.save_processing_state(job_state)

            # Transform using SQL in Snowflake for better performance
            from etl.common import get_core_connection

            connection = get_core_connection()

            transform_sql = """
                CREATE OR REPLACE TABLE CORE.ETA_HOURLY AS
                SELECT 
                    from_hex,
                    to_hex,
                    weekday,
                    hour,
                    MIN(duration_s) as min_eta_s,
                    MAX(duration_s) as max_eta_s,
                    ROUND(AVG(duration_s), 1) as avg_eta_s,
                    ROUND(MEDIAN(duration_s), 1) as median_eta_s,
                    COUNT(*) as sample_count,
                    MAX(provider) as provider_pref,
                    CURRENT_TIMESTAMP() as updated_at
                FROM RAW.ROUTES_RAW
                WHERE depart_ts >= DATEADD('day', -1, CURRENT_DATE())
                  AND from_hex IS NOT NULL 
                  AND to_hex IS NOT NULL
                  AND duration_s > 0
                GROUP BY from_hex, to_hex, weekday, hour
                HAVING COUNT(*) >= 1
            """

            connection.execute_query(transform_sql)

            # Get record count
            count_result = connection.execute_query(
                "SELECT COUNT(*) as count FROM CORE.ETA_HOURLY", fetch=True
            )
            record_count = count_result[0]["COUNT"] if count_result else 0

            # Update job state
            job_state.status = ProcessingStatus.COMPLETED
            job_state.processed_records = record_count
            job_state.end_time = datetime.utcnow()
            state_store.save_processing_state(job_state)

            return {
                "job_id": job_id,
                "hourly_aggregations": record_count,
                "success": True,
            }

        except Exception as e:
            logger.error(f"Hourly aggregation failed: {e}")
            job_state.status = ProcessingStatus.FAILED
            job_state.error_message = str(e)
            job_state.end_time = datetime.utcnow()
            state_store.save_processing_state(job_state)
            raise


def publish_report(**context) -> Dict[str, Any]:
    """
    Publish run metrics and summary report.

    Returns:
        Dict with report results
    """
    execution_date = context["execution_date"]

    with TimedLogger(logger, "publish_report"):
        try:
            # Get processing states for this run
            state_store = get_state_store()

            # Query final table statistics
            from etl.common import get_core_connection

            connection = get_core_connection()

            stats_sql = """
                SELECT 
                    COUNT(*) as total_hex_pairs,
                    COUNT(DISTINCT from_hex) as unique_from_hexes,
                    COUNT(DISTINCT to_hex) as unique_to_hexes,
                    SUM(sample_count) as total_samples,
                    AVG(sample_count) as avg_samples_per_pair,
                    MIN(min_eta_s) as fastest_eta_s,
                    MAX(max_eta_s) as slowest_eta_s,
                    AVG(avg_eta_s) as overall_avg_eta_s
                FROM CORE.ETA_SLABS
                WHERE updated_at >= CURRENT_DATE()
            """

            stats_result = connection.execute_query(stats_sql, fetch=True)
            stats = stats_result[0] if stats_result else {}

            # Create report
            report = {
                "execution_date": execution_date.isoformat(),
                "pipeline_status": "SUCCESS",
                "statistics": stats,
                "timestamp": datetime.utcnow().isoformat(),
            }

            logger.info("Pipeline execution report", extra=report)

            # Store report in Airflow Variable for external access
            Variable.set(
                f"eta_pipeline_report_{execution_date.strftime('%Y%m%d')}",
                report,
                serialize_json=True,
            )

            return report

        except Exception as e:
            logger.error(f"Report publishing failed: {e}")
            error_report = {
                "execution_date": execution_date.isoformat(),
                "pipeline_status": "FAILED",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat(),
            }

            Variable.set(
                f"eta_pipeline_report_{execution_date.strftime('%Y%m%d')}",
                error_report,
                serialize_json=True,
            )

            raise


# Create the DAG
dag = DAG(
    DAG_ID,
    default_args=default_args,
    description=DESCRIPTION,
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=False,
    max_active_runs=1,
    tags=["etl", "predictive-eta", "production"],
    doc_md=__doc__,
)

# Task 1: Bootstrap city grid (idempotent)
bootstrap_grid_task = PythonOperator(
    task_id="bootstrap_city_grid",
    python_callable=bootstrap_city_grid,
    timeout=timedelta(minutes=30),
    dag=dag,
    doc_md="""
    Bootstrap H3 city grid into CORE.H3_LOOKUP table.
    This is an idempotent operation that ensures the grid exists.
    """,
)

# Task 2: Ingest distance matrix
ingest_matrix_task = PythonOperator(
    task_id="ingest_distance_matrix",
    python_callable=ingest_distance_matrix,
    timeout=TASK_TIMEOUT,
    dag=dag,
    doc_md="""
    Ingest routing data from providers (OSRM, Google, HERE) for hex pairs.
    Stores raw route data in RAW.ROUTES_RAW table.
    """,
)

# Task 3: Transform hourly aggregations
transform_hourly_task = PythonOperator(
    task_id="transform_hourly_agg",
    python_callable=transform_hourly_agg,
    timeout=timedelta(hours=1),
    dag=dag,
    doc_md="""
    Aggregate raw routing data by hour to create CORE.ETA_HOURLY table.
    """,
)

# Task Group: dbt transformations
with TaskGroup("dbt_transformations", dag=dag) as dbt_group:
    # dbt deps
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="cd /opt/airflow/dags/repo/dbt && dbt deps",
        timeout=timedelta(minutes=10),
    )

    # dbt run staging models
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command="cd /opt/airflow/dags/repo/dbt && dbt run --select staging",
        timeout=timedelta(minutes=30),
    )

    # dbt run marts models
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command="cd /opt/airflow/dags/repo/dbt && dbt run --select marts",
        timeout=timedelta(hours=1),
    )

    # dbt test
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dags/repo/dbt && dbt test",
        timeout=timedelta(minutes=30),
    )

    # Set dependencies within dbt group
    dbt_deps >> dbt_run_staging >> dbt_run_marts >> dbt_test

# Task: Generate final ETA_SLABS table
generate_eta_slabs = SnowflakeOperator(
    task_id="generate_eta_slabs",
    snowflake_conn_id="snowflake_default",
    sql="""
        CREATE OR REPLACE TABLE CORE.ETA_SLABS AS
        SELECT 
            from_hex,
            to_hex,
            weekday,
            time_slab as slab,
            min_eta_s,
            max_eta_s,
            rain_eta_s,
            primary_provider as provider_pref,
            sample_count,
            updated_at
        FROM CORE.FCT_ETA_HEX_PAIR
        WHERE data_quality_score >= 40
          AND sample_count >= 5;
        
        ALTER TABLE CORE.ETA_SLABS 
        CLUSTER BY (from_hex, to_hex, weekday, slab);
    """,
    timeout=timedelta(minutes=30),
    dag=dag,
)

# Task: Publish report
publish_report_task = PythonOperator(
    task_id="publish_report",
    python_callable=publish_report,
    timeout=timedelta(minutes=10),
    dag=dag,
    doc_md="""
    Generate and publish pipeline execution report with metrics.
    """,
)

# Define task dependencies
bootstrap_grid_task >> ingest_matrix_task >> transform_hourly_task
transform_hourly_task >> dbt_group
dbt_group >> generate_eta_slabs >> publish_report_task

# Add task documentation
dag.doc_md = f"""
# Predictive ETA Calculator ETL Pipeline

This DAG orchestrates the complete ETL process for the Predictive ETA Calculator system.

## Pipeline Flow
1. **Bootstrap City Grid**: Ensure H3 hexagonal grid exists for the target city
2. **Ingest Distance Matrix**: Fetch routing data from providers (OSRM/Google/HERE)  
3. **Transform Hourly**: Aggregate raw routes into hourly statistics
4. **dbt Transformations**: Run staging and mart models with data quality tests
5. **Generate ETA Slabs**: Create final serving table with time slab aggregations
6. **Publish Report**: Generate metrics and status report

## Configuration
- **Schedule**: Daily at 2 AM UTC
- **Retries**: 2 with exponential backoff
- **Timeout**: 4 hours maximum
- **Concurrency**: 1 active run at a time

## Monitoring
Check Airflow Variables for execution reports:
- `eta_pipeline_report_YYYYMMDD`

## Manual Triggers
This DAG supports manual execution and backfilling for historical date ranges.
"""
