"""
State management for the Predictive ETA Calculator pipeline.

Provides backfill tracking, watermark management, and processing state persistence
to enable reliable ETL operations with restart capabilities.
"""

import json
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from enum import Enum

from ..common import config, get_logger, TimedLogger, get_core_connection

logger = get_logger("state.state_store")


class ProcessingStatus(Enum):
    """Processing status enumeration."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class ProcessingState:
    """State information for a processing job."""

    job_id: str
    job_type: str  # 'daily_etl', 'backfill', 'bootstrap'
    status: ProcessingStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    processed_records: int = 0
    failed_records: int = 0
    metadata: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        result = asdict(self)
        result["status"] = self.status.value
        result["start_time"] = self.start_time.isoformat()
        if self.end_time:
            result["end_time"] = self.end_time.isoformat()
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ProcessingState":
        """Create from dictionary."""
        data = data.copy()
        data["status"] = ProcessingStatus(data["status"])
        data["start_time"] = datetime.fromisoformat(data["start_time"])
        if data.get("end_time"):
            data["end_time"] = datetime.fromisoformat(data["end_time"])
        return cls(**data)


@dataclass
class BackfillJob:
    """Backfill job definition."""

    job_id: str
    start_date: datetime
    end_date: datetime
    hex_pairs: Optional[List[Tuple[str, str]]] = None
    providers: Optional[List[str]] = None
    status: ProcessingStatus = ProcessingStatus.PENDING
    created_at: datetime = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow()

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        result = asdict(self)
        result["status"] = self.status.value
        result["start_date"] = self.start_date.isoformat()
        result["end_date"] = self.end_date.isoformat()
        result["created_at"] = self.created_at.isoformat()
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BackfillJob":
        """Create from dictionary."""
        data = data.copy()
        data["status"] = ProcessingStatus(data["status"])
        data["start_date"] = datetime.fromisoformat(data["start_date"])
        data["end_date"] = datetime.fromisoformat(data["end_date"])
        data["created_at"] = datetime.fromisoformat(data["created_at"])
        return cls(**data)


class StateStore:
    """Manages processing state and watermarks in Snowflake."""

    def __init__(self):
        """Initialize state store."""
        self.connection = get_core_connection()
        self.logger = logger

        # Table names
        self.processing_state_table = "PROCESSING_STATE"
        self.backfill_jobs_table = "BACKFILL_JOBS"
        self.watermarks_table = "WATERMARKS"

        # Ensure tables exist
        self._initialize_tables()

    def _initialize_tables(self):
        """Initialize state management tables if they don't exist."""

        # Processing state table
        processing_state_ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.processing_state_table} (
            job_id STRING PRIMARY KEY,
            job_type STRING NOT NULL,
            status STRING NOT NULL,
            start_time TIMESTAMP_TZ NOT NULL,
            end_time TIMESTAMP_TZ,
            processed_records INTEGER DEFAULT 0,
            failed_records INTEGER DEFAULT 0,
            metadata VARIANT,
            error_message STRING,
            updated_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
        )
        """

        # Backfill jobs table
        backfill_jobs_ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.backfill_jobs_table} (
            job_id STRING PRIMARY KEY,
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            hex_pairs VARIANT,
            providers VARIANT,
            status STRING NOT NULL,
            created_at TIMESTAMP_TZ NOT NULL,
            updated_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
        )
        """

        # Watermarks table
        watermarks_ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.watermarks_table} (
            watermark_name STRING PRIMARY KEY,
            watermark_value TIMESTAMP_TZ NOT NULL,
            metadata VARIANT,
            updated_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
        )
        """

        try:
            self.connection.execute_query(processing_state_ddl)
            self.connection.execute_query(backfill_jobs_ddl)
            self.connection.execute_query(watermarks_ddl)
            self.logger.info("State management tables initialized")
        except Exception as e:
            self.logger.error(f"Failed to initialize state tables: {e}")
            raise

    def save_processing_state(self, state: ProcessingState) -> bool:
        """
        Save processing state to Snowflake.

        Args:
            state: ProcessingState object

        Returns:
            True if successful
        """
        try:
            # Use MERGE for upsert behavior
            merge_sql = f"""
            MERGE INTO {self.processing_state_table} AS target
            USING (
                SELECT 
                    %(job_id)s as job_id,
                    %(job_type)s as job_type,
                    %(status)s as status,
                    %(start_time)s as start_time,
                    %(end_time)s as end_time,
                    %(processed_records)s as processed_records,
                    %(failed_records)s as failed_records,
                    PARSE_JSON(%(metadata)s) as metadata,
                    %(error_message)s as error_message
            ) AS source
            ON target.job_id = source.job_id
            WHEN MATCHED THEN UPDATE SET
                status = source.status,
                end_time = source.end_time,
                processed_records = source.processed_records,
                failed_records = source.failed_records,
                metadata = source.metadata,
                error_message = source.error_message,
                updated_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT (
                job_id, job_type, status, start_time, end_time, 
                processed_records, failed_records, metadata, error_message
            ) VALUES (
                source.job_id, source.job_type, source.status, source.start_time, 
                source.end_time, source.processed_records, source.failed_records, 
                source.metadata, source.error_message
            )
            """

            params = {
                "job_id": state.job_id,
                "job_type": state.job_type,
                "status": state.status.value,
                "start_time": state.start_time.isoformat(),
                "end_time": state.end_time.isoformat() if state.end_time else None,
                "processed_records": state.processed_records,
                "failed_records": state.failed_records,
                "metadata": json.dumps(state.metadata) if state.metadata else None,
                "error_message": state.error_message,
            }

            self.connection.execute_query(merge_sql, params)
            self.logger.debug(f"Saved processing state for job {state.job_id}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to save processing state: {e}")
            return False

    def get_processing_state(self, job_id: str) -> Optional[ProcessingState]:
        """
        Get processing state by job ID.

        Args:
            job_id: Job identifier

        Returns:
            ProcessingState object or None if not found
        """
        try:
            query = f"""
            SELECT job_id, job_type, status, start_time, end_time,
                   processed_records, failed_records, metadata, error_message
            FROM {self.processing_state_table}
            WHERE job_id = %(job_id)s
            """

            results = self.connection.execute_query(
                query, {"job_id": job_id}, fetch=True
            )

            if not results:
                return None

            row = results[0]

            # Parse metadata if present
            metadata = None
            if row.get("METADATA"):
                try:
                    metadata = (
                        json.loads(row["METADATA"])
                        if isinstance(row["METADATA"], str)
                        else row["METADATA"]
                    )
                except json.JSONDecodeError:
                    metadata = row["METADATA"]

            return ProcessingState(
                job_id=row["JOB_ID"],
                job_type=row["JOB_TYPE"],
                status=ProcessingStatus(row["STATUS"]),
                start_time=datetime.fromisoformat(
                    row["START_TIME"].replace("Z", "+00:00")
                ),
                end_time=datetime.fromisoformat(row["END_TIME"].replace("Z", "+00:00"))
                if row.get("END_TIME")
                else None,
                processed_records=row.get("PROCESSED_RECORDS", 0),
                failed_records=row.get("FAILED_RECORDS", 0),
                metadata=metadata,
                error_message=row.get("ERROR_MESSAGE"),
            )

        except Exception as e:
            self.logger.error(f"Failed to get processing state for {job_id}: {e}")
            return None

    def save_backfill_job(self, job: BackfillJob) -> bool:
        """
        Save backfill job definition.

        Args:
            job: BackfillJob object

        Returns:
            True if successful
        """
        try:
            merge_sql = f"""
            MERGE INTO {self.backfill_jobs_table} AS target
            USING (
                SELECT 
                    %(job_id)s as job_id,
                    %(start_date)s as start_date,
                    %(end_date)s as end_date,
                    PARSE_JSON(%(hex_pairs)s) as hex_pairs,
                    PARSE_JSON(%(providers)s) as providers,
                    %(status)s as status,
                    %(created_at)s as created_at
            ) AS source
            ON target.job_id = source.job_id
            WHEN MATCHED THEN UPDATE SET
                status = source.status,
                updated_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT (
                job_id, start_date, end_date, hex_pairs, providers, status, created_at
            ) VALUES (
                source.job_id, source.start_date, source.end_date, 
                source.hex_pairs, source.providers, source.status, source.created_at
            )
            """

            params = {
                "job_id": job.job_id,
                "start_date": job.start_date.date().isoformat(),
                "end_date": job.end_date.date().isoformat(),
                "hex_pairs": json.dumps(job.hex_pairs) if job.hex_pairs else None,
                "providers": json.dumps(job.providers) if job.providers else None,
                "status": job.status.value,
                "created_at": job.created_at.isoformat(),
            }

            self.connection.execute_query(merge_sql, params)
            self.logger.debug(f"Saved backfill job {job.job_id}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to save backfill job: {e}")
            return False

    def get_backfill_jobs(
        self, status: Optional[ProcessingStatus] = None, limit: int = 100
    ) -> List[BackfillJob]:
        """
        Get backfill jobs, optionally filtered by status.

        Args:
            status: Optional status filter
            limit: Maximum number of jobs to return

        Returns:
            List of BackfillJob objects
        """
        try:
            query = f"""
            SELECT job_id, start_date, end_date, hex_pairs, providers, status, created_at
            FROM {self.backfill_jobs_table}
            """

            params = {}
            if status:
                query += " WHERE status = %(status)s"
                params["status"] = status.value

            query += " ORDER BY created_at DESC LIMIT %(limit)s"
            params["limit"] = limit

            results = self.connection.execute_query(query, params, fetch=True)

            jobs = []
            for row in results:
                # Parse arrays if present
                hex_pairs = None
                if row.get("HEX_PAIRS"):
                    try:
                        hex_pairs = (
                            json.loads(row["HEX_PAIRS"])
                            if isinstance(row["HEX_PAIRS"], str)
                            else row["HEX_PAIRS"]
                        )
                    except json.JSONDecodeError:
                        hex_pairs = row["HEX_PAIRS"]

                providers = None
                if row.get("PROVIDERS"):
                    try:
                        providers = (
                            json.loads(row["PROVIDERS"])
                            if isinstance(row["PROVIDERS"], str)
                            else row["PROVIDERS"]
                        )
                    except json.JSONDecodeError:
                        providers = row["PROVIDERS"]

                job = BackfillJob(
                    job_id=row["JOB_ID"],
                    start_date=datetime.fromisoformat(str(row["START_DATE"])),
                    end_date=datetime.fromisoformat(str(row["END_DATE"])),
                    hex_pairs=hex_pairs,
                    providers=providers,
                    status=ProcessingStatus(row["STATUS"]),
                    created_at=datetime.fromisoformat(
                        row["CREATED_AT"].replace("Z", "+00:00")
                    ),
                )
                jobs.append(job)

            return jobs

        except Exception as e:
            self.logger.error(f"Failed to get backfill jobs: {e}")
            return []

    def set_watermark(
        self, name: str, value: datetime, metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Set a watermark value.

        Args:
            name: Watermark name
            value: Watermark value
            metadata: Optional metadata

        Returns:
            True if successful
        """
        try:
            merge_sql = f"""
            MERGE INTO {self.watermarks_table} AS target
            USING (
                SELECT 
                    %(name)s as watermark_name,
                    %(value)s as watermark_value,
                    PARSE_JSON(%(metadata)s) as metadata
            ) AS source
            ON target.watermark_name = source.watermark_name
            WHEN MATCHED THEN UPDATE SET
                watermark_value = source.watermark_value,
                metadata = source.metadata,
                updated_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT (
                watermark_name, watermark_value, metadata
            ) VALUES (
                source.watermark_name, source.watermark_value, source.metadata
            )
            """

            params = {
                "name": name,
                "value": value.isoformat(),
                "metadata": json.dumps(metadata) if metadata else None,
            }

            self.connection.execute_query(merge_sql, params)
            self.logger.debug(f"Set watermark {name} to {value}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to set watermark: {e}")
            return False

    def get_watermark(
        self, name: str
    ) -> Optional[Tuple[datetime, Optional[Dict[str, Any]]]]:
        """
        Get a watermark value.

        Args:
            name: Watermark name

        Returns:
            Tuple of (watermark_value, metadata) or None if not found
        """
        try:
            query = f"""
            SELECT watermark_value, metadata
            FROM {self.watermarks_table}
            WHERE watermark_name = %(name)s
            """

            results = self.connection.execute_query(query, {"name": name}, fetch=True)

            if not results:
                return None

            row = results[0]
            watermark_value = datetime.fromisoformat(
                row["WATERMARK_VALUE"].replace("Z", "+00:00")
            )

            metadata = None
            if row.get("METADATA"):
                try:
                    metadata = (
                        json.loads(row["METADATA"])
                        if isinstance(row["METADATA"], str)
                        else row["METADATA"]
                    )
                except json.JSONDecodeError:
                    metadata = row["METADATA"]

            return watermark_value, metadata

        except Exception as e:
            self.logger.error(f"Failed to get watermark {name}: {e}")
            return None

    def cleanup_old_states(self, older_than_days: int = 30) -> int:
        """
        Clean up old processing states.

        Args:
            older_than_days: Delete states older than this many days

        Returns:
            Number of records deleted
        """
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=older_than_days)

            delete_sql = f"""
            DELETE FROM {self.processing_state_table}
            WHERE start_time < %(cutoff_date)s
            AND status IN ('completed', 'failed', 'cancelled')
            """

            self.connection.execute_query(
                delete_sql, {"cutoff_date": cutoff_date.isoformat()}
            )

            # Get count of deleted records (this is a simplification)
            # In practice, you might want to capture the actual count
            self.logger.info(f"Cleaned up processing states older than {cutoff_date}")
            return 0  # Snowflake doesn't return affected count easily

        except Exception as e:
            self.logger.error(f"Failed to cleanup old states: {e}")
            return 0


# Convenience functions
def get_state_store() -> StateStore:
    """Get state store instance."""
    return StateStore()


def create_processing_job(
    job_id: str, job_type: str, metadata: Optional[Dict[str, Any]] = None
) -> ProcessingState:
    """Create a new processing job state."""
    return ProcessingState(
        job_id=job_id,
        job_type=job_type,
        status=ProcessingStatus.PENDING,
        start_time=datetime.utcnow(),
        metadata=metadata,
    )


def create_backfill_job(
    start_date: datetime,
    end_date: datetime,
    hex_pairs: Optional[List[Tuple[str, str]]] = None,
    providers: Optional[List[str]] = None,
) -> BackfillJob:
    """Create a new backfill job definition."""
    job_id = f"backfill_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}_{int(datetime.utcnow().timestamp())}"

    return BackfillJob(
        job_id=job_id,
        start_date=start_date,
        end_date=end_date,
        hex_pairs=hex_pairs,
        providers=providers,
    )
