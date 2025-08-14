"""
Snowflake connection and utility functions for the Predictive ETA Calculator pipeline.

Provides connection management, bulk loading, and common database operations
optimized for the ETL pipeline requirements.
"""

import os
import json
import tempfile
from typing import Any, Dict, List, Optional, Union, Iterator
from contextlib import contextmanager
from pathlib import Path
import pandas as pd

import snowflake.connector
from snowflake.connector import DictCursor
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key

from .config import config
from .logging import get_logger, TimedLogger, log_database_operation

logger = get_logger("snowflake")


class SnowflakeConnection:
    """Manages Snowflake database connections and operations."""

    def __init__(self, schema: Optional[str] = None):
        """
        Initialize Snowflake connection manager.

        Args:
            schema: Default schema to use (RAW or CORE)
        """
        self.config = config.snowflake
        self.default_schema = schema
        self._connection = None

    def _get_auth_params(self) -> Dict[str, Any]:
        """Get authentication parameters for Snowflake connection."""
        auth_params = {
            "account": self.config.account,
            "user": self.config.user,
            "role": self.config.role,
            "warehouse": self.config.warehouse,
            "database": self.config.database,
        }

        if self.default_schema:
            auth_params["schema"] = self.default_schema

        # Use password or private key authentication
        if self.config.password:
            auth_params["password"] = self.config.password
        elif self.config.private_key_path:
            private_key = self._load_private_key(self.config.private_key_path)
            auth_params["private_key"] = private_key
        else:
            raise ValueError("Either password or private_key_path must be configured")

        return auth_params

    def _load_private_key(self, key_path: str) -> bytes:
        """Load private key from file for keypair authentication."""
        try:
            with open(key_path, "rb") as key_file:
                private_key = load_pem_private_key(
                    key_file.read(),
                    password=None,  # Assuming no passphrase
                )

            return private_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        except Exception as e:
            logger.error(f"Failed to load private key from {key_path}: {e}")
            raise

    @contextmanager
    def get_connection(self, autocommit: bool = True):
        """
        Get a Snowflake connection with automatic cleanup.

        Args:
            autocommit: Enable autocommit mode

        Yields:
            Snowflake connection object
        """
        connection = None
        try:
            auth_params = self._get_auth_params()
            connection = snowflake.connector.connect(**auth_params)
            connection.autocommit(autocommit)

            logger.info(
                "Connected to Snowflake",
                extra=log_database_operation(
                    operation="CONNECT",
                    table="",
                    database=self.config.database,
                    schema=self.default_schema,
                ),
            )

            yield connection

        except Exception as e:
            logger.error(f"Snowflake connection error: {e}")
            raise
        finally:
            if connection:
                try:
                    connection.close()
                    logger.debug("Closed Snowflake connection")
                except Exception as e:
                    logger.warning(f"Error closing Snowflake connection: {e}")

    def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        fetch: bool = False,
        schema: Optional[str] = None,
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Execute a SQL query against Snowflake.

        Args:
            query: SQL query to execute
            params: Query parameters for parameterized queries
            fetch: Whether to fetch and return results
            schema: Schema override for this query

        Returns:
            Query results if fetch=True, otherwise None
        """
        with TimedLogger(logger, f"execute_query: {query[:100]}..."):
            with self.get_connection() as conn:
                # Switch schema if specified
                if schema:
                    conn.cursor().execute(f"USE SCHEMA {self.config.database}.{schema}")

                cursor = conn.cursor(DictCursor) if fetch else conn.cursor()

                try:
                    if params:
                        cursor.execute(query, params)
                    else:
                        cursor.execute(query)

                    if fetch:
                        results = cursor.fetchall()
                        logger.info(
                            "Query executed successfully",
                            extra=log_database_operation(
                                operation="SELECT",
                                table="multiple",
                                rows_affected=len(results) if results else 0,
                            ),
                        )
                        return results
                    else:
                        rows_affected = cursor.rowcount
                        logger.info(
                            "Query executed successfully",
                            extra=log_database_operation(
                                operation="DML",
                                table="multiple",
                                rows_affected=rows_affected,
                            ),
                        )
                        return None

                finally:
                    cursor.close()

    def bulk_insert_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: Optional[str] = None,
        if_exists: str = "append",
        chunk_size: int = 16384,
    ) -> bool:
        """
        Bulk insert a pandas DataFrame into Snowflake table.

        Args:
            df: DataFrame to insert
            table_name: Target table name
            schema: Schema override
            if_exists: Action if table exists ('append', 'replace', 'fail')
            chunk_size: Number of rows per chunk

        Returns:
            True if successful
        """
        if df.empty:
            logger.warning(f"Empty DataFrame provided for {table_name}")
            return True

        target_schema = schema or self.default_schema

        with TimedLogger(logger, f"bulk_insert_dataframe: {table_name}"):
            with self.get_connection(autocommit=False) as conn:
                try:
                    # Switch to target schema
                    if target_schema:
                        conn.cursor().execute(
                            f"USE SCHEMA {self.config.database}.{target_schema}"
                        )

                    success, nchunks, nrows, _ = write_pandas(
                        conn=conn,
                        df=df,
                        table_name=table_name,
                        overwrite=(if_exists == "replace"),
                        auto_create_table=True,
                        chunk_size=chunk_size,
                    )

                    if success:
                        conn.commit()
                        logger.info(
                            f"Successfully inserted {nrows} rows into {table_name}",
                            extra=log_database_operation(
                                operation="BULK_INSERT",
                                table=f"{target_schema}.{table_name}",
                                rows_affected=nrows,
                            ),
                        )
                        return True
                    else:
                        conn.rollback()
                        logger.error(f"Failed to insert data into {table_name}")
                        return False

                except Exception as e:
                    conn.rollback()
                    logger.error(f"Error during bulk insert to {table_name}: {e}")
                    raise

    def stage_and_copy(
        self,
        df: pd.DataFrame,
        table_name: str,
        stage_name: str = "@~/predictive_eta_stage",
        schema: Optional[str] = None,
        file_format: str = "CSV",
        merge_keys: Optional[List[str]] = None,
    ) -> bool:
        """
        Load data via Snowflake stage for large datasets.

        Args:
            df: DataFrame to load
            table_name: Target table name
            stage_name: Snowflake stage name
            schema: Schema override
            file_format: File format (CSV, JSON, PARQUET)
            merge_keys: Keys for MERGE operation (if None, uses INSERT)

        Returns:
            True if successful
        """
        if df.empty:
            logger.warning(f"Empty DataFrame provided for {table_name}")
            return True

        target_schema = schema or self.default_schema

        with TimedLogger(logger, f"stage_and_copy: {table_name}"):
            # Create temporary file
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=f".{file_format.lower()}", delete=False
            ) as tmp_file:
                temp_path = tmp_file.name

                try:
                    # Write DataFrame to temporary file
                    if file_format.upper() == "CSV":
                        df.to_csv(temp_path, index=False, header=True)
                    elif file_format.upper() == "JSON":
                        df.to_json(temp_path, orient="records", lines=True)
                    elif file_format.upper() == "PARQUET":
                        df.to_parquet(temp_path, index=False)
                    else:
                        raise ValueError(f"Unsupported file format: {file_format}")

                    # Load via Snowflake stage
                    with self.get_connection() as conn:
                        cursor = conn.cursor()

                        # Switch schema
                        if target_schema:
                            cursor.execute(
                                f"USE SCHEMA {self.config.database}.{target_schema}"
                            )

                        # Create stage if it doesn't exist
                        cursor.execute(f"CREATE STAGE IF NOT EXISTS {stage_name}")

                        # Upload file to stage
                        file_name = f"{table_name}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.{file_format.lower()}"
                        cursor.execute(
                            f"PUT file://{temp_path} {stage_name}/{file_name}"
                        )

                        # Copy data from stage to table
                        if merge_keys:
                            # Use MERGE for upsert operations
                            merge_sql = self._build_merge_sql(
                                table_name,
                                stage_name,
                                file_name,
                                merge_keys,
                                file_format,
                            )
                            cursor.execute(merge_sql)
                        else:
                            # Use COPY INTO for insert operations
                            copy_sql = f"""
                            COPY INTO {table_name}
                            FROM {stage_name}/{file_name}
                            FILE_FORMAT = (TYPE = {file_format} FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
                            ON_ERROR = 'ABORT_STATEMENT'
                            """
                            cursor.execute(copy_sql)

                        # Clean up stage file
                        cursor.execute(f"REMOVE {stage_name}/{file_name}")

                        rows_affected = cursor.rowcount
                        logger.info(
                            f"Successfully loaded {rows_affected} rows into {table_name}",
                            extra=log_database_operation(
                                operation="STAGE_COPY",
                                table=f"{target_schema}.{table_name}",
                                rows_affected=rows_affected,
                            ),
                        )

                        cursor.close()
                        return True

                except Exception as e:
                    logger.error(f"Error during stage and copy to {table_name}: {e}")
                    raise
                finally:
                    # Clean up temporary file
                    if os.path.exists(temp_path):
                        os.unlink(temp_path)

    def _build_merge_sql(
        self,
        table_name: str,
        stage_name: str,
        file_name: str,
        merge_keys: List[str],
        file_format: str,
    ) -> str:
        """Build MERGE SQL statement for upsert operations."""
        # This is a simplified version - in production, you'd want to get actual column info
        # from the table schema and build the MERGE statement dynamically

        key_conditions = " AND ".join(
            [f"target.{key} = source.{key}" for key in merge_keys]
        )

        merge_sql = f"""
        MERGE INTO {table_name} AS target
        USING (
            SELECT * FROM {stage_name}/{file_name}
            (FILE_FORMAT => (TYPE = {file_format} FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1))
        ) AS source
        ON {key_conditions}
        WHEN MATCHED THEN
            UPDATE SET 
                updated_at = CURRENT_TIMESTAMP(),
                duration_s = source.duration_s,
                distance_m = source.distance_m
        WHEN NOT MATCHED THEN
            INSERT (from_hex, to_hex, provider, distance_m, duration_s, depart_ts, weekday, hour, ingested_at, request_id)
            VALUES (source.from_hex, source.to_hex, source.provider, source.distance_m, source.duration_s, 
                   source.depart_ts, source.weekday, source.hour, CURRENT_TIMESTAMP(), source.request_id)
        """
        return merge_sql

    def get_table_info(
        self, table_name: str, schema: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get column information for a table."""
        target_schema = schema or self.default_schema

        query = f"""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = '{target_schema}'
        AND table_name = '{table_name.upper()}'
        ORDER BY ordinal_position
        """

        return self.execute_query(query, fetch=True)

    def table_exists(self, table_name: str, schema: Optional[str] = None) -> bool:
        """Check if a table exists in the specified schema."""
        target_schema = schema or self.default_schema

        query = f"""
        SELECT COUNT(*) as count
        FROM information_schema.tables
        WHERE table_schema = '{target_schema}'
        AND table_name = '{table_name.upper()}'
        """

        result = self.execute_query(query, fetch=True)
        return result[0]["COUNT"] > 0 if result else False


# Convenience functions for common operations
def get_raw_connection() -> SnowflakeConnection:
    """Get a connection configured for the RAW schema."""
    return SnowflakeConnection(schema=config.snowflake.schema_raw)


def get_core_connection() -> SnowflakeConnection:
    """Get a connection configured for the CORE schema."""
    return SnowflakeConnection(schema=config.snowflake.schema_core)


def execute_sql_file(file_path: str, schema: Optional[str] = None) -> None:
    """Execute SQL statements from a file."""
    conn = SnowflakeConnection(schema=schema)

    with open(file_path, "r") as f:
        sql_content = f.read()

    # Split on semicolons and execute each statement
    statements = [stmt.strip() for stmt in sql_content.split(";") if stmt.strip()]

    for statement in statements:
        logger.info(f"Executing SQL: {statement[:100]}...")
        conn.execute_query(statement)
