"""
Configuration management for the Predictive ETA Calculator pipeline.

This module provides centralized configuration loading and validation using Pydantic.
All environment variables are loaded and validated at startup.
"""

import os
from typing import Optional, Literal, List
from pydantic import BaseModel, validator, Field
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class SnowflakeConfig(BaseModel):
    """Snowflake database configuration."""

    account: str = Field(..., description="Snowflake account identifier")
    user: str = Field(..., description="Snowflake username")
    password: Optional[str] = Field(None, description="Snowflake password")
    private_key_path: Optional[str] = Field(
        None, description="Path to private key file for keypair auth"
    )
    role: str = Field(default="SYSADMIN", description="Snowflake role")
    warehouse: str = Field(default="COMPUTE_WH", description="Snowflake warehouse")
    database: str = Field(default="PREDICTIVE_ETA", description="Snowflake database")
    schema_raw: str = Field(default="RAW", description="Raw data schema")
    schema_core: str = Field(default="CORE", description="Core data schema")

    @validator("password", "private_key_path")
    def validate_auth_method(cls, v, values):
        """Ensure either password or private key is provided."""
        if "password" in values and not values.get("password") and not v:
            raise ValueError("Either password or private_key_path must be provided")
        return v


class ProviderConfig(BaseModel):
    """Routing provider configuration."""

    provider: Literal["osrm", "google", "here"] = Field(
        default="osrm", description="Primary routing provider"
    )
    osrm_base_url: str = Field(
        default="http://router.project-osrm.org", description="OSRM base URL"
    )
    google_api_key: Optional[str] = Field(None, description="Google Maps API key")
    here_api_key: Optional[str] = Field(None, description="HERE API key")

    # Rate limiting and retry configuration
    requests_per_second: int = Field(
        default=5, description="Rate limit for API requests"
    )
    max_retries: int = Field(default=3, description="Maximum number of retry attempts")
    backoff_factor: float = Field(default=1.0, description="Exponential backoff factor")
    timeout_seconds: int = Field(default=30, description="Request timeout in seconds")


class H3Config(BaseModel):
    """H3 hexagonal grid configuration."""

    resolution: int = Field(default=7, description="H3 resolution level")
    city_name: str = Field(..., description="City name for grid generation")
    city_bbox: List[float] = Field(
        ..., description="City bounding box [min_lat, min_lng, max_lat, max_lng]"
    )

    # Neighbor discovery distances (approximate km)
    neighbor_distance_1km: int = Field(
        default=1, description="K-ring distance for ~1km neighbors"
    )
    neighbor_distance_3km: int = Field(
        default=2, description="K-ring distance for ~3km neighbors"
    )
    neighbor_distance_8km: int = Field(
        default=4, description="K-ring distance for ~8km neighbors"
    )

    @validator("city_bbox")
    def validate_bbox(cls, v):
        """Validate bounding box format."""
        if len(v) != 4:
            raise ValueError(
                "Bounding box must have 4 values: [min_lat, min_lng, max_lat, max_lng]"
            )
        if v[0] >= v[2] or v[1] >= v[3]:
            raise ValueError(
                "Invalid bounding box: min values must be less than max values"
            )
        return v


class ETAConfig(BaseModel):
    """ETA calculation configuration."""

    mode: Literal["direct", "path_sum"] = Field(
        default="direct", description="ETA calculation mode"
    )
    rain_uplift_pct: float = Field(
        default=0.0, description="Rain weather uplift percentage"
    )

    # Time slab definitions
    time_slabs: List[str] = Field(
        default=["0-4", "4-8", "8-12", "12-16", "16-20", "20-24"],
        description="Time slabs for aggregation",
    )

    # Batch processing
    batch_size: int = Field(
        default=100, description="Batch size for processing hex pairs"
    )
    max_concurrent_requests: int = Field(
        default=10, description="Maximum concurrent API requests"
    )


class LoggingConfig(BaseModel):
    """Logging configuration."""

    level: str = Field(default="INFO", description="Logging level")
    format_str: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        description="Log format string",
    )
    enable_structured_logging: bool = Field(
        default=True, description="Enable structured JSON logging"
    )


class AppConfig(BaseModel):
    """Main application configuration."""

    snowflake: SnowflakeConfig
    provider: ProviderConfig
    h3: H3Config
    eta: ETAConfig
    logging: LoggingConfig

    # Environment
    environment: str = Field(default="development", description="Environment name")
    debug: bool = Field(default=False, description="Enable debug mode")

    class Config:
        env_nested_delimiter = "__"


def load_config() -> AppConfig:
    """Load and validate configuration from environment variables."""

    # Required environment variables
    required_vars = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "CITY_NAME", "CITY_BBOX"]

    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}")

    # Parse city bounding box
    city_bbox_str = os.getenv("CITY_BBOX", "")
    try:
        city_bbox = [float(x.strip()) for x in city_bbox_str.split(",")]
    except (ValueError, AttributeError):
        raise ValueError(
            "CITY_BBOX must be comma-separated floats: 'min_lat,min_lng,max_lat,max_lng'"
        )

    config_dict = {
        "snowflake": {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "private_key_path": os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"),
            "role": os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            "database": os.getenv("SNOWFLAKE_DATABASE", "PREDICTIVE_ETA"),
            "schema_raw": os.getenv("SNOWFLAKE_SCHEMA_RAW", "RAW"),
            "schema_core": os.getenv("SNOWFLAKE_SCHEMA_CORE", "CORE"),
        },
        "provider": {
            "provider": os.getenv("PROVIDER", "osrm"),
            "osrm_base_url": os.getenv(
                "OSRM_BASE_URL", "http://router.project-osrm.org"
            ),
            "google_api_key": os.getenv("GOOGLE_API_KEY"),
            "here_api_key": os.getenv("HERE_API_KEY"),
            "requests_per_second": int(os.getenv("REQUESTS_PER_SECOND", "5")),
            "max_retries": int(os.getenv("MAX_RETRIES", "3")),
            "backoff_factor": float(os.getenv("BACKOFF_FACTOR", "1.0")),
            "timeout_seconds": int(os.getenv("TIMEOUT_SECONDS", "30")),
        },
        "h3": {
            "resolution": int(os.getenv("H3_RESOLUTION", "7")),
            "city_name": os.getenv("CITY_NAME"),
            "city_bbox": city_bbox,
            "neighbor_distance_1km": int(os.getenv("NEIGHBOR_DISTANCE_1KM", "1")),
            "neighbor_distance_3km": int(os.getenv("NEIGHBOR_DISTANCE_3KM", "2")),
            "neighbor_distance_8km": int(os.getenv("NEIGHBOR_DISTANCE_8KM", "4")),
        },
        "eta": {
            "mode": os.getenv("ETA_MODE", "direct"),
            "rain_uplift_pct": float(os.getenv("RAIN_UPLIFT_PCT", "0.0")),
            "batch_size": int(os.getenv("BATCH_SIZE", "100")),
            "max_concurrent_requests": int(os.getenv("MAX_CONCURRENT_REQUESTS", "10")),
        },
        "logging": {
            "level": os.getenv("LOG_LEVEL", "INFO"),
            "enable_structured_logging": os.getenv(
                "ENABLE_STRUCTURED_LOGGING", "true"
            ).lower()
            == "true",
        },
        "environment": os.getenv("ENVIRONMENT", "development"),
        "debug": os.getenv("DEBUG", "false").lower() == "true",
    }

    return AppConfig(**config_dict)


# Global configuration instance
config = load_config()
