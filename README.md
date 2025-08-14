# Predictive ETA Calculator Pipeline

A production-grade ETL pipeline for computing and serving ETA ranges between H3 hexagonal grid cells using multiple routing providers, with time-based aggregations and weather adjustments.

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://python.org)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8+-orange.svg)](https://airflow.apache.org)
[![dbt](https://img.shields.io/badge/dbt-1.7+-green.svg)](https://getdbt.com)
[![Snowflake](https://img.shields.io/badge/Snowflake-Compatible-blue.svg)](https://snowflake.com)

## 🚀 Overview

This pipeline processes routing data from multiple providers (OSRM, Google Maps, HERE) to generate predictive ETA ranges for city-wide transportation analysis. It uses H3 hexagonal grids for spatial partitioning and provides time-slab-based aggregations optimized for real-time serving applications.

### Key Features

- **Multi-Provider Routing**: OSRM (default), Google Maps, HERE Maps with automatic fallback
- **H3 Spatial Indexing**: Resolution 7 hexagonal grids (~1.22km edge length)
- **Time-Based Aggregations**: 6 time slabs across weekdays with min/max ETA ranges
- **Weather Adjustments**: Configurable rain uplift factors
- **Production-Ready**: Comprehensive error handling, logging, monitoring, and testing
- **Scalable Architecture**: Batch processing with configurable concurrency
- **Data Quality**: Extensive validation, deduplication, and quality scoring

## 📊 Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │  Orchestration  │    │  Data Warehouse │
│                 │    │                 │    │                 │
│ • OSRM API      │───▶│ Apache Airflow  │───▶│   Snowflake     │
│ • Google Maps   │    │                 │    │                 │
│ • HERE Maps     │    │ • DAG Scheduler │    │ • RAW Schema    │
│                 │    │ • Task Manager  │    │ • CORE Schema   │
└─────────────────┘    │ • Error Handling│    │ • MARTS Schema  │
                       └─────────────────┘    └─────────────────┘
                                │                       │
                       ┌─────────────────┐    ┌─────────────────┐
                       │ Transformation  │    │     Serving     │
                       │                 │    │                 │
                       │ dbt Models      │    │ • ETA_SLABS     │
                       │ • Staging       │◀───┤ • API Ready     │
                       │ • Marts         │    │ • Materialized  │
                       │ • Tests         │    │   Views         │
                       └─────────────────┘    └─────────────────┘
```

## 🏗️ Repository Structure

```
predictive-eta-pipeline/
├── airflow/                    # Airflow orchestration
│   ├── dags/
│   │   └── dag_predictive_eta.py
│   ├── include/sql/
│   └── docker-compose.yml     # Local development
├── etl/                       # ETL Python package
│   ├── common/                # Shared utilities
│   │   ├── config.py          # Configuration management
│   │   ├── logging.py         # Structured logging
│   │   └── snowflake.py       # Database connections
│   ├── h3/                    # H3 grid utilities
│   │   ├── grid.py            # Grid generation
│   │   └── neighbors.py       # Neighbor discovery
│   ├── ingest/                # Data ingestion
│   │   ├── osrm_client.py     # OSRM integration
│   │   ├── google_client.py   # Google Maps integration
│   │   ├── here_client.py     # HERE Maps integration
│   │   └── distance_matrix.py # Provider-agnostic wrapper
│   ├── transform/             # Data transformation
│   │   ├── routing_paths.py   # Path mapping
│   │   └── slab_agg.py        # Time aggregation
│   ├── load/                  # Data loading
│   │   └── snowflake_load.py  # Snowflake operations
│   └── state/                 # State management
│       └── state_store.py     # Job tracking
├── dbt/                       # dbt transformations
│   ├── dbt_project.yml
│   ├── packages.yml
│   ├── models/
│   │   ├── staging/           # Staging models
│   │   │   ├── stg_routes_raw.sql
│   │   │   └── stg_h3_lookup.sql
│   │   ├── marts/             # Business logic
│   │   │   ├── fct_eta_hex_pair.sql
│   │   │   └── dim_time_slab.sql
│   │   └── schema.yml         # Tests & documentation
│   └── macros/
│       └── merge_upsert.sql   # Custom macros
├── infra/                     # Infrastructure
│   ├── snowflake_ddl.sql      # Database setup
│   └── roles_policies.sql     # Security configuration
├── scripts/                   # Utility scripts
│   ├── bootstrap_city_grid.py # Grid initialization
│   └── backfill_eta.py        # Historical processing
├── .env.example               # Configuration template
├── requirements.txt           # Python dependencies
├── README.md                  # This file
└── LICENSE                    # MIT License
```

## 🛠️ Setup & Installation

### Prerequisites

- Python 3.11+
- Snowflake account with appropriate permissions
- (Optional) Google Maps API key for enhanced routing
- (Optional) HERE Maps API key for additional coverage

### 1. Clone Repository

```bash
git clone <repository-url>
cd predictive-eta-pipeline
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Environment

```bash
cp .env.example .env
# Edit .env with your configuration values
```

### 4. Set Up Snowflake

```bash
# Run infrastructure setup
snowsql -f infra/snowflake_ddl.sql
snowsql -f infra/roles_policies.sql
```

### 5. Initialize dbt

```bash
cd dbt
dbt deps
dbt debug
```

## ⚙️ Configuration

### Required Environment Variables

```bash
# Snowflake Connection
SNOWFLAKE_ACCOUNT=your_account.region.cloud
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ROLE=PREDICTIVE_ETA_ETL_ROLE
SNOWFLAKE_WAREHOUSE=PREDICTIVE_ETA_WH
SNOWFLAKE_DATABASE=PREDICTIVE_ETA

# City Configuration
CITY_NAME=Dubai
CITY_BBOX=24.9,54.8,25.4,55.6  # min_lat,min_lng,max_lat,max_lng
H3_RESOLUTION=7

# Provider Settings
PROVIDER=osrm
OSRM_BASE_URL=http://router.project-osrm.org
# GOOGLE_API_KEY=your_key_here  # Optional
# HERE_API_KEY=your_key_here    # Optional
```

### City Bounding Boxes

Pre-configured bounding boxes for major cities:

| City      | Bounding Box                        |
| --------- | ----------------------------------- |
| Dubai     | `24.9,54.8,25.4,55.6`               |
| New York  | `40.4774,-74.2591,40.9176,-73.7004` |
| London    | `51.2868,-0.5103,51.6918,0.3340`    |
| Singapore | `1.1304,103.6026,1.4784,104.0120`   |
| Paris     | `48.8155,2.2241,48.9021,2.4699`     |
| Tokyo     | `35.5322,139.3796,35.8177,139.9190` |

## 🚀 Quick Start

### 1. Bootstrap City Grid

```bash
python scripts/bootstrap_city_grid.py --city Dubai
```

### 2. Run Initial ETL

```bash
# Start Airflow (if using Docker)
cd airflow && docker-compose up -d

# Trigger DAG
airflow dags trigger dag_predictive_eta
```

### 3. Run dbt Models

```bash
cd dbt
dbt run --select staging
dbt run --select marts
dbt test
```

### 4. Verify Data

```sql
-- Check final ETA data
SELECT
    COUNT(*) as total_records,
    COUNT(DISTINCT from_hex) as origin_hexes,
    COUNT(DISTINCT to_hex) as destination_hexes,
    AVG(sample_count) as avg_samples_per_record
FROM CORE.ETA_SLABS;
```

## 📋 Data Model

### RAW Schema

**`ROUTES_RAW`** - Raw routing data from providers

- `from_hex`, `to_hex` - H3 hex identifiers
- `provider` - Routing provider (osrm, google, here)
- `distance_m`, `duration_s` - Route metrics
- `depart_ts`, `weekday`, `hour` - Time dimensions
- `request_id` - Idempotency key

### CORE Schema

**`H3_LOOKUP`** - H3 hexagonal grid

- `hex_id` - H3 hex identifier
- `city`, `resolution` - Grid metadata
- `centroid_lat`, `centroid_lng` - Geographic coordinates

**`ETA_SLABS`** - Final serving table

- `from_hex`, `to_hex` - Route endpoints
- `weekday`, `slab` - Time dimensions
- `min_eta_s`, `max_eta_s` - ETA range
- `rain_eta_s` - Weather-adjusted ETA
- `sample_count` - Data quality indicator

### Time Slabs

| Slab  | Hours    | Business Period |
| ----- | -------- | --------------- |
| 0-4   | 12AM-4AM | Night           |
| 4-8   | 4AM-8AM  | Morning Rush    |
| 8-12  | 8AM-12PM | Business Hours  |
| 12-16 | 12PM-4PM | Business Hours  |
| 16-20 | 4PM-8PM  | Evening Rush    |
| 20-24 | 8PM-12AM | Evening         |

## 🔄 Operations

### Daily Pipeline

The pipeline runs daily at 2 AM UTC:

1. **Bootstrap Grid** - Ensure H3 grid exists (idempotent)
2. **Ingest Routes** - Fetch routing data from providers
3. **Transform** - Aggregate by time periods
4. **dbt Models** - Run staging and marts
5. **Materialize** - Create final serving tables
6. **Test** - Data quality validation
7. **Report** - Publish metrics

### Backfill Historical Data

```bash
# Backfill specific date range
python scripts/backfill_eta.py \
    --start-date 2024-01-01 \
    --end-date 2024-01-07 \
    --providers osrm,google

# Sample-based backfill for testing
python scripts/backfill_eta.py \
    --start-date 2024-01-01 \
    --end-date 2024-01-01 \
    --sample-rate 0.1 \
    --hours 8,12,18 \
    --dry-run
```

### Airflow Commands

```bash
# Trigger specific DAG run
airflow dags trigger dag_predictive_eta

# Backfill date range
airflow dags backfill dag_predictive_eta \
    --start-date 2024-01-01 \
    --end-date 2024-01-07

# Check DAG status
airflow dags state dag_predictive_eta 2024-01-15

# View task logs
airflow tasks logs dag_predictive_eta bootstrap_city_grid 2024-01-15
```

### dbt Commands

```bash
# Run all models
dbt run

# Run specific selection
dbt run --select staging
dbt run --select marts
dbt run --select +fct_eta_hex_pair

# Test data quality
dbt test
dbt test --select staging
dbt test --models fct_eta_hex_pair

# Generate documentation
dbt docs generate
dbt docs serve
```

## 📊 Monitoring & Observability

### Data Quality Metrics

- **Completeness**: Hex pair coverage across time periods
- **Accuracy**: ETA range validation and outlier detection
- **Freshness**: Maximum age of ETA calculations
- **Consistency**: Cross-provider ETA comparisons

### Key Performance Indicators

```sql
-- Daily pipeline health check
SELECT
    DATE(updated_at) as pipeline_date,
    COUNT(*) as total_eta_records,
    AVG(sample_count) as avg_sample_count,
    MIN(min_eta_s) as fastest_route_s,
    MAX(max_eta_s) as slowest_route_s,
    COUNT(CASE WHEN sample_count < 5 THEN 1 END) as low_quality_records
FROM CORE.ETA_SLABS
WHERE updated_at >= CURRENT_DATE - 7
GROUP BY DATE(updated_at)
ORDER BY pipeline_date DESC;
```

### Operational Alerts

Monitor these conditions:

- Pipeline failures or timeouts
- Data freshness > 24 hours
- Sample count drops below thresholds
- Provider API failures > 10%
- Warehouse credit consumption spikes

## 🧪 Testing

### Unit Tests

```bash
pytest tests/unit/
```

### Integration Tests

```bash
pytest tests/integration/
```

### dbt Tests

```bash
dbt test --select staging
dbt test --select marts
```

### Data Quality Tests

The pipeline includes comprehensive data quality tests:

- **Uniqueness**: Primary key constraints
- **Referential Integrity**: Foreign key relationships
- **Range Validation**: ETA bounds and geographic coordinates
- **Completeness**: Required field validation
- **Business Logic**: Min ≤ Max ETA, rain adjustments

## 🔧 Troubleshooting

### Common Issues

**1. Snowflake Connection Errors**

```bash
# Test connection
python -c "from etl.common import get_core_connection; conn = get_core_connection(); print('✅ Connected')"

# Check permissions
snowsql -q "SHOW GRANTS TO ROLE PREDICTIVE_ETA_ETL_ROLE;"
```

**2. Provider API Rate Limits**

```bash
# Reduce concurrent requests
export MAX_CONCURRENT_REQUESTS=5
export REQUESTS_PER_SECOND=2
```

**3. H3 Grid Generation Issues**

```bash
# Test with smaller area
python scripts/bootstrap_city_grid.py --city Dubai --bbox "25.0,55.0,25.1,55.1" --dry-run
```

**4. dbt Model Failures**

```bash
# Debug specific model
dbt run --select stg_routes_raw --debug
dbt compile --select fct_eta_hex_pair
```

### Performance Optimization

**Warehouse Sizing**

- Development: `SMALL` (1 credit/hour)
- Production: `MEDIUM` to `LARGE` (2-8 credits/hour)
- Backfill: `X-LARGE` temporarily (16 credits/hour)

**Query Optimization**

- Use clustering keys on high-cardinality columns
- Partition large tables by date
- Optimize JOIN order and predicates
- Use materialized views for frequently queried aggregations

**ETL Tuning**

```bash
# Increase batch size for better throughput
export BATCH_SIZE=500

# Adjust concurrent workers based on provider limits
export MAX_CONCURRENT_REQUESTS=20
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines

- Follow PEP 8 style guidelines
- Add type hints to all functions
- Write comprehensive docstrings
- Include unit tests for new functionality
- Update documentation for API changes

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🔗 Resources

- [H3 Spatial Index Documentation](https://h3geo.org/)
- [Snowflake Documentation](https://docs.snowflake.com/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [dbt Documentation](https://docs.getdbt.com/)
- [OSRM API Documentation](http://project-osrm.org/docs/v5.24.0/api/)

## 📞 Support

For questions, issues, or contributions:

- Create an issue in the repository
- Review existing documentation
- Check the troubleshooting section
- Contact the data engineering team

---

**Built with ❤️ for efficient urban mobility analysis**
