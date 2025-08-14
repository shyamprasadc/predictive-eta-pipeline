{{
  config(
    materialized='view',
    description='Staged and cleaned routing data from raw ingestion with provider precedence and deduplication'
  )
}}

with raw_routes as (
    select * from {{ source('raw', 'routes_raw') }}
),

-- Apply provider precedence: OSRM > Google > HERE (configurable)
provider_precedence as (
    select 
        *,
        case 
            when provider = 'osrm' then 1
            when provider = 'google' then 2  
            when provider = 'here' then 3
            else 4
        end as provider_rank
    from raw_routes
),

-- Deduplicate by request_id and select best provider per route
deduplicated_routes as (
    select * from (
        select 
            *,
            row_number() over (
                partition by from_hex, to_hex, date_trunc('hour', depart_ts), weekday 
                order by provider_rank asc, ingested_at desc
            ) as row_num
        from provider_precedence
        where 
            -- Data quality filters
            from_hex is not null 
            and to_hex is not null
            and from_hex != to_hex
            and distance_m > 0 
            and duration_s > 0
            and duration_s <= {{ var('max_eta_seconds') }}
            and duration_s >= {{ var('min_eta_seconds') }}
            and depart_ts is not null
            and weekday is not null
            and hour is not null
    )
    where row_num = 1
),

-- Final staging transformation
staged_routes as (
    select
        -- Identifiers
        from_hex,
        to_hex,
        provider,
        request_id,
        
        -- Measurements
        distance_m,
        duration_s,
        traffic_duration_s,
        
        -- Time dimensions
        depart_ts,
        weekday,
        hour,
        date_trunc('day', depart_ts) as depart_date,
        
        -- Time slab assignment
        case 
            when hour >= 0 and hour < 4 then '0-4'
            when hour >= 4 and hour < 8 then '4-8' 
            when hour >= 8 and hour < 12 then '8-12'
            when hour >= 12 and hour < 16 then '12-16'
            when hour >= 16 and hour < 20 then '16-20'
            when hour >= 20 and hour < 24 then '20-24'
            else null
        end as time_slab,
        
        -- Metadata
        ingested_at,
        batch_id,
        
        -- Derived metrics
        round(distance_m / 1000.0, 2) as distance_km,
        round(duration_s / 60.0, 2) as duration_minutes,
        case 
            when distance_m > 0 then round((distance_m / duration_s) * 3.6, 2)
            else null 
        end as avg_speed_kmh,
        
        -- Data quality flags
        case when traffic_duration_s is not null then true else false end as has_traffic_data,
        case when batch_id is not null then true else false end as is_batch_load

    from deduplicated_routes
)

select * from staged_routes
