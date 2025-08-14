{{
  config(
    materialized='table',
    description='Fact table with ETA statistics aggregated by hex pairs, weekdays, and time slabs',
    tags=['fact'],
    post_hook=[
      "{{ create_or_replace_table_indexes() }}",
      "{{ analyze_table() }}"
    ]
  )
}}

with staged_routes as (
    select * from {{ ref('stg_routes_raw') }}
),

time_slab_dim as (
    select * from {{ ref('dim_time_slab') }}
),

h3_lookup as (
    select * from {{ ref('stg_h3_lookup') }}
),

-- Join routes with time slab dimension
routes_with_slabs as (
    select 
        r.*,
        ts.business_period,
        ts.expected_traffic_intensity,
        ts.traffic_multiplier,
        ts.is_rush_hour,
        ts.is_business_hours,
        ts.is_weekend
    from staged_routes r
    inner join time_slab_dim ts 
        on r.weekday = ts.weekday_name 
        and r.time_slab = ts.slab_name
    where r.time_slab is not null
),

-- Add geographic context from H3 lookup
routes_with_geography as (
    select 
        r.*,
        h_from.district as from_district,
        h_from.zone_type as from_zone_type,
        h_from.distance_from_center_km as from_center_distance_km,
        h_to.district as to_district,
        h_to.zone_type as to_zone_type,
        h_to.distance_from_center_km as to_center_distance_km
    from routes_with_slabs r
    left join h3_lookup h_from on r.from_hex = h_from.hex_id
    left join h3_lookup h_to on r.to_hex = h_to.hex_id
),

-- Calculate route characteristics
routes_with_characteristics as (
    select 
        *,
        
        -- Route type classification
        case 
            when from_zone_type = 'Core' and to_zone_type = 'Core' then 'Core-to-Core'
            when from_zone_type = 'Core' and to_zone_type in ('Urban', 'Suburban', 'Outer') then 'Core-to-Outbound'
            when from_zone_type in ('Urban', 'Suburban', 'Outer') and to_zone_type = 'Core' then 'Inbound-to-Core'
            when from_zone_type = to_zone_type then 'Same-Zone'
            else 'Cross-Zone'
        end as route_type,
        
        -- Distance classification
        case 
            when distance_km <= 2 then 'Short (≤2km)'
            when distance_km <= 5 then 'Medium (2-5km)'
            when distance_km <= 10 then 'Long (5-10km)'
            else 'Very Long (>10km)'
        end as distance_category,
        
        -- Calculate straight-line distance for comparison
        case 
            when from_center_distance_km is not null and to_center_distance_km is not null then
                abs(from_center_distance_km - to_center_distance_km)
            else null
        end as radial_distance_diff_km
        
    from routes_with_geography
),

-- Aggregate by hex pair, weekday, and time slab
eta_aggregations as (
    select 
        -- Grouping keys
        from_hex,
        to_hex,
        weekday,
        time_slab,
        
        -- Geographic attributes (take first non-null value)
        max(from_district) as from_district,
        max(from_zone_type) as from_zone_type,
        max(to_district) as to_district,
        max(to_zone_type) as to_zone_type,
        max(route_type) as route_type,
        max(distance_category) as distance_category,
        
        -- Time attributes
        max(business_period) as business_period,
        max(expected_traffic_intensity) as expected_traffic_intensity,
        max(traffic_multiplier) as traffic_multiplier,
        max(is_rush_hour) as is_rush_hour,
        max(is_business_hours) as is_business_hours,
        max(is_weekend) as is_weekend,
        
        -- ETA statistics
        min(duration_s) as min_eta_s,
        max(duration_s) as max_eta_s,
        round(avg(duration_s), 1) as avg_eta_s,
        round(median(duration_s), 1) as median_eta_s,
        round(stddev(duration_s), 1) as std_eta_s,
        
        -- Percentile statistics
        round(percentile_cont(0.25) within group (order by duration_s), 1) as p25_eta_s,
        round(percentile_cont(0.75) within group (order by duration_s), 1) as p75_eta_s,
        round(percentile_cont(0.90) within group (order by duration_s), 1) as p90_eta_s,
        round(percentile_cont(0.95) within group (order by duration_s), 1) as p95_eta_s,
        
        -- Distance statistics
        round(avg(distance_m), 1) as avg_distance_m,
        round(avg(distance_km), 2) as avg_distance_km,
        
        -- Speed statistics
        round(avg(avg_speed_kmh), 1) as avg_speed_kmh,
        round(min(avg_speed_kmh), 1) as min_speed_kmh,
        round(max(avg_speed_kmh), 1) as max_speed_kmh,
        
        -- Provider statistics
        count(distinct provider) as provider_count,
        max(provider) as primary_provider,  -- Most recent
        
        -- Sample statistics
        count(*) as sample_count,
        count(distinct date_trunc('day', depart_ts)) as distinct_days,
        min(depart_ts) as earliest_sample,
        max(depart_ts) as latest_sample,
        
        -- Traffic data availability
        sum(case when has_traffic_data then 1 else 0 end) as traffic_samples,
        round(avg(case when traffic_duration_s is not null then traffic_duration_s else duration_s end), 1) as avg_traffic_eta_s
        
    from routes_with_characteristics
    group by from_hex, to_hex, weekday, time_slab
    having count(*) >= {{ var('min_sample_count') }}  -- Data quality filter
),

-- Add rain-adjusted ETA and quality scores
final_eta_aggregations as (
    select 
        *,
        
        -- Rain-adjusted ETA (configurable uplift)
        round(max_eta_s * (1 + {{ var('rain_uplift_default') }})) as rain_eta_s,
        
        -- Data quality score (0-100)
        least(100, 
            case 
                when sample_count >= 50 then 100
                when sample_count >= 20 then 80
                when sample_count >= 10 then 60
                when sample_count >= 5 then 40
                else 20
            end +
            case 
                when distinct_days >= 7 then 0
                when distinct_days >= 3 then -10
                when distinct_days >= 2 then -20
                else -30
            end +
            case 
                when provider_count > 1 then 10
                else 0
            end
        ) as data_quality_score,
        
        -- Reliability indicators
        case 
            when (max_eta_s - min_eta_s) / avg_eta_s <= 0.3 then 'High'
            when (max_eta_s - min_eta_s) / avg_eta_s <= 0.6 then 'Medium'
            else 'Low'
        end as reliability_category,
        
        -- Business recommendations
        case 
            when avg_eta_s <= 300 then 'Excellent'      -- ≤ 5 minutes
            when avg_eta_s <= 600 then 'Good'           -- ≤ 10 minutes
            when avg_eta_s <= 1200 then 'Acceptable'    -- ≤ 20 minutes
            when avg_eta_s <= 1800 then 'Poor'          -- ≤ 30 minutes
            else 'Very Poor'                             -- > 30 minutes
        end as eta_performance_rating,
        
        -- Metadata
        current_timestamp() as updated_at,
        '{{ run_started_at }}' as dbt_run_id
        
    from eta_aggregations
)

select * from final_eta_aggregations
