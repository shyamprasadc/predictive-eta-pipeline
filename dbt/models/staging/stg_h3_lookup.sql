{{
  config(
    materialized='view',
    description='Staged H3 hexagonal grid lookup with geographic enrichments'
  )
}}

with raw_h3_lookup as (
    select * from {{ source('core', 'h3_lookup') }}
),

-- Add geographic enrichments and validations
enriched_h3 as (
    select
        -- Core identifiers
        hex_id,
        city,
        resolution,
        
        -- Geographic coordinates  
        centroid_lat,
        centroid_lng,
        
        -- Geographic validations
        case 
            when centroid_lat between -90 and 90 then true 
            else false 
        end as is_valid_latitude,
        
        case 
            when centroid_lng between -180 and 180 then true 
            else false 
        end as is_valid_longitude,
        
        -- Regional classifications (customize based on your city)
        case 
            when city = 'Dubai' then
                case
                    when centroid_lat >= 25.0 and centroid_lat < 25.15 and centroid_lng >= 55.0 and centroid_lng < 55.2 then 'Downtown'
                    when centroid_lat >= 25.15 and centroid_lat < 25.25 and centroid_lng >= 55.1 and centroid_lng < 55.3 then 'Marina'
                    when centroid_lat >= 25.05 and centroid_lat < 25.12 and centroid_lng >= 55.12 and centroid_lng < 55.25 then 'DIFC'
                    when centroid_lat >= 25.2 and centroid_lat < 25.35 and centroid_lng >= 55.25 and centroid_lng < 55.45 then 'Deira'
                    else 'Other'
                end
            else 'Unknown'
        end as district,
        
        -- Distance calculations from city center (customize coordinates for your city)
        case 
            when city = 'Dubai' then
                -- Dubai city center approximate coordinates: 25.2048, 55.2708
                round(
                    st_distance(
                        st_point(centroid_lng, centroid_lat),
                        st_point(55.2708, 25.2048)
                    ) / 1000.0, 
                    2
                ) 
            else null
        end as distance_from_center_km,
        
        -- Hex characteristics based on resolution
        case 
            when resolution = 7 then 5.16  -- km^2 approximate area for res 7
            when resolution = 6 then 36.13 -- km^2 approximate area for res 6
            when resolution = 8 then 0.74  -- km^2 approximate area for res 8
            else null
        end as hex_area_km2,
        
        case 
            when resolution = 7 then 1.22  -- km approximate edge length for res 7
            when resolution = 6 then 3.23  -- km approximate edge length for res 6
            when resolution = 8 then 0.46  -- km approximate edge length for res 8
            else null
        end as hex_edge_length_km,
        
        -- Business classifications (customize based on your requirements)
        case 
            when distance_from_center_km <= 5 then 'Core'
            when distance_from_center_km <= 15 then 'Urban'
            when distance_from_center_km <= 30 then 'Suburban'
            else 'Outer'
        end as zone_type,
        
        -- Metadata
        current_timestamp() as processed_at

    from raw_h3_lookup
),

-- Final staging with quality filters
staged_h3_lookup as (
    select 
        hex_id,
        city,
        resolution,
        centroid_lat,
        centroid_lng,
        district,
        distance_from_center_km,
        hex_area_km2,
        hex_edge_length_km,
        zone_type,
        processed_at
        
    from enriched_h3
    where 
        -- Data quality filters
        is_valid_latitude = true
        and is_valid_longitude = true
        and hex_id is not null
        and city is not null
        and resolution is not null
        and resolution between 0 and 15
)

select * from staged_h3_lookup
