{{
  config(
    materialized='table',
    description='Time slab dimension table with hour ranges and business period classifications',
    tags=['dimension']
  )
}}

with time_slab_definitions as (
    -- Static time slab definitions
    select slab_name, start_hour, end_hour from values 
        ('0-4', 0, 4),
        ('4-8', 4, 8),
        ('8-12', 8, 12),
        ('12-16', 12, 16),
        ('16-20', 16, 20),
        ('20-24', 20, 24)
    as t(slab_name, start_hour, end_hour)
),

weekday_definitions as (
    -- Static weekday definitions
    select weekday_name, weekday_number, is_weekend from values
        ('Monday', 1, false),
        ('Tuesday', 2, false),
        ('Wednesday', 3, false),
        ('Thursday', 4, false),
        ('Friday', 5, false),
        ('Saturday', 6, true),
        ('Sunday', 7, true)
    as w(weekday_name, weekday_number, is_weekend)
),

-- Cross-join to create all combinations
time_slab_weekday_combinations as (
    select
        ts.slab_name,
        ts.start_hour,
        ts.end_hour,
        wd.weekday_name,
        wd.weekday_number,
        wd.is_weekend
    from time_slab_definitions ts
    cross join weekday_definitions wd
),

-- Add business period classifications
enriched_time_slabs as (
    select 
        -- Primary keys
        slab_name || '_' || weekday_name as slab_weekday_key,
        slab_name,
        weekday_name,
        
        -- Time attributes
        start_hour,
        end_hour,
        end_hour - start_hour as duration_hours,
        weekday_number,
        is_weekend,
        
        -- Business period classifications
        case 
            when slab_name in ('8-12', '12-16') and not is_weekend then 'Business Hours'
            when slab_name in ('16-20') and not is_weekend then 'Evening Rush'
            when slab_name in ('4-8') and not is_weekend then 'Morning Rush'
            when is_weekend and slab_name in ('8-12', '12-16', '16-20') then 'Weekend Active'
            when slab_name in ('20-24', '0-4') then 'Night'
            else 'Off-Peak'
        end as business_period,
        
        -- Traffic intensity estimates (customize based on your city)
        case 
            when slab_name in ('4-8', '16-20') and not is_weekend then 'High'
            when slab_name in ('8-12', '12-16') and not is_weekend then 'Medium-High'
            when is_weekend and slab_name in ('12-16', '16-20') then 'Medium'
            when slab_name in ('20-24', '0-4') then 'Low'
            else 'Medium'
        end as expected_traffic_intensity,
        
        -- Hour range display
        case 
            when start_hour = 0 then '12:00 AM'
            when start_hour < 12 then start_hour || ':00 AM'
            when start_hour = 12 then '12:00 PM'
            else (start_hour - 12) || ':00 PM'
        end || ' - ' ||
        case 
            when end_hour = 0 then '12:00 AM'
            when end_hour <= 12 and end_hour > 0 then 
                case when end_hour = 12 then '12:00 PM' else end_hour || ':00 AM' end
            else (end_hour - 12) || ':00 PM'
        end as hour_range_display,
        
        -- Individual hours in slab (for joining)
        array_construct(
            case when start_hour + 0 < end_hour then start_hour + 0 else null end,
            case when start_hour + 1 < end_hour then start_hour + 1 else null end,
            case when start_hour + 2 < end_hour then start_hour + 2 else null end,
            case when start_hour + 3 < end_hour then start_hour + 3 else null end
        ) as hours_in_slab,
        
        -- Metadata
        current_timestamp() as created_at
        
    from time_slab_weekday_combinations
),

-- Add rush hour flags and seasonal adjustments
final_time_slabs as (
    select 
        *,
        
        -- Rush hour classifications
        case 
            when business_period in ('Morning Rush', 'Evening Rush') then true 
            else false 
        end as is_rush_hour,
        
        case 
            when business_period = 'Business Hours' then true 
            else false 
        end as is_business_hours,
        
        case 
            when business_period = 'Night' then true 
            else false 
        end as is_night_hours,
        
        -- Relative traffic multipliers (baseline = 1.0)
        case 
            when expected_traffic_intensity = 'High' then 1.4
            when expected_traffic_intensity = 'Medium-High' then 1.2
            when expected_traffic_intensity = 'Medium' then 1.0
            when expected_traffic_intensity = 'Low' then 0.7
            else 1.0
        end as traffic_multiplier,
        
        -- Sort order for display
        case 
            when slab_name = '0-4' then 1
            when slab_name = '4-8' then 2
            when slab_name = '8-12' then 3
            when slab_name = '12-16' then 4
            when slab_name = '16-20' then 5
            when slab_name = '20-24' then 6
            else 99
        end as slab_sort_order
        
    from enriched_time_slabs
)

select * from final_time_slabs
order by weekday_number, slab_sort_order
