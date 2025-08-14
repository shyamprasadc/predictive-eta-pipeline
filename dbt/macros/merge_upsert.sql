{% macro merge_upsert(target_table, source_table, unique_key, update_columns=none, insert_columns=none, where_condition=none) %}
  {#
    Generic MERGE macro for Snowflake upsert operations
    
    Args:
      target_table: Name of the target table to merge into
      source_table: Name of the source table/view to merge from  
      unique_key: Column(s) to match on (string or list)
      update_columns: Columns to update on match (defaults to all except unique_key)
      insert_columns: Columns to insert on no match (defaults to all)
      where_condition: Optional WHERE clause for source filtering
  #}
  
  {%- set unique_key_list = unique_key if unique_key is iterable and unique_key is not string else [unique_key] -%}
  
  {%- if not update_columns -%}
    {%- set update_columns = adapter.get_columns_in_relation(ref(source_table)) | map(attribute='name') | reject('in', unique_key_list) | list -%}
  {%- endif -%}
  
  {%- if not insert_columns -%}
    {%- set insert_columns = adapter.get_columns_in_relation(ref(source_table)) | map(attribute='name') | list -%}
  {%- endif -%}
  
  merge into {{ target_table }} as target
  using (
    select * from {{ source_table }}
    {% if where_condition -%}
    where {{ where_condition }}
    {%- endif %}
  ) as source
  on (
    {%- for key in unique_key_list %}
    target.{{ key }} = source.{{ key }}
    {%- if not loop.last %} and {% endif -%}
    {%- endfor %}
  )
  when matched then
    update set
    {%- for column in update_columns %}
      {{ column }} = source.{{ column }}
      {%- if not loop.last %},{% endif -%}
    {%- endfor %},
      updated_at = current_timestamp()
  when not matched then
    insert (
      {%- for column in insert_columns %}
      {{ column }}{% if not loop.last %},{% endif %}
      {%- endfor %}
    )
    values (
      {%- for column in insert_columns %}
      source.{{ column }}{% if not loop.last %},{% endif %}
      {%- endfor %}
    )

{% endmacro %}


{% macro create_or_replace_table_indexes() %}
  {#
    Create performance indexes for fact tables
    Snowflake uses clustering keys instead of traditional indexes
  #}
  
  {% if model.name == 'fct_eta_hex_pair' %}
    alter table {{ this }} cluster by (from_hex, to_hex, weekday, time_slab);
  {% endif %}
  
{% endmacro %}


{% macro analyze_table() %}
  {#
    Update table statistics (Snowflake equivalent)
  #}
  
  -- Snowflake automatically maintains statistics, but we can force a refresh
  {% if target.type == 'snowflake' %}
    select system$clustering_information('{{ this }}', '(from_hex, to_hex, weekday, time_slab)');
  {% endif %}
  
{% endmacro %}


{% macro optimize_table() %}
  {#
    Optimize table for better performance
  #}
  
  {% if target.type == 'snowflake' %}
    -- Snowflake optimization through clustering
    {% if model.name in ['fct_eta_hex_pair', 'dim_time_slab'] %}
      alter table {{ this }} resume recluster;
    {% endif %}
  {% endif %}
  
{% endmacro %}


{% macro generate_eta_slabs_final() %}
  {#
    Generate the final ETA_SLABS table by merging from fact table
    This macro creates the production table that serves the API
  #}
  
  create or replace table {{ target.schema }}.eta_slabs as
  select 
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
  from {{ ref('fct_eta_hex_pair') }}
  where data_quality_score >= 40  -- Only include high-quality data
    and sample_count >= {{ var('min_sample_count') }};
  
  -- Add clustering for performance
  alter table {{ target.schema }}.eta_slabs 
  cluster by (from_hex, to_hex, weekday, slab);
  
{% endmacro %}


{% macro validate_eta_consistency() %}
  {#
    Custom test macro to validate ETA consistency across models
  #}
  
  with consistency_check as (
    select 
      from_hex,
      to_hex,
      weekday,
      time_slab,
      case 
        when min_eta_s > max_eta_s then 'min_greater_than_max'
        when avg_eta_s < min_eta_s or avg_eta_s > max_eta_s then 'avg_outside_range'
        when rain_eta_s < max_eta_s then 'rain_less_than_max'
        else 'valid'
      end as consistency_status
    from {{ ref('fct_eta_hex_pair') }}
  )
  
  select *
  from consistency_check 
  where consistency_status != 'valid'
  
{% endmacro %}


{% macro get_model_freshness(model_name, max_age_hours=24) %}
  {#
    Check data freshness for a model
  #}
  
  select 
    '{{ model_name }}' as model_name,
    max(updated_at) as last_updated,
    datediff('hour', max(updated_at), current_timestamp()) as hours_since_update,
    case 
      when datediff('hour', max(updated_at), current_timestamp()) > {{ max_age_hours }} 
      then 'stale' 
      else 'fresh' 
    end as freshness_status
  from {{ ref(model_name) }}
  where updated_at is not null
  
{% endmacro %}
