{% if var('static_incremental_days', false ) %} -- if true, this part of the code will be running only if the variable static_incremental_days exist. if it does not exist, the default value of it will be false. Therefore, the code in the block will not run
    {% set partitions_to_replace = [] %} --  Initializes an empty list named partitions_to_replace aim :  This list is intended to store partition values for incremental loading.

    {% for i in range(var('static_incremental_days')) %}   -- loop for 0 to incrementalsday-1
        {% set partitions_to_replace = partitions_to_replace.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %} -- getting a list of date where we remove the amount of days from 0 to incrementals days-1
    {% endfor %}


 --  config : This function generates configurations for the data loading process. so the first part here is configuring the upload 
    {{
        config(                             
            materialized = 'incremental',
            incremental_strategy = 'insert_overwrite',
            partition_by={
                "field": "date",
                "data_type": "date",
            },
            partitions = partitions_to_replace,
        )
    }}
{% else %}

--meaning her no 'static_incremental_days' value, and therefore incremental update over here
    {{
        config(
            materialized = 'incremental',
            incremental_strategy = 'insert_overwrite',
            partition_by={
                "field": "date",
                "data_type": "date",
            },
        )
    }}
{% endif %}

with screen_name_events2 as (
    select
        event_date_dt,
        TIMESTAMP_MICROS(event_timestamp) as  time,
        event_name,
        user_key,
        ga_session_id,
        stream_id, 
        user_pseudo_id,
        session_key,
        app_info_version,
        (select value.string_value from unnest(event_params) where key = 'item_category') as item_category,
        (select value.string_value from unnest(event_params) where key = 'content_type') as content_type,
        (select value.string_value from unnest(user_properties) where key = 'polestar_market') as polestar_market,
        (select value.string_value from unnest(user_properties) where key = 'logged_in') as logged_in,
        (select value.string_value from unnest(user_properties) where key = 'is_paired') as is_paired
    from {{ref('stg_ga4__events')}}
    {% if is_incremental() %}

        {% if var('static_incremental_days', false ) %}
            and event_date_dt in ({{ partitions_to_replace | join(',')}})
        {% else %}
            and event_date_dt >= _dbt_max_partition
        {% endif %}
    {% endif %}
),

include_derived_session_properties2 as (
    select 
        screen_name_events2.*,
        session_properties.engagement_time_msec,
        session_properties.session_engaged
    from screen_name_events2 
    {% if var('derived_session_properties', false) %}
    -- If derived user properties have been assigned as variables, join them on the user_key
    left join {{ref('stg_ga4__derived_session_properties')}} as session_properties using (session_key)
    {% endif %}
),
Journey as (
    select event_date_dt as date,
        time,
        event_name,
        user_key,
        ga_session_id,
        stream_id, 
        user_pseudo_id,
        session_key,
        engagement_time_msec,
        session_engaged,
        item_category,
        content_type,
        polestar_market,
        logged_in,
        is_paired,
        app_info_version,
       (case when engagement_time_msec > 0 or session_engaged = 1 then user_key else null end) as active_user_key
    
    from include_derived_session_properties2
    where logged_in in ('false','null')
)

select * from Journey