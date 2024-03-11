-- This checks if the variable static_incremental_days is set and is not false. If it's true, it means that incremental loading with specific partitioning is enabled.
-- If incremental loading is enabled, this loop generates a list of partition values to be replaced. 
-- It iterates static_incremental_days times and constructs a list of SQL expressions representing dates in the past, using date_sub to subtract days from the current date.

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
                "field": "event_date_dt",
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
                "field": "event_date_dt",
                "data_type": "date",
            },
        )
    }}
{% endif %}


with events_base as (
    select 
        event_date_dt, 
        event_name,
        user_pseudo_id,
        active_user_index,
        user_key,
        session_key,
        stream_id,
        device_operating_system,
        app_info_version,
        (select value.string_value from unnest(user_properties) where key = 'polestar_market') as polestar_market,
        (select value.string_value from unnest(event_params) where key = 'item_category') as item_category,
        (select value.string_value from unnest(event_params) where key = 'content_type') as content_type,
        (select value.string_value from unnest(event_params) where key = 'item_id') as item_id,
        (select value.string_value from unnest(user_properties) where key = 'logged_in') as logged_in,
        (select value.string_value from unnest(user_properties) where key = 'is_paired') as is_paired
    from {{ref('stg_ga4__events')}}
    where event_name like 'select_content'
),

-- returning all the values of the session_properties 
-- returns all rows from the left table (event base), and the matched rows from the right table (stg_ga4__derived_session_properties). If there's no match, NULL values are returned for the columns from the right table.
include_derived_session_properties as (
    select 
        events_base.*,
        session_properties.engagement_time_msec,
        session_properties.session_engaged
    from events_base
    {% if var('derived_session_properties', false) %}
    -- If derived user properties have been assigned as variables, join them on the session_key
    left join {{ref('stg_ga4__derived_session_properties')}} as session_properties using (session_key)
    {% endif %}
),
app_events as (
    select 
        event_name,
        content_type,
        item_category,
        item_id,
        {{ga4.car_control_category('content_type')}} as car_control_category,
        event_date_dt as date,
        polestar_market,
        logged_in,
        is_paired,
        user_pseudo_id,
        active_user_index,
        (case when engagement_time_msec > 0 or session_engaged = 1 then user_key else null end) as active_user_key,
        user_key,
        session_key,
        device_operating_system,
        app_info_version
        
    from include_derived_session_properties 
    where LOWER(item_category) like 'app:carcontrol%'
)

select * from app_events 
