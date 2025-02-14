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





with group1 as (
    select 
        session_key as session_key,
        event_date_dt as date,
        user_key as user_key,
        user_pseudo_id,
        device_operating_system,
        -- sum((engagement_time_msec))/1000 as engagement_time_sec,
        min (event_timestamp) as first_event_timestamp,
        max(event_timestamp) as last_event_timestamp,
        (max(event_timestamp)- min(event_timestamp))/1000000 as sessiontimesec

    from {{ref('stg_ga4__events')}} 
    group by 1,2,3,4, 5
   
),
-- select * from group1 where sessiontimesec > 0


include_user_properties_market as (
    select
     group1.*,
     user_properties.polestar_market,
     user_properties.logged_in,
     user_properties.is_paired
     from group1
     {% if var('user_properties', false) %}
    -- If user properties have been assigned as variables, join them on the user_key
    left join {{ref('stg_ga4__user_properties')}} as user_properties using (user_key)
    {% endif %}
)


select * from include_user_properties_market  where sessiontimesec > 0
     {% if is_incremental() %}

        {% if var('static_incremental_days', false ) %}
            and date in ({{ partitions_to_replace | join(',')}})
        {% else %}
            and date >= _dbt_max_partition
        {% endif %}
    {% endif %}
   
    





