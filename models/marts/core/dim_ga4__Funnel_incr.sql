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



with users_base as (
    select 
        event_name,
        event_date_dt,
        user_pseudo_id,
        stream_id,
        engagement_time_msec,
        app_info_version,
        (select value.int_value from unnest(event_params) where key = 'engaged_session_event') as engaged_session_event,
        (select value.int_value from unnest(event_params) where key = 'ga_session_id') as ga_session_id,
        (select value.int_value from unnest(event_params) where key = 'ga_session_number') as ga_session_number,
        (select value.string_value from unnest(user_properties) where key = 'polestar_market') as polestar_market,
        (select value.string_value from unnest(user_properties) where key = 'logged_in') as logged_in,
        (select value.string_value from unnest(user_properties) where key = 'is_paired') as is_paired,
        (select value.string_value from unnest(user_properties) where key = 'is_car_owner') as is_car_owner,
        active_user_index,
        device_operating_system,
        geo_country

    from {{ref('base_ga4__events')}}
    
    {% if is_incremental() %}

        {% if var('static_incremental_days', false ) %}
            where event_date_dt in ({{ partitions_to_replace | join(',')}})
        {% else %}
            where event_date_dt >= _dbt_max_partition
        {% endif %}
    {% endif %}),

session_key_added as (
    select 
        users_base.*,
        -- to_base64(md5(CONCAT(stream_id, user_pseudo_id, CAST(ga_session_id as STRING)))) as session_key,
        concat(user_pseudo_id, ga_session_id,  stream_id) as session_key,
        (case when engagement_time_msec > 0 or engaged_session_event = 1 then user_pseudo_id else null end) as active_user_key
        from users_base
),

final_table as (
    select
        event_date_dt,
        user_pseudo_id,
        active_user_index,
        active_user_key,
        session_key,
        CASE when event_name = 'first_open' THEN 1 ELSE 0 END as first_open,
        ga_session_id,
        ga_session_number,
        polestar_market,
        logged_in,
        is_paired,
         CASE when is_car_owner = 'true' THEN 1 ELSE 0 END as is_car_owner,
        device_operating_system,
        app_info_version,
        geo_country
    from session_key_added)
   

select * from final_table