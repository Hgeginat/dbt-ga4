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


with screen_name_events as (
    select 
        event_date_dt,
        user_key,
        ga_session_id,
        stream_id, 
        user_pseudo_id,
        session_key,
        device_operating_system,
        app_info_version,
        active_user_index,
        (select value.string_value from unnest(user_properties) where key = 'polestar_market') as polestar_market,
        (select value.string_value from unnest(user_properties) where key = 'logged_in') as logged_in,
        (select value.string_value from unnest(user_properties) where key = 'is_paired') as is_paired,
        (select value.string_value from unnest(user_properties) where key = 'owner_type') as owner_type,
        CASE 
            WHEN (select value.string_value from unnest(user_properties) where key = 'garage_amount_cars') IS NOT NULL
                THEN (select value.string_value from unnest(user_properties) where key = 'garage_amount_cars')
            ELSE 
                (select value.string_value from unnest(event_params) where key = 'garage_amount_cars')
        END as garage_amount_cars,
        (select value.string_value from unnest(user_properties) where key = 'is_car_owner') as is_car_owner,
        (select value.string_value from unnest(user_properties) where key = 'car_model') as car_model,
        (select value.string_value from unnest(event_params) where key = 'item_category') as item_category,
        (select value.string_value from unnest(event_params) where key = 'content_type') as content_type,
        (select value.string_value from unnest(event_params) where key = 'item_id') as item_id
        
       
    from {{ref('stg_ga4__events')}}
    where event_name = 'select_content'
    {% if is_incremental() %}

        {% if var('static_incremental_days', false ) %}
            and event_date_dt in ({{ partitions_to_replace | join(',')}})
        {% else %}
            and event_date_dt >= _dbt_max_partition
        {% endif %}
    {% endif %}),





include_derived_session_properties as (
    select 
        screen_name_events.*,
        session_properties.engagement_time_msec,
        session_properties.session_engaged
    from screen_name_events
    {% if var('derived_session_properties', false) %}
    -- If derived user properties have been assigned as variables, join them on the user_key
    left join {{ref('stg_ga4__derived_session_properties')}} as session_properties using (session_key)
    {% endif %}
   )
,
garage_data as (
    select event_date_dt as date,
        user_key,
        ga_session_id,
        stream_id, 
        user_pseudo_id,
        session_key,
        polestar_market,
        logged_in,
        is_paired,
        is_car_owner,
        owner_type,
        CAST(garage_amount_cars AS INT) AS garage_amount_cars,
        item_category,
        device_operating_system,
        app_info_version,
        content_type,
        item_id,
        car_model,
        (case when engagement_time_msec > 0 or session_engaged = 1 then user_key else null end) as active_user_key,
        active_user_index
        

       
    from include_derived_session_properties
    where (item_category  like 'App:garage') or (item_category  like 'App:carcontrol%' and content_type='pairing_success'))



select * from garage_data


