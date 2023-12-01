
with screen_name_events as (
    select 
        event_date_dt as date,
        concat(event_date_dt,' ', cast(format("%02d",extract(hour from timestamp_micros(event_timestamp))) as string),':', format("%02d",extract(minute from timestamp_micros(event_timestamp)))) as datetime,
        user_key,
        ga_session_id,
        stream_id, 
        user_pseudo_id,
        session_key,
        device_operating_system,
        app_info_version,
        geo_country,
        (select value.string_value from unnest(user_properties) where key = 'polestar_market') as polestar_market,
        (select value.string_value from unnest(user_properties) where key = 'logged_in') as logged_in,
        (select value.string_value from unnest(user_properties) where key = 'is_paired') as is_paired,
        (select value.string_value from unnest(user_properties) where key = 'owner_type') as owner_type,
        (select value.string_value from unnest(user_properties) where key = 'garage_amount_cars') as garage_amount_cars,
        (select value.string_value from unnest(user_properties) where key = 'is_car_owner') as is_car_owner,
        (select value.string_value from unnest(user_properties) where key = 'car_model') as car_model,
        (select value.string_value from unnest(event_params) where key = 'item_category') as item_category,
        (select value.string_value from unnest(event_params) where key = 'content_type') as content_type,
        (select value.string_value from unnest(event_params) where key = 'item_id') as item_id,
        (select value.string_value from unnest(event_params) where key = 'item_name') as item_name
        
       
    from {{ref('stg_ga4__events')}}
    where event_name = 'select_content'
),




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
    select date,
        datetime,
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
        content_type,
        item_id,
        car_model,
        item_name,
        app_info_version,
        geo_country as country,
        (case when engagement_time_msec > 0 or session_engaged = 1 then user_key else null end) as active_user_key
        

       
    from include_derived_session_properties
    where item_category  like 'App:carcontrol%' and app_info_version ="4.0.0"  )



select * from garage_data


