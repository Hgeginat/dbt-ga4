with events_base2 as (
    select 
        event_name,
        event_date_dt,
        user_pseudo_id,
        active_user_index,
        user_key,
        session_key,
        stream_id,
        device_operating_system,
        (select value.string_value from unnest(user_properties) where key = 'logged_in') as logged_in,
        (select value.string_value from unnest(user_properties) where key = 'is_paired') as is_paired,
        (select value.string_value from unnest(user_properties) where key = 'polestar_market') as polestar_market,
        (select value.string_value from unnest(event_params) where key = 'item_category') as item_category,
        (select value.string_value from unnest(event_params) where key = 'item_id') as item_id,
        (select value.string_value from unnest(event_params) where key = 'car_model') as car_model,
        (select value.string_value from unnest(event_params) where key = 'content_type') as content_type
      

    from {{ref('stg_ga4__events')}}
    where event_name like 'select_content'
),
include_derived_session_properties2 as (
    select 
        events_base2.*,
        session_properties.engagement_time_msec,
        session_properties.session_engaged
    from events_base2
    {% if var('derived_session_properties', false) %}
    -- If derived user properties have been assigned as variables, join them on the user_key
    left join {{ref('stg_ga4__derived_session_properties')}} as session_properties using (session_key)
    {% endif %}
),
app_events2 as (
    select 
        event_date_dt as date,
        event_name,
        content_type,
        car_model,
        item_id,
        item_category,
        {{ga4.car_control_category('content_type')}} as car_control_category,
        polestar_market,
        active_user_index,
        (case when engagement_time_msec > 0 or session_engaged = 1 then user_key else null end) as active_user_key,
        user_key,
        session_key,
        device_operating_system,
        logged_in,
        is_paired
    from include_derived_session_properties2
    where  LOWER(content_type) like 'store_click%'or LOWER(content_type) like 'page_view%' or LOWER(content_type) like 'check_out_click%' or LOWER(content_type) like 'configurator_base_model_click' 

)

select * from app_events2
