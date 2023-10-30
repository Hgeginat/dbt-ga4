
with screen_name_events as (
    select 
        event_date_dt as date,
        user_key,
        ga_session_id,
        stream_id, 
        user_pseudo_id,
        session_key,
        device_operating_system,
        (select value.string_value from unnest(user_properties) where key = 'polestar_market') as polestar_market,
        (select value.string_value from unnest(user_properties) where key = 'logged_in') as logged_in,
        (select value.string_value from unnest(user_properties) where key = 'is_paired') as is_paired,
        (select value.string_value from unnest(event_params) where key = 'item_category') as item_category,
        (select value.string_value from unnest(event_params) where key = 'content_type') as content_type,
        (select value.string_value from unnest(event_params) where key = 'item_id') as item_id
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
micro_moments as (
    select date,
        user_key,
        ga_session_id,
        stream_id, 
        user_pseudo_id,
        session_key,
        polestar_market,
        logged_in,
        is_paired,
        device_operating_system,
        content_type,
        item_id,
        (case when engagement_time_msec > 0 or session_engaged = 1 then user_key else null end) as active_user_key,
        {{ga4.micro_moment_names_new('item_id')}} as micro_name_article
       
         

       
    from include_derived_session_properties
    where item_category  like 'App:discover' or item_category  like 'App:discover '  )



select * from micro_moments




