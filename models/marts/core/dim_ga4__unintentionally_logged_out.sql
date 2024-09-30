-- with events_base as (
--     select 
--         event_name,
--         event_date_dt,
--         user_pseudo_id,
--         user_key,
--         session_key,
--         stream_id,
--         device_operating_system,
--         app_info_version,
--         (select value.string_value from unnest(user_properties) where key = 'polestar_market') as polestar_market,
--         (select value.string_value from unnest(event_params) where key = 'item_category') as item_category,
--         (select value.string_value from unnest(event_params) where key = 'content_type') as content_type,
--         (select value.string_value from unnest(event_params) where key = 'item_id') as item_id,
--     from {{ref('stg_ga4__events')}}
--     where event_name like 'select_content'
-- ),
-- include_derived_session_properties as (
--     select 
--         events_base.*,
--         session_properties.engagement_time_msec,
--         session_properties.session_engaged
--     from events_base
--     {% if var('derived_session_properties', false) %}
--     -- If derived user properties have been assigned as variables, join them on the user_key
--     left join {{ref('stg_ga4__derived_session_properties')}} as session_properties using (session_key)
--     {% endif %}
-- ),

-- final as (
--     select 
--         event_name,
--         content_type,
--         item_category,
--         item_id,
--         event_date_dt as date,
--         polestar_market,
--         (case when engagement_time_msec > 0 or session_engaged = 1 then user_key else null end) as active_user_key,
--         user_key,
--         session_key,
--         device_operating_system,
--         app_info_version
       

--     from include_derived_session_properties 
--     where (LOWER(item_category) like 'app:you:polestarid' and lower(content_type) like 'logged_out_unintentionally')
-- )

-- select * from final

