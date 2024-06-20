-- with part1 as (
--     select
--         event_date_dt as date,
--         event_timestamp as  time,
--         event_name,
--         user_pseudo_id,
--         user_key,
--         session_key as session_key,
--         ga_session_id, 
--         app_info_version,
--         device_operating_system,
--         (select value.string_value from unnest(event_params) where key = 'item_category') as item_category,
--         (select value.string_value from unnest(event_params) where key = 'content_type') as content_type,
--         (select value.string_value from unnest(user_properties) where key = 'polestar_market') as polestar_market,
--         (select value.string_value from unnest(user_properties) where key = 'logged_in') as logged_in,
--         (select value.string_value from unnest(user_properties) where key = 'is_paired') as is_paired
--     from {{ref('stg_ga4__events')}}
-- ),

-- part2 as( 
--     select
--     session_key,
--     user_pseudo_id,
--     user_key,
--     app_info_version,
--     device_operating_system,
--     max(logged_in) as logged_in_full_session,
--     max(is_paired) as paired_in_full_session
--     from part1 group by 1,2,3,4,5),

-- part3 as ( 
--     select
--     part1.*,
--     full_session_properties.logged_in_full_session,
--     full_session_properties.paired_in_full_session

--     from part1
--     left join part2 as full_session_properties using (session_key)
-- )


-- include_derived_session_properties as (
--     select 
--         screen_name_events.*,
--         session_properties.engagement_time_msec,
--         session_properties.session_engaged
--     from screen_name_events
--     {% if var('derived_session_properties', false) %}
--     -- If derived user properties have been assigned as variables, join them on the user_key
--     left join {{ref('stg_ga4__derived_session_properties')}} as session_properties using (session_key)
--     {% endif %}
--    )


-- select * from part2    