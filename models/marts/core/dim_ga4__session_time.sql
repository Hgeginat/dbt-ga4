with group1 as (
    select 
        session_key as session_key,
        event_date_dt as date,
        user_key as user_key,
        user_pseudo_id,
        -- event_name as event_name,
        device_operating_system,
        sum((engagement_time_msec))/1000 as engagement_time_sec,
        min (event_timestamp) as first_event_timestamp,
        max(event_timestamp) as last_event_timestamp,
        (max(event_timestamp)- min(event_timestamp))/1000000 as sessiontimesec

    from {{ref('stg_ga4__events')}} 
   
    group by 1,2,3,4,5
)
select * from group1
-- select * from group1 where sessiontimesec > 0 and user_pseudo_id ='0D8AA365C35D49E5ADCC711B660C2C63'

-- include_user_properties_market as (
--     select
--      group1.*,
--      user_properties.polestar_market,
--      user_properties.logged_in,
--      user_properties.is_paired
--      from group1
--      {% if var('user_properties', false) %}
--     -- If user properties have been assigned as variables, join them on the user_key
--     left join {{ref('stg_ga4__user_properties')}} as user_properties using (user_key)
--     {% endif %}
-- ),
-- include_engagement as (
--     select
--     include_user_properties_market.*,
--     session_identifyer.session_engaged as session_eng,
--     session_identifyer.ga_session_id as ga_session_id,
--     session_identifyer.ga_session_number as ga_session_number,
--     from include_user_properties_market
--     left join {{ref('dim_ga4__sessions')}} as session_identifyer using (session_key)

-- ),
-- unduplicate as (
--     select 
--     date,
--     polestar_market,
--     user_pseudo_id,
--     user_key,
--     session_key,
--     ga_session_id,
--     ga_session_number,
--     event_name,
--     session_eng,
--     engagement_time_sec,
--     logged_in,
--     is_paired,
--     device_operating_system,
--     ROW_NUMBER() over(partition by date,
--                                     CAST(engagement_time_sec as STRING),
--                                     polestar_market,
--                                     user_pseudo_id,
--                                     user_key,
--                                     session_key,
--                                     user_key,
--                                     ga_session_id,
--                                     ga_session_number,
--                                     event_name,
--                                     session_eng,
--                                     logged_in,
--                                     is_paired
                                    
--             order by date) AS DuplicateCount
--     from include_engagement
-- )

-- select * from unduplicate 


 