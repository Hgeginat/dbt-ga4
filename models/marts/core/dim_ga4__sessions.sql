-- Dimension table for sessions based on the session_start event.
with session_start_dims as (
    select 
        session_key,
        user_key,
        user_pseudo_id,
        event_date_dt as date,
        geo_continent,
        geo_country,
        geo_region,
        geo_city,
        geo_sub_continent,
        geo_metro,
        stream_id,
        platform,
        device_category,
        device_mobile_brand_name,
        device_mobile_model_name,
        device_mobile_marketing_name,
        device_mobile_os_hardware_model,
        device_operating_system,
        device_operating_system_version,
        device_vendor_id,
        device_advertising_id,
        device_language,
        device_is_limited_ad_tracking,
        device_time_zone_offset_seconds,
        device_browser,
        device_web_info_browser,
        device_web_info_browser_version,
        device_web_info_hostname,
        traffic_source_name,
        traffic_source_medium,
        traffic_source_source,
        ga_session_id,
        ga_session_number,
    from {{ref('stg_ga4__sessions_first_session_start_event')}}
),

include_session_properties as (
    select 
         session_start_dims.*,
         session_properties.session_engaged
    from session_start_dims
    {% if var('derived_session_properties', false) %}
    -- If derived session properties have been assigned as variables, join them on the session_key
    left join {{ref('stg_ga4__derived_session_properties')}} as session_properties using (session_key)
    {% endif %}
),
include_user_properties as (
    select 
        include_session_properties.*,
        user_properties.logged_in,
        user_properties.is_paired,
        user_properties.polestar_market,
        user_properties.first_open_time
    from include_session_properties
    {% if var('user_properties', false) %}
    -- If user properties have been assigned as variables, join them on the user_key
    left join {{ref('stg_ga4__user_properties')}} as user_properties using (user_key)
    {% endif %}
)

select * from include_user_properties
