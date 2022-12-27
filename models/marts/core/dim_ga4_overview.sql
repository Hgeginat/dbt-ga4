with group1 as (
    select 
        event_date_dt as date,
        user_key,
        count(distinct session_key) as nb_sessions,
    from {{ref('stg_ga4__events')}}
    group by 1,2
),

include_user_properties_market as (
    select
     group1.*,
     user_properties.polestar_market
     from group1
     {% if var('user_properties', false) %}
    -- If user properties have been assigned as variables, join them on the user_key
    left join {{ref('stg_ga4__user_properties')}} as user_properties using (user_key)
    {% endif %}
)
     
select * from include_user_properties_market


 