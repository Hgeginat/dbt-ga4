with export1 as (


SELECT
event_date_dt, 
device_category,
device_mobile_brand_name,
device_mobile_model_name,
device_mobile_marketing_name,
device_operating_system,
device_operating_system_version,
(select value.string_value from unnest(user_properties) where key = 'polestar_market') as polestar_market,
user_key
from {{ref('stg_ga4__events')}}
where event_date_dt BETWEEN '2022-06-01' AND '2023-06-01'),

export2 as (
    select
    polestar_market,
    device_category,
    device_operating_system,
    device_mobile_brand_name,
    device_mobile_model_name,
    device_mobile_marketing_name,
    Count(distinct(user_key)) as Total_users
    
    from export1
    group by 1,2,3,4,5,6
)

select * from export2
