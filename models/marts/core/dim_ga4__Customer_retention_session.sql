with first_part_data as (

select 
event_date_dt,
user_key,
session_key,
(select value.string_value from unnest(user_properties) where key = 'polestar_market') as polestar_market,
device_operating_system

from {{ref('stg_ga4__events')}}),

second_part_data as (

select 
event_date_dt as date,
user_key,
polestar_market,
device_operating_system,
COUNT(DISTINCT session_key) as amount_sessions

from first_part_data
group by 1,2,3,4)

select * from second_part_data where (amount_sessions >0)
   