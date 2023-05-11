with first_part_data as (

select 
event_date_dt,
active_user_key,
polestar_market

from {{ref('dim_ga4__Funnel')}}),

second_part_data as (

select 
active_user_key,
polestar_market,
min(event_date_dt) as first_time_seen,
max(event_date_dt) as Last_time_seen

from first_part_data
group by 1,2),

last_part_data as (
    select
    active_user_key,
    polestar_market,
    (CASE  WHEN extract(ISOWEEK from first_time_seen)<10 THEN   extract(ISOYEAR from first_time_seen) || "0" || extract(ISOWEEK from first_time_seen) 
           ELSE extract(ISOYEAR from first_time_seen) || "" || extract(ISOWEEK from first_time_seen) END) as first_week_seen,
    (CASE  WHEN extract(ISOWEEK from Last_time_seen) <10 THEN   extract(ISOYEAR from Last_time_seen) || "0" || extract(ISOWEEK from Last_time_seen)
           ELSE extract(ISOYEAR from Last_time_seen) || "" || extract(ISOWEEK from Last_time_seen) END) as last_week_seen,
    first_time_seen as date
    

  


    from second_part_data
    )

select * from last_part_data 






 

