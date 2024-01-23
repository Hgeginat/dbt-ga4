with first_part_data as (

select 
event_date_dt,
active_user_key,
user_pseudo_id,
polestar_market,
device_operating_system

from {{ref('dim_ga4__Funnel')}}),

second_part_data as (

select 
active_user_key,
user_pseudo_id,
polestar_market,
device_operating_system,
min(event_date_dt) as first_time_seen,
max(event_date_dt) as Last_time_seen

from first_part_data
group by 1,2,3,4),

last_part_data as (
    select
    active_user_key,
    user_pseudo_id,
    polestar_market,
    device_operating_system,
    (CASE  WHEN extract(ISOWEEK from first_time_seen)<10 THEN   extract(ISOYEAR from first_time_seen) || "0" || extract(ISOWEEK from first_time_seen) 
           ELSE extract(ISOYEAR from first_time_seen) || "" || extract(ISOWEEK from first_time_seen) END) as first_week_seen,
    (CASE  WHEN extract(ISOWEEK from Last_time_seen) <10 THEN   extract(ISOYEAR from Last_time_seen) || "0" || extract(ISOWEEK from Last_time_seen)
           ELSE extract(ISOYEAR from Last_time_seen) || "" || extract(ISOWEEK from Last_time_seen) END) as last_week_seen,
    DATE_TRUNC(first_time_seen, MONTH) AS Aquisition_Month,
    DATE_TRUNC(Last_time_seen, MONTH) AS Latest_Month,
    first_time_seen as inital_date,
    Last_time_seen as date,
    date_diff(CURRENT_DATE, Last_time_seen, DAY) as diff_days
   
    

  


    from second_part_data
    )

select *,
    CASE
        WHEN EXTRACT(MONTH FROM Aquisition_Month)<10 THEN ( EXTRACT(ISOYEAR FROM Aquisition_Month) || '0' || EXTRACT(MONTH FROM Aquisition_Month))
        ELSE (EXTRACT(ISOYEAR FROM Aquisition_Month) || '' || EXTRACT(MONTH FROM Aquisition_Month)) 
    END  AS Aquisition_Month,

    CASE
        WHEN EXTRACT(MONTH FROM Latest_Month)<10 THEN ( EXTRACT(ISOYEAR FROM Latest_Month) || '0' || EXTRACT(MONTH FROM Latest_Month))
        ELSE (EXTRACT(ISOYEAR FROM Latest_Month) || '' || EXTRACT(MONTH FROM Latest_Month)) 
    END  AS Latest_Month

 from last_part_data 






 

