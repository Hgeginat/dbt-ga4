WITH first_part_data AS (
  SELECT 
    event_date_dt,
    active_user_key,
    user_pseudo_id,
    ga_session_number,
    --polestar_market, 
    is_car_owner,  
    CASE when is_paired = 'true' THEN 1 ELSE 0 END as is_paired,                                                                                                                                                                                                                                                                                                                                                   
    device_operating_system
  FROM {{ref('dim_ga4__Funnel')}}
),

second_part_data AS (
  SELECT 
    user_pseudo_id,
    active_user_key,
    -- polestar_market,
    device_operating_system,
    MIN(event_date_dt) AS first_time_seen
  FROM first_part_data
  WHERE ga_session_number = 1
  GROUP BY 1, 2, 3
),

third_part_data AS (
  SELECT 
    user_pseudo_id,
    active_user_key,
    -- polestar_market,
    device_operating_system,
    MAX(event_date_dt) AS last_time_seen,
    MAX(is_car_owner) AS is_car_owner,
    MAX(is_paired) as is_paired
  FROM first_part_data
  GROUP BY 1, 2, 3
),

regrouped_data AS (
  SELECT
    f.user_pseudo_id,
    f.active_user_key,
    f.device_operating_system,
    s.first_time_seen,
  FROM first_part_data f
  LEFT JOIN second_part_data s ON f.user_pseudo_id = s.user_pseudo_id
),

regrouped_data_2 AS (
  SELECT
    q.user_pseudo_id,
    q.active_user_key,
    q.device_operating_system,
    q.first_time_seen,
    t.is_car_owner,
    t.is_paired,
    t.last_time_seen
  FROM regrouped_data q
  LEFT JOIN third_part_data t ON q.user_pseudo_id = t.user_pseudo_id
),


max_grouped_data AS (
    SELECT
    user_pseudo_id,
    active_user_key,
    device_operating_system,
    Max(is_car_owner) as is_car_owner,
    MAX(is_paired) as is_paired,
    min(first_time_seen) as first_time_seen,
    max(last_time_seen) As last_time_seen
    from regrouped_data_2
    GROUP BY 1, 2, 3
),

last_part_data as (
    select
    user_pseudo_id,
    active_user_key,
    -- polestar_market,
    device_operating_system,
    is_car_owner,
    is_paired,
    (CASE  WHEN extract(ISOWEEK from first_time_seen)<10 THEN   extract(YEAR from first_time_seen) || "0" || extract(ISOWEEK from first_time_seen) 
           ELSE extract(YEAR from first_time_seen) || "" || extract(ISOWEEK from first_time_seen) END) as first_week_seen,
    (CASE  WHEN extract(ISOWEEK from Last_time_seen) <10 THEN   extract(YEAR from Last_time_seen) || "0" || extract(ISOWEEK from Last_time_seen)
           ELSE extract(YEAR from Last_time_seen) || "" || extract(ISOWEEK from Last_time_seen) END) as last_week_seen,
    first_time_seen as inital_date,
    Last_time_seen as date,
    date_diff(CURRENT_DATE, Last_time_seen, DAY) as diff_days
    from  max_grouped_data),

final_data as (  
select *,
    CASE
        WHEN EXTRACT(MONTH FROM inital_date)<10 THEN ( EXTRACT(YEAR FROM inital_date) || '0' || EXTRACT(MONTH FROM inital_date))
        ELSE (EXTRACT(YEAR FROM inital_date) || '' || EXTRACT(MONTH FROM inital_date)) 
    END  AS Aquisition_Month,

    CASE
        WHEN EXTRACT(MONTH FROM date) <10 THEN ( EXTRACT(YEAR FROM date) || '0' || EXTRACT(MONTH FROM date))
        ELSE (EXTRACT(YEAR FROM date) || '' || EXTRACT(MONTH FROM date)) 
    END  AS Latest_Month

 from last_part_data)

SELECT * FROM final_data
   


