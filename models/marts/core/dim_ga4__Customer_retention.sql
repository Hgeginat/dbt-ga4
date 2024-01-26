WITH first_part_data AS (
  SELECT 
    event_date_dt,
    active_user_key,
    user_pseudo_id,
    ga_session_number,
    -- polestar_market,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
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
    MAX(event_date_dt) AS last_time_seen
  FROM first_part_data
  GROUP BY 1, 2, 3
),

regrouped_data AS (
  SELECT
    s.user_pseudo_id,
    s.active_user_key,
    s.device_operating_system,
    s.first_time_seen,
    t.last_time_seen
  FROM second_part_data s
  LEFT JOIN third_part_data t ON s.user_pseudo_id = t.user_pseudo_id
),


max_grouped_data AS (
    SELECT
    user_pseudo_id,
    active_user_key,
    device_operating_system,
    min(first_time_seen) as first_time_seen,
    max(last_time_seen) As last_time_seen
    from regrouped_data
    GROUP BY 1, 2, 3
),

max_grouped_data_1 AS (
    SELECT
    f.user_pseudo_id,
    f.active_user_key,
    f.device_operating_system,
    m.first_time_seen,
    m.last_time_seen
    from first_part_data f
    LEFT JOIN max_grouped_data m  ON f.user_pseudo_id = m.user_pseudo_id

),

last_part_data as (
    select
    user_pseudo_id,
    active_user_key,
    -- polestar_market,
    device_operating_system,
    (CASE  WHEN extract(ISOWEEK from first_time_seen)<10 THEN   extract(ISOYEAR from first_time_seen) || "0" || extract(ISOWEEK from first_time_seen) 
           ELSE extract(ISOYEAR from first_time_seen) || "" || extract(ISOWEEK from first_time_seen) END) as first_week_seen,
    (CASE  WHEN extract(ISOWEEK from Last_time_seen) <10 THEN   extract(ISOYEAR from Last_time_seen) || "0" || extract(ISOWEEK from Last_time_seen)
           ELSE extract(ISOYEAR from Last_time_seen) || "" || extract(ISOWEEK from Last_time_seen) END) as last_week_seen,
    first_time_seen as inital_date,
    Last_time_seen as date,
    date_diff(CURRENT_DATE, Last_time_seen, DAY) as diff_days
    from  max_grouped_data_1),

final_data as (  
select *,
    CASE
        WHEN EXTRACT(MONTH FROM inital_date)<10 THEN ( EXTRACT(ISOYEAR FROM inital_date) || '0' || EXTRACT(MONTH FROM inital_date))
        ELSE (EXTRACT(ISOYEAR FROM inital_date) || '' || EXTRACT(MONTH FROM inital_date)) 
    END  AS Aquisition_Month,

    CASE
        WHEN EXTRACT(MONTH FROM date) <10 THEN ( EXTRACT(ISOYEAR FROM date) || '0' || EXTRACT(MONTH FROM date))
        ELSE (EXTRACT(ISOYEAR FROM date) || '' || EXTRACT(MONTH FROM date)) 
    END  AS Latest_Month

 from last_part_data)

SELECT * FROM final_data
   


