with first_part_data AS (
  SELECT 
    event_date_dt,
    active_user_key,
    user_pseudo_id,
    polestar_market,
    device_operating_system
  FROM {{ ref('dim_ga4__Funnel') }}
),

second_part_data AS (
  SELECT 
    active_user_key,
    user_pseudo_id,
    polestar_market,
    device_operating_system,
    MIN(event_date_dt) AS first_time_seen,
    MAX(event_date_dt) AS last_time_seen
  FROM first_part_data
  GROUP BY 1, 2, 3, 4
),

last_part_data AS (
  SELECT
    active_user_key,
    user_pseudo_id,
    polestar_market,
    device_operating_system,
    (CASE 
      WHEN EXTRACT(ISOWEEK FROM first_time_seen) < 10 
      THEN EXTRACT(ISOYEAR FROM first_time_seen) || '0' || EXTRACT(ISOWEEK FROM first_time_seen) 
      ELSE EXTRACT(ISOYEAR FROM first_time_seen) || '' || EXTRACT(ISOWEEK FROM first_time_seen) 
    END) AS first_week_seen,
    (CASE 
      WHEN EXTRACT(ISOWEEK FROM last_time_seen) < 10 
      THEN EXTRACT(ISOYEAR FROM last_time_seen) || '0' || EXTRACT(ISOWEEK FROM last_time_seen) 
      ELSE EXTRACT(ISOYEAR FROM last_time_seen) || '' || EXTRACT(ISOWEEK FROM last_time_seen) 
    END) AS last_week_seen,
    last_time_seen AS date,
    DATE_DIFF(CURRENT_DATE, last_time_seen, DAY) AS diff_days,
    DATE_TRUNC(last_time_seen, MONTH) AS month_group
  FROM second_part_data
)

SELECT 
  *
FROM last_part_data;