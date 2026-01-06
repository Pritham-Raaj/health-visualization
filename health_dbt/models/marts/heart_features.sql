 {{ config(materialized='table', schema=env_var('SNOWFLAKE_ANALYTICS_SCHEMA','analytics')) }}

  select
      age,
      chol,
      max_heartrate,
      oldpeak,
      result,
      dense_rank() over (order by sex) - 1 as sex_label,
      dense_rank() over (order by location) - 1 as location_label,
      dense_rank() over (order by chestpaintype) - 1 as chestpaintype_label,
      dense_rank() over (order by restingbp) - 1 as restingbp_label,
      dense_rank() over (order by fastingbs) - 1 as fastingbs_label,
      dense_rank() over (order by restecg) - 1 as restecg_label,
      dense_rank() over (order by exercise_chestpain) - 1 as exercise_chestpain_label
  from {{ ref('heart_curated') }}