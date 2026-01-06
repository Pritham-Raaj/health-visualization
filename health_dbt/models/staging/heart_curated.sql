{{ config(materialized='table', schema=env_var('SNOWFLAKE_ANALYTICS_SCHEMA','analytics')) }}

  with renamed as (
      select
          id,
          age,
          sex,
          location,
          cp as chestpaintype,
          trestbps as restingbp,
          chol,
          fbs as fastingbs,
          restecg,
          thalch as max_heartrate,
          exang as exercise_chestpain,
          oldpeak,
          num
      from {{ ref('health_source') }}
  ),
  filtered as (
      select *
      from renamed
      where age is not null
        and sex is not null
        and location is not null
        and chestpaintype is not null
        and restingbp is not null
        and chol is not null
        and fastingbs is not null
        and restecg is not null
        and max_heartrate is not null
        and exercise_chestpain is not null
        and oldpeak is not null
        and num is not null
  ),
  deduped as (
      select *
      from filtered
      qualify row_number() over (
          partition by age, sex, location, chestpaintype, restingbp, chol,
                       fastingbs, restecg, max_heartrate, exercise_chestpain,
                       oldpeak, num
          order by id
      ) = 1
  )
  select
      age,
      sex,
      location,
      chestpaintype,
      restingbp,
      chol,
      fastingbs,
      restecg,
      max_heartrate,
      exercise_chestpain,
      oldpeak,
      case when num > 0 then 1 else 0 end as result
  from deduped