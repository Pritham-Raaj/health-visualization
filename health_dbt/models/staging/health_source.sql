with healthdata_raw as(
    select * from {{ source('raw', 'raw_health') }} 
)
select
    id,
    age,
    sex,
    dataset as location,
    cp, 
    trestbps,
    chol,
    fbs,
    restecg,
    thalch,
    exang,
    oldpeak,
    slope,
    ca,
    thal,
    num
from healthdata_raw