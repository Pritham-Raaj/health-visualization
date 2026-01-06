use warehouse cal_wh;
use database healthdata;
use schema healthdata.raw;

set aws_key_id = '{{ env_var("AWS_ACCESS_KEY_ID") }}';
set aws_secret = '{{ env_var("AWS_SECRET_ACCESS_KEY") }}';
set bucket = '{{ env_var("S3_BUCKET") }}';
  create stage if not exists healthstage
      url = concat('s3://', $bucket)
      credentials=(aws_key_id=$aws_key_id aws_secret_key=$aws_secret);

CREATE OR REPLACE TABLE raw_health (
    id INTEGER,
    age INTEGER,
    sex VARCHAR(10),
    dataset VARCHAR(50),
    cp VARCHAR(50),  -- chest pain type
    trestbps INTEGER,  -- resting blood pressure
    chol INTEGER,  -- cholesterol
    fbs BOOLEAN,  -- fasting blood sugar
    restecg VARCHAR(50),  -- resting ECG
    thalch INTEGER,  -- max heart rate
    exang BOOLEAN,  -- exercise induced angina
    oldpeak FLOAT,  -- ST depression
    slope VARCHAR(50),
    ca INTEGER,  -- number of major vessels
    thal VARCHAR(50),  -- thalassemia
    num INTEGER  -- diagnosis (0-4)
)
CLUSTER BY (dataset, num);

copy into raw_health
from '@healthstage/heart_disease_uci.csv'
file_format= (type= 'CSV' skip_header= 1 field_optionally_enclosed_by= '"' ); 
