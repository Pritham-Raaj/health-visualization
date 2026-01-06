{{ config(
    materialized='view',
    schema=env_var('SNOWFLAKE_ANALYTICS_SCHEMA','analytics')
) }}

-- Tableau-optimized view with descriptive labels
WITH base_data AS (
    SELECT * FROM {{ ref('heart_curated') }}
),

enriched_data AS (
    SELECT
        age,
        -- Convert numeric codes to descriptive labels
        CASE sex
            WHEN 'M' THEN 'Male'
            WHEN 'F' THEN 'Female'
            ELSE sex
        END AS gender,
        
        -- Location/Dataset labels
        CASE location
            WHEN 'Cleveland' THEN 'Cleveland, OH'
            WHEN 'Hungary' THEN 'Hungarian Institute'
            WHEN 'Switzerland' THEN 'University Hospital, Zurich'
            WHEN 'VA Long Beach' THEN 'VA Medical Center, Long Beach'
            ELSE location
        END AS study_location,
        
        -- Chest Pain Type descriptions
        CASE chestpaintype
            WHEN 'typical' THEN 'Typical Angina'
            WHEN 'asymptomatic' THEN 'Asymptomatic'
            WHEN 'nonanginal' THEN 'Non-Anginal Pain'
            WHEN 'nontypical' THEN 'Atypical Angina'
            ELSE chestpaintype
        END AS chest_pain_description,
        
        restingbp AS resting_blood_pressure,
        chol AS cholesterol,
        
        -- Fasting Blood Sugar
        CASE fastingbs
            WHEN 1 THEN 'High (>120 mg/dl)'
            WHEN 0 THEN 'Normal (<=120 mg/dl)'
            ELSE 'Unknown'
        END AS fasting_blood_sugar,
        
        -- Resting ECG
        CASE restecg
            WHEN 'normal' THEN 'Normal'
            WHEN 'lv hypertrophy' THEN 'LV Hypertrophy'
            WHEN 'st-t abnormality' THEN 'ST-T Wave Abnormality'
            ELSE restecg
        END AS resting_ecg_result,
        
        max_heartrate AS maximum_heart_rate,
        
        -- Exercise Induced Angina
        CASE exercise_chestpain
            WHEN 1 THEN 'Yes'
            WHEN 0 THEN 'No'
            ELSE 'Unknown'
        END AS exercise_induced_angina,
        
        oldpeak AS st_depression,
        
        -- Heart Disease Result
        CASE result
            WHEN 1 THEN 'Disease Present'
            WHEN 0 THEN 'No Disease'
            ELSE 'Unknown'
        END AS heart_disease_diagnosis,
        
        -- Age groups for analysis
        CASE
            WHEN age < 40 THEN '< 40'
            WHEN age BETWEEN 40 AND 49 THEN '40-49'
            WHEN age BETWEEN 50 AND 59 THEN '50-59'
            WHEN age BETWEEN 60 AND 69 THEN '60-69'
            WHEN age >= 70 THEN '70+'
            ELSE 'Unknown'
        END AS age_group,
        
        -- Cholesterol categories
        CASE
            WHEN chol < 200 THEN 'Desirable (<200)'
            WHEN chol BETWEEN 200 AND 239 THEN 'Borderline High (200-239)'
            WHEN chol >= 240 THEN 'High (>=240)'
            ELSE 'Unknown'
        END AS cholesterol_category,
        
        -- Blood Pressure categories
        CASE
            WHEN restingbp < 120 THEN 'Normal (<120)'
            WHEN restingbp BETWEEN 120 AND 129 THEN 'Elevated (120-129)'
            WHEN restingbp BETWEEN 130 AND 139 THEN 'Stage 1 Hypertension (130-139)'
            WHEN restingbp >= 140 THEN 'Stage 2 Hypertension (>=140)'
            ELSE 'Unknown'
        END AS blood_pressure_category,
        
        -- Risk score calculation (simple scoring system)
        (
            CASE WHEN age > 60 THEN 1 ELSE 0 END +
            CASE WHEN chol >= 240 THEN 1 ELSE 0 END +
            CASE WHEN restingbp >= 140 THEN 1 ELSE 0 END +
            CASE WHEN fastingbs = 1 THEN 1 ELSE 0 END +
            CASE WHEN exercise_chestpain = 1 THEN 1 ELSE 0 END +
            CASE WHEN oldpeak > 2 THEN 1 ELSE 0 END
        ) AS risk_factor_count,
        
        -- Original numeric values for scatter plots
        restingbp,
        chol,
        max_heartrate,
        oldpeak,
        result AS has_disease
        
    FROM base_data
)

SELECT * FROM enriched_data