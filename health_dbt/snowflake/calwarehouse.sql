create role if not exists access;
grant role access to role accountadmin;
create warehouse if not exists cal_wh with warehouse_size="x-small";
grant operate on warehouse cal_wh to role access;
create user if not exists dbt
    password='health123'
    login_name='dbt'
    must_change_password=false
    default_warehouse='cal_wh'
    default_role=access
    default_namespace='healthdata_raw'
    comment='controlled usgae to transform data';
alter user dbt set type=legacy_service;
grant role access to user dbt;
create database if not exists healthdata;
create schema if not exists healthdata.raw;
create schema if not exists healthdata.analytics;
grant all on warehouse cal_wh to role access;
grant all on database healthdata to role access;
grant all on all schemas in database healthdata to role access;
grant all on future schemas in database healthdata to role access;
grant all on all tables in schema healthdata.raw to role access;
grant all on future tables in schema healthdata.raw to role access;c  
