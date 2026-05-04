create database if not exists flights_project;
use flights_project;

drop table if exists flights;

create external table if not exists flights (
    month bigint,
    fl_date timestamp,
    op_unique_carrier string,
    op_carrier_fl_num double,
    origin string,
    dest string,
    dep_delay double,
    arr_delay double,
    cancelled bigint,
    diverted bigint,
    cancellation_code string,
    carrier_delay bigint,
    weather_delay bigint,
    nas_delay bigint,
    security_delay bigint,
    late_aircraft_delay bigint
)
stored as parquet
location '/user/andy/data/flights_project';

msck repair table flights;

select count(*) as totale_record from flights;