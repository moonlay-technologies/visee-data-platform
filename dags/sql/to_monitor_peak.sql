-- Monitor Peak
with
raw_data as (
select 
client_id 
, zone_id 
, gender 
, age 
, concat(
    to_char(%(filter_start)s::TIMESTAMPTZ AT TIME ZONE 'Asia/Bangkok', 'HH24:MI') ||' - '||
    to_char(%(filter_end)s::TIMESTAMPTZ AT TIME ZONE 'Asia/Bangkok', 'HH24:MI')
) as "hour"
, count(object_id) as "count"
from visitor v
where 
"out" >= %(filter_start)s::TIMESTAMPTZ
and 
"out" <= %(filter_end)s::TIMESTAMPTZ
group by
client_id 
, zone_id 
, gender 
, age 
, concat(
    to_char(%(filter_start)s::TIMESTAMPTZ, 'HH24:MI') ||' - '||
    to_char(%(filter_end)s::TIMESTAMPTZ, 'HH24:MI'))
)
, final_result as (
select 
current_timestamp as created_at 
, current_timestamp  as updated_at 
, rw.client_id 
, mc.name as client_name
, rw.zone_id 
, mz.zone_name as zone_name
, rw."hour"
, rw.gender 
, rw."age" 
, rw."count"
from raw_data rw
left join master_client mc on rw.client_id = mc.id
left join  master_zone mz on rw.zone_id = mz.id
)
insert into monitor_peak (created_at, updated_at, client_id, client_name, zone_id, zone_name, "hour", gender, "age", "count")
select 
created_at
, updated_at
, client_id
, client_name
, zone_id
, zone_name
, "hour"
, gender
, "age"
, "count"
from final_result fr
