-- Monitor Peak
with
raw_data as (
select 
client_id 
, zone_id 
, gender 
, age 
, object_id
--
, "hour"
from visitor_dump
where "date" = %(filter_start)s::date
--from visitor
)
, get_count as (
select 
client_id
, zone_id
, gender
, age
, "hour"
, count(object_id) as "count"
from raw_data
group by 
"hour"
, client_id
, zone_id
, gender
, age
)
, final_result as (
select 
current_timestamp as created_at 
, current_timestamp  as updated_at 
, gc.client_id 
, mc.name as client_name
, gc.zone_id 
, mz.zone_name as zone_name
, gc."hour"
, gc.gender 
, gc."age" 
, gc."count"
from get_count gc
left join master_client mc on gc.client_id = mc.id
left join  master_zone mz on gc.zone_id = mz.id
order by gc."hour"
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
-- on conflict on constraint monitor_peak_unq
-- do NOTHING
-- 	updated_at = EXCLUDED.updated_at
-- 	,"count" = EXCLUDED."count"