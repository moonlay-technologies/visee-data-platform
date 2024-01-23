-- Monitor Peak
with
raw_data as (
select 
client_id 
, zone_id 
, gender 
, age 
, object_id 
, duration 
, created_at
, updated_at
, concat(to_char(created_at,'HH24:MI') ||' - '|| to_char(updated_at,'HH24:MI')) as "hour"
from visitor v
where 
created_at >= %(filter_start)s::TIMESTAMP
and 
updated_at <= %(filter_end)s ::TIMESTAMP
)
, get_count as (
select 
client_id 
, zone_id 
, gender 
, age 
, count(object_id) as count_visitor
from raw_data 
group by 
client_id
, zone_id 
, gender 
, age 
)
, final_result as (
select 
current_timestamp as created_at 
, current_timestamp as updated_at 
, gc.client_id
, 'ABC' as client_name
, gc.zone_id
, 'Zone' as zone_name
, gc.gender
, gc.age
, rw."hour"
, gc.count_visitor as counts
from get_count gc 
inner join raw_data rw on gc.client_id = rw.client_id
	and gc.zone_id = rw.zone_id
	and gc.gender = rw.gender
	and gc.age = rw.age
)
insert into monitor_peak (created_at, updated_at, client_id, client_name, zone_id, zone_name, gender, age, "hour", counts)
select 
created_at 
, updated_at
, client_id 
, client_name
, zone_id
, zone_name
, "hour" 
, gender 
, age 
, counts
from final_result