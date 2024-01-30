with
raw_data as (
select 
client_id 
, zone_id 
, gender 
, age 
, object_id 
, duration 
from visitor v
where date(created_at) = %(filter_date)s
)
, get_counts as (
select 
client_id 
, zone_id 
, gender 
, age 
, count(object_id) as "count"
from raw_data
group by 
client_id 
, zone_id 
, gender 
, age
)
, get_dwell_time as (
select 
client_id 
, zone_id 
, gender 
, age 
, avg(duration) as avg_dwell_time
from raw_data
group by 
client_id 
, zone_id 
, gender 
, age 
)
, final_result as (
select 
current_date as "date"
, current_timestamp as created_at 
, gc.client_id 
, mc.name as client_name
, gc.zone_id 
, mz.zone_name as zone_name
, gc.gender 
, gc.age 
, gc."count"
, gd.avg_dwell_time
from get_counts gc 
join get_dwell_time gd on gc.client_id = gd.client_id
	and gc.zone_id = gd.zone_id
	and gc.gender = gd.gender
	and gc.age = gd.age
left join master_client mc on gc.client_id = mc.id
left join master_zone mz on gc.zone_id = mz.id
)
insert into history_state ("date", created_at ,client_id, client_name, zone_id, zone_name, gender, age, "count", avg_dwell_time)
select 
"date"
, created_at 
, client_id 
, client_name
, zone_id 
, zone_name
, gender
, age 
, "count"
, avg_dwell_time
from final_result