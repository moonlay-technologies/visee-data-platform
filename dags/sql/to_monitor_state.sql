-- Monitor State
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
--where session_id =12
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
, get_dwell_time as (
select 
client_id 
, zone_id 
, gender 
, age 
, avg(duration) as avg_dwelling_time
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
, gc.count_visitor as counts
, gdt.avg_dwelling_time
from get_count gc 
join get_dwell_time gdt on gc.client_id = gdt.client_id
	and gc.zone_id = gdt.zone_id
	and gc.gender = gdt.gender
	and gc.age = gdt.age
)
insert into monitor_state  (created_at,updated_at,client_id,client_name,zone_id,zone_name,gender,age,counts, avg_dwelling_time)
select 
created_at 
, updated_at
, client_id 
, client_name
, zone_id
, zone_name
, gender 
, age 
, counts
, avg_dwelling_time
from final_result
on conflict on constraint monitor_state_uniqe
do update set 
	updated_at = excluded.updated_at
	, counts = excluded.counts
	, avg_dwelling_time = excluded.avg_dwelling_time