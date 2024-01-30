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
-- where 
-- updated_at >= %(filter_start)s::TIMESTAMPTZ
-- and 
-- updated_at <= %(filter_end)s ::TIMESTAMPTZ
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
, mc.name as client_name
, gc.zone_id
, mz.zone_name as zone_name
, gc.gender
, gc.age
, gc.count_visitor as count
, gdt.avg_dwelling_time
from get_count gc 
join get_dwell_time gdt on gc.client_id = gdt.client_id
	and gc.zone_id = gdt.zone_id
	and gc.gender = gdt.gender
	and gc.age = gdt.age
left join master_client mc on gc.client_id = mc.id
left join master_zone mz on gc.zone_id = mz.id
)
insert into monitor_state  (created_at,updated_at,client_id,client_name,zone_id,zone_name,gender,age,count, avg_dwelling_time)
select 
created_at 
, updated_at
, client_id 
, client_name
, zone_id
, zone_name
, gender 
, age 
, count
, avg_dwelling_time
from final_result
on CONFLICT on constraint monitor_state_conflict
DO UPDATE SET
	updated_at = EXCLUDED.updated_at
	, count = EXCLUDED.count
	, avg_dwelling_time = EXCLUDED.avg_dwelling_time