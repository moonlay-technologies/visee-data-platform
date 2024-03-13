with raw_data as (
select distinct
recording_time::timestamptz as recording_time
, created_at::timestamptz as created_at 
, visitor_in::int
, male_in::int
, female_in::int
, client_id::int
, device_id::int
, zone_id::int
from viseetor_line 
where camera_type = 'far'
    and created_at::date = %(filter_date)s
)
, get_state as (
select 
max(visitor_in) as visitor_state, client_id , device_id , zone_id 
from raw_data
group by client_id , device_id , zone_id 
)
, get_first as (
select
 row_number() over(partition by gs.visitor_state, rd.client_id, rd.device_id, rd.zone_id order by rd.recording_time desc) as rn
 , rd.recording_time
 , rd.created_at 
 , gs.visitor_state
 , rd.male_in
 , rd.female_in
 , rd.client_id
 , rd.device_id
 , rd.zone_id
from raw_data rd 
join get_state gs on rd.client_id = gs.client_id
	and rd.device_id = gs.device_id
	and rd.zone_id = gs.zone_id
	and rd.visitor_in = gs.visitor_state
)
, get_count as (
select 
 client_id 
 , device_id 
 , zone_id 
 , case 
 	when female_in >=0 then 'female'
 end as gender
 , case 
 	when female_in >=0 then female_in 
 end as "count"
from get_first
where rn =1
union all 
select 
 client_id 
 , device_id 
 , zone_id 
 , case 
 	when male_in >=0 then 'male'
 end as gender
 , case 
 	when male_in >=0 then male_in 
 end as "count"
from get_first
where rn =1
)
, final_result as (
select
 current_timestamp as created_at 
 , current_timestamp as updated_at
 , gc.client_id 
 , mc.name as client_name
 , gc.device_id
 , mz.zone_name 
 , gc.zone_id 
 , gc.gender 
 , '' as "age"
 , gc."count"
 , '00:00:00'::interval as avg_dwelling_time
from get_count gc 
left join master_client mc on gc.client_id = mc.id
left join master_zone mz on gc.zone_id = mz.id
)
insert into monitor_state (created_at, updated_at, client_id, client_name, zone_id, zone_name, gender, "age", "count", avg_dwelling_time)
select 
 created_at 
 , updated_at
 , client_id 
 , client_name 
 , zone_id 
 , zone_name 
--  , device_id 
 , gender 
 , "age"
 , coalesce("count",0)
 , avg_dwelling_time
from final_result