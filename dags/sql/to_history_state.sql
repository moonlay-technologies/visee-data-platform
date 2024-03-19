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
    and recording_time::date = %(filter_date)s
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
, pre_dwell as (
select 
client_id::int
, zone_id::int
, device_id::int
, gender 
, activity_date_in
, activity_date_out
, (activity_date_out - activity_date_in) as "duration"
from temp_dwell 
where activity_date_in::date = %(filter_date)s
)
, get_dwell as (
select 
client_id
, zone_id
, device_id
, gender 
, avg("duration") as avg_dwell_time
from pre_dwell
group by 
client_id
, zone_id
, device_id
, gender 
)
, final_result as (
select
 current_timestamp as created_at 
 , %(filter_date)s::date as "date"
 , gc.client_id 
 , mc.name as client_name
 , gc.device_id
 , mz.zone_name 
 , gc.zone_id 
 , gc.gender 
 , '' as "age"
 , gc."count"
 , gd.avg_dwell_time
from get_count gc 
left join get_dwell gd on gc.client_id = gd.client_id
	and gc.zone_id = gd.zone_id
	and gc.device_id = gd.device_id
	and gc.gender = gd.gender
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
, coalesce("count",0) as "count"
, avg_dwell_time
from final_result