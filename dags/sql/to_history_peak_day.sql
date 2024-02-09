with
raw_data as (
select 
client_id 
, zone_id 
, gender 
, age 
, object_id 
, to_char(date_trunc('hour', "in" AT TIME ZONE 'Asia/Bangkok'),'HH24:MI') || ' - ' || to_char((date_trunc('hour', "in" AT TIME ZONE 'Asia/Bangkok') + INTERVAL '1 hour'), 'HH24:MI') as "hour"
from visitor_dump
where "date" = %(filter_date)s
)
, get_counts as (
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
%(filter_date)s::date as "date"
, gc.client_id 
, mc.name as client_name
, gc.zone_id 
, mz.zone_name as zone_name
,  "hour"
, gc.gender 
, gc.age 
, gc."count"
from get_counts gc 
left join master_client mc on gc.client_id = mc.id
left join master_zone mz on gc.zone_id = mz.id
)
insert into history_peak_day ("date", client_id, client_name, zone_id, zone_name, "hour", gender, age, "count")
select 
"date"
, client_id 
, client_name
, zone_id 
, zone_name
, "hour"
, gender
, age 
, "count"
from final_result