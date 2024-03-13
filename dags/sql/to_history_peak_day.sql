with raw_data as (
select distinct
recording_time::timestamptz as recording_time
, created_at::timestamptz as created_at 
, visitor_peak::int
, male_peak::int
, female_peak::int
, client_id::int
, device_id::int
, zone_id::int
from viseetor_line 
where recording_time::date = %(filter_date)s
)
, to_hour as (
select recording_time, created_at, visitor_peak, male_peak, female_peak, client_id , device_id, zone_id 
, to_char(date_trunc('hour',recording_time AT TIME ZONE 'Asia/Bangkok'),'HH24:MI') || ' - ' || to_char(DATE_TRUNC('hour', recording_time AT TIME ZONE 'Asia/Bangkok') + INTERVAL '1 hour', 'HH24:MI') as "hour"
from raw_data 
)
, get_max as (
select 
max(visitor_peak) as peak_visitor, "hour", client_id , device_id, zone_id 
from to_hour
group by "hour", client_id , device_id , zone_id 
)
, get_first as (
select 
row_number () over (partition by th."hour", th.client_id, th.device_id, th.zone_id, gm.peak_visitor order by th.created_at desc) as rn
, th."hour"
, th.created_at
, gm.peak_visitor
, th.male_peak
, th.female_peak
, th.client_id
, mc."name" as client_name
, th.zone_id
, mz.zone_name 
, th.device_id
from to_hour th
join get_max gm on th."hour" = gm."hour"
	and th.visitor_peak = gm.peak_visitor
	and th.client_id = gm.client_id
	and th.device_id = gm.device_id
	and th.zone_id = gm.zone_id
left join master_client mc on th.client_id = mc.id
left join master_zone mz on th.zone_id = mz.id 
)
, final_result as (
select
 current_timestamp as created_at 
, current_timestamp as updated_at
, "hour"
, peak_visitor
, case 
	when female_peak >=1 then 'female'
end as gender 
, case 
	when female_peak >=1 then female_peak
end as "count"
, client_id 
, client_name
, zone_id 
, zone_name
, device_id 
from get_first
where rn=1
union all 
select 
 current_timestamp as created_at 
, current_timestamp as updated_at
, "hour"
, peak_visitor
, case 
	when male_peak >=1 then 'male'
end as gender 
, case 
	when male_peak >=1 then male_peak
end as "count"
, client_id 
, client_name
, zone_id 
, zone_name
, device_id 
from get_first
where rn=1
)
insert into history_peak_day (client_id, client_name, zone_id, zone_name, "hour", gender, "age", "count", "date",created_at)
select
 client_id
 , client_name 
 , zone_id  
 , zone_name
 , "hour"
 , gender
 , '' as "age"
 , "count"
 , %(filter_date)s as "date"
 , created_at 
from final_result