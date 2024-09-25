with grouped_data as (
	select 
		id_dynamo
		, created_at::timestamptz
		, date_trunc('minute', to_timestamp(created_at, 'YYYY-MM-DD HH24:MI:SS')) - interval '1 minute' * (extract(minute from to_timestamp(created_at, 'YYYY-MM-DD HH24:MI:SS'))::int % 5) as interval_5min
		, female_peak 
		, male_peak 
		, client_id 
		, zone_id 
		, recording_time::timestamptz
	from viseetor_line
	--where created_at between '2024-09-24 11:30:00' and '2024-09-24 11:59:59'
)
, female_mode as (
	select 
		interval_5min 
		, mode () within group (order by female_peak) as mode
		, client_id 
		, zone_id 
		, 'female' as gender
	from grouped_data
	group by 
		interval_5min
		, client_id 
		, zone_id 
		, gender
) 
, male_mode as (
	select 
		interval_5min 
		, mode () within group (order by male_peak) as mode
		, client_id 
		, zone_id 
		, 'male' as gender
	from grouped_data
	group by 
		interval_5min
		, client_id 
		, zone_id 
		, gender
) 
, mode_combined as (
	select * from female_mode
	union
	select * from male_mode
) 
insert into live_visitor (created_at, client_id, zone_id, count, gender, record_time)
select distinct on (gd.interval_5min, gd.client_id, gd.zone_id)
	gd.created_at
	, gd.client_id
	, gd.zone_id
	, mc.mode as count
	, mc.gender
	, gd.recording_time as record_time
from grouped_data gd
join mode_combined mc 
on gd.interval_5min = mc.interval_5min
	and gd.client_id = mc.client_id
	and gd.zone_id = mc.zone_id;