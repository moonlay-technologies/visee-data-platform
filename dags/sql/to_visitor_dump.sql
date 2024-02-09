    with 
    raw_data as (
    select 
    client_id 
    , device_id 
    , session_id ::uuid
    , zone_id 
    , object_id 
    , gender
    , recording_time::TIMESTAMPTZ created_at 
    , recording_time::TIMESTAMPTZ updated_at 
    , cast (confidence as float) as confidence 
    from raw_table 
   where object_id ~ E'^\\d+$'
   and camera_type = 'far'
   and
    (created_at::TIMESTAMPTZ >=  %(filter_start)s::timestamptz
    AND 
    created_at::TIMESTAMPTZ <= %(filter_end)s::timestamptz)
    )
    , count_mode as ( --get mode of gender from each object_id
    select 
    client_id 
    , device_id 
    , session_id 
    , zone_id 
    , object_id 
    , gender
    , count(gender) as mode_count
    from raw_data
    group by 
    client_id 
    , device_id 
    , session_id 
    , zone_id 
    , object_id 
    , gender
    )
    , get_best_mode as ( --get the best mode gender from each object_id
    select 
    client_id 
    , device_id 
    , session_id 
    , zone_id 
    , object_id 
    , gender
    , max(mode_count) as best_mode
    from count_mode
    where mode_count > 1
    group by 
    client_id 
    , device_id 
    , session_id 
    , zone_id 
    , object_id 
    , gender
    )
    , elimination1 as ( --elimination 1 to get just 1 best mode gender from each object_id
    select 
    client_id 
    , device_id 
    , session_id 
    , zone_id 
    , object_id 
    , case
        when count(best_mode) = 1 then 'Correct'
        else 'Incorrect'
    end as flag
    from get_best_mode
    group by 
    client_id 
    , device_id 
    , session_id 
    , zone_id 
    , object_id 
    )
    , gender_by_mode as ( -- final gender by mode
    select 
    gbm.client_id 
    , gbm.device_id 
    , gbm.session_id 
    , gbm.zone_id
    , gbm.object_id
    , gbm.gender
    , e1.flag
    , 'Gender by Mode' as get_by
    from elimination1 e1
    join get_best_mode gbm on e1.client_id = gbm.client_id
        and e1.device_id = gbm.device_id
        and e1.session_id = gbm.session_id
        and e1.zone_id = gbm.zone_id
        and e1.object_id = gbm.object_id
    where e1.flag = 'Correct'
    )
    , get_stdv as ( --the object_id that have same value of the best mode will be process here
    select 
    rd.client_id
    , rd.device_id
    , rd.session_id
    , rd.zone_id
    , rd.object_id
    , rd.gender
    , stddev(rd.confidence) as stdv_conf
    from get_best_mode gm 
    join elimination1 e1 on gm.client_id = e1.client_id
        and gm.device_id = e1.device_id
        and gm.session_id = e1.session_id
        and gm.zone_id = e1.zone_id
        and gm.object_id = e1.object_id
    join raw_data rd on gm.client_id = rd.client_id
        and gm.device_id = rd.device_id
        and gm.session_id = rd.session_id
        and gm.zone_id = rd.zone_id
        and gm.object_id = rd.object_id
        and gm.gender = rd.gender
    where e1.flag = 'Incorrect'
    group by 
    rd.client_id
    , rd.device_id
    , rd.session_id
    , rd.zone_id
    , rd.object_id
    , rd.gender
    )
    , final_stdv as (
    select 
    client_id 
    , device_id 
    , session_id 
    , zone_id
    , object_id 
    , min(stdv_conf) as conf_final
    from get_stdv
    group by 
    client_id 
    , device_id 
    , session_id 
    , zone_id
    , object_id 
    )
    , gender_by_stdv as (
    select
    row_number () over (partition by gs.client_id, gs.device_id, gs.session_id, gs.zone_id, gs.object_id  order by random()) as rn
    , gs.client_id 
    , gs.device_id 
    , gs.session_id
    , gs.zone_id
    , gs.object_id
    , gs.gender
    --, fst.conf_final
    , 'Correct' as flag
    , 'Gender by STDV' as get_by
    from final_stdv fst
    join get_stdv gs on fst.client_id = gs.client_id
        and fst.device_id = gs.device_id
        and fst.session_id = gs.session_id
        and fst.zone_id = gs.zone_id
        and fst.object_id = gs.object_id
        and fst.conf_final = gs.stdv_conf
    )
    , union_tab1 as (
    select client_id,device_id,session_id,zone_id,object_id,gender,flag,get_by
    from gender_by_mode
    union all
    select client_id,device_id,session_id,zone_id,object_id,gender,flag,get_by
    from gender_by_stdv where rn = 1
    )
    , get_single_record as (
    select 
    client_id 
    , device_id 
    , session_id 
    , zone_id 
    , object_id 
    , gender 
    , 'Correct' as flag
    , 'Single Row' as get_by
    from raw_data
    where concat(client_id,'|',device_id,'|',session_id,'|',zone_id,'|',object_id) not in (select concat(client_id,'|',device_id,'|',session_id,'|',zone_id,'|',object_id) from union_tab1)
    )
    , union_tab as (
    select * from union_tab1
    union all
    select * from get_single_record
    )
    , final_gender as (
    select 
    rd.client_id 
    , rd.device_id 
    , rd.session_id 
    , rd.zone_id
    , rd.object_id
    , rd.gender
    , ut.get_by
    , max(rd.confidence) as confidence 
    from union_tab ut
    join raw_data rd on ut.client_id = rd.client_id
        and ut.device_id = rd.device_id
        and ut.session_id = rd.session_id
        and ut.zone_id = rd.zone_id
        and ut.object_id = rd.object_id
        and ut.gender = rd.gender
    group by 
    rd.client_id 
    , rd.device_id 
    , rd.session_id
    , rd.zone_id
    , rd.object_id
    , rd.gender
    , ut.get_by
    )
    , get_time as ( --update max(created_at) as "out"  
    select 
    rw.client_id 
    , rw.device_id 
    , rw.session_id 
    , rw.zone_id 
    , rw.object_id 
    , min(case when v."in" is not null then v."in" else rw.created_at end) as "in"
    , max(rw.created_at) as "out"
    from raw_data rw 
    left join visitor v on rw.client_id::int = v.client_id::int
    	and rw.device_id::int = v.device_id::int
    	and rw.session_id = v.session_id 
    	and rw.zone_id::int = v.zone_id::int
    	and rw.object_id::int = v.object_id::int
    group by 
    rw.client_id 
    , rw.device_id 
    , rw.session_id 
    , rw.zone_id 
    , rw.object_id 
    )
    , final_query AS (
        SELECT 
            current_timestamp::TIMESTAMPTZ as created_at
            , current_timestamp::TIMESTAMPTZ as updated_at
            , fg.client_id::int as client_id
            , fg.device_id::int as device_id
            , fg.session_id
            , fg.object_id::int as object_id
            , fg.zone_id::int as zone_id
            , date(gt."in") AS "date"
            , gt."in"
            , gt."out"
            , (gt."out" - gt."in") AS duration
            , TO_CHAR(
            ("out" AT TIME ZONE 'Asia/Bangkok') - 
            MOD(EXTRACT(MINUTE FROM "out" AT TIME ZONE 'Asia/Bangkok'), 5) * INTERVAL '1 minute',
            'HH24:MI'
        ) || 
        ' - ' || 
        TO_CHAR(
            ("out" AT TIME ZONE 'Asia/Bangkok') + 
            ((5 - MOD(EXTRACT(MINUTE FROM "out" AT TIME ZONE 'Asia/Bangkok'), 5)) * INTERVAL '1 minute'),
            'HH24:MI'
        ) AS "hour"
            , gender
            , '' as age
            , fg.confidence
        FROM final_gender fg
        join get_time gt on fg.client_id::int = gt.client_id::int
            and fg.device_id = gt.device_id
            and fg.session_id = gt.session_id
            and fg.zone_id = gt.zone_id
            and fg.object_id = gt.object_id
    )
    INSERT INTO visitor_dump (created_at, updated_at, client_id, device_id, session_id, object_id, zone_id, "date", "in", "out", duration,"hour",gender, age, confidence)
    SELECT
        created_at 
        ,updated_at
        ,client_id
        ,device_id
        ,session_id
        ,object_id
        ,zone_id
        ,"date"
        ,"in"
        ,"out"
        ,duration
        , "hour"
        ,gender
        ,age
        ,confidence
    FROM final_query
    ON CONFLICT on constraint visitor_dump_unq --(client_id, device_id, session_id, object_id, zone_id, "date")
    DO nothing 