    with 
    raw_data as (
    select 
    client_id 
    , device_id 
    , session_id 
    , zone_id 
    , object_id 
    , concat(gender,' - ',age) as gender_age
    , created_at 
    , updated_at 
    , cast (confidence as float) as confidence 
    from visitor_raw vr 
    --where object_id in (6969, 7171,8888)
    )
    , count_mode as ( --get mode of gender_age from each object_id
    select 
    client_id 
    , device_id 
    , session_id 
    , zone_id 
    , object_id 
    , gender_age
    , count(gender_age) as mode_count
    from raw_data
    group by 
    client_id 
    , device_id 
    , session_id 
    , zone_id 
    , object_id 
    , gender_age
    )
    , get_best_mode as ( --get the best mode gender_age from each object_id
    select 
    client_id 
    , device_id 
    , session_id 
    , zone_id 
    , object_id 
    , gender_age
    , max(mode_count) as best_mode
    from count_mode
    where mode_count > 1
    group by 
    client_id 
    , device_id 
    , session_id 
    , zone_id 
    , object_id 
    , gender_age
    )
    , elimination1 as ( --elimination 1 to get just 1 best mode gender_age from each object_id
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
    , gender_by_mode as ( -- final gender_age by mode
    select 
    gbm.client_id 
    , gbm.device_id 
    , gbm.session_id 
    , gbm.zone_id
    , gbm.object_id
    , gbm.gender_age
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
    , rd.gender_age
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
        and gm.gender_age = rd.gender_age
    where e1.flag = 'Incorrect'
    group by 
    rd.client_id
    , rd.device_id
    , rd.session_id
    , rd.zone_id
    , rd.object_id
    , rd.gender_age
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
    gs.client_id 
    , gs.device_id 
    , gs.session_id
    , gs.zone_id
    , gs.object_id
    , gs.gender_age
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
    , union_tab as (
    select *
    from gender_by_mode
    union all
    select *
    from gender_by_stdv
    )
    , final_gender as (
    select 
    rd.client_id 
    , rd.device_id 
    , rd.session_id 
    , rd.zone_id
    , rd.object_id
    , rd.gender_age
    , ut.get_by
    , max(rd.confidence) as confidence 
    from union_tab ut
    join raw_data rd on ut.client_id = rd.client_id
        and ut.device_id = rd.device_id
        and ut.session_id = rd.session_id
        and ut.zone_id = rd.zone_id
        and ut.object_id = rd.object_id
        and ut.gender_age = rd.gender_age
    group by 
    rd.client_id 
    , rd.device_id 
    , rd.session_id
    , rd.zone_id
    , rd.object_id
    , rd.gender_age
    , ut.get_by
    )
    , get_time as (
    select 
    client_id 
    , device_id 
    , session_id 
    , zone_id 
    , object_id 
    , min(created_at) as to_inside
    , max(updated_at) as to_outside
    from raw_data
    group by 
    client_id 
    , device_id 
    , session_id 
    , zone_id 
    , object_id 
    )
    , final_query AS (
        SELECT 
            -- row_number() OVER(ORDER BY fg.client_id, fg.device_id, fg.session_id, fg.object_id) AS id,
            fg.client_id,
            fg.device_id,
            fg.session_id,
            fg.object_id,
            fg.zone_id,
            date(gt.to_inside) AS date,
            gt.to_inside,
            gt.to_outside,
            (gt.to_outside - gt.to_inside) AS duration,
            split_part(gender_age, ' - ', 1) AS gender,
            split_part(gender_age, ' - ', 2) AS age,
            fg.confidence
        FROM final_gender fg
        join get_time gt on fg.client_id = gt.client_id
            and fg.device_id = gt.device_id
            and fg.session_id = gt.session_id
            and fg.zone_id = gt.zone_id
            and fg.object_id = gt.object_id
    )
    INSERT INTO visitor (client_id, device_id, session_id, object_id, zone_id, date, to_inside, to_outside, duration, gender, age, confidence)
    SELECT 
        client_id
        ,device_id
        ,session_id
        ,object_id
        ,zone_id
        ,date
        ,to_inside
        ,to_outside
        ,duration
        ,gender
        ,age
        ,confidence
    FROM final_query
    ON CONFLICT on constraint visitor_unique
    DO UPDATE SET 
        to_outside = EXCLUDED.to_outside
        ,duration = EXCLUDED.duration
        ,gender = EXCLUDED.gender
        ,age = EXCLUDED.age
        ,confidence = EXCLUDED.confidence;