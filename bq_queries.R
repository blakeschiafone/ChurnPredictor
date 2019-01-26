query.models <- function(){
glue::glue("select
            *
            from
            (select
            *,
            max(modified) over (partition by namespace, onboarding, return) as max_mod
            from
            (select
            (table_id) as namespace,
            modified,
            onboarding,
            return,
            user_lifetime_median,
            user_lifetime_mean,
            def_1,
            def_2,
            def_3,
            def_4,
            def_5,
            def_6
            def_7,
            def_8,
            def_9
            from table_query([{project}:kahuna_churn_predictor], \"not table_id contains '_event' and not table_id contains '_session'\")))
            where modified = max_mod", project = Sys.getenv('PROJECT'))
}


query.user_session <- function(namespace='8tracks', onboarding=0, retention=1, date_begin='2017-07-01'){
  glue::glue("with user_dates as (select
             userid,
             device_type,
             (first_seen) as begin_onboarding,
             date_add(first_seen, interval {onboarding} day) as end_onboarding,
             date_add(first_seen, interval {retention} day) as end_retention
             from
             (select
             *,
             min(dates_seen) over (partition by userid, device_type) as first_seen,
             row_number() over (partition by userid order by dates_seen) as row_number
             from
             (select
             if(two.user_merged_id is not null, two.user_id, one.user_id) as userid,
             device_type,
             dates_seen
             from
             (select
             user_id,
             (case when dev_id like '%-%' then 'android' else 'ios' end) as device_type,
             date(time) as dates_seen
             from `{project}.kahuna_event.{namespace}` 
             where date(time) >= '{date_begin}'
             group by 1, 2, 3) as one
             
             left join
             
             (select
             user_id,
             user_merged_id
             from `{project}.kahuna_user_merge.{namespace}_*`) as two
             on one.user_id = two.user_merged_id))
             where row_number = 1
             group by 1, 2, 3, 4, 5),
             
             
             user_session as (select
             *
             from
             (select
             userid,
             min(time) over (partition by userid, user_session_id) as session_start_time,
             #global_session_id,
             user_session_id,
             round(timestamp_diff(max(time) over (partition by userid, user_session_id), min(time) over (partition by userid, user_session_id), second) / 60) as session_minutes
             from
             (select
             userid,
             time,
             #sum(session_flag) over (order by userid, time) as global_session_id,
             sum(session_flag) over (partition by userid order by time) as user_session_id
             from
             (select
             *,
             (case when (timestamp_diff(time, lag_event, minute) > 10 or lag_event is null) then 1 else 0 end) as session_flag
             from
             (select
             *,
             lag(time) over (partition by userid order by time) as lag_event
             from
             (select
             if(two.user_merged_id is not null, two.user_id, one.user_id) as userid,
             time
             from
             (select
             user_id,
             time
             from `{project}.kahuna_event.{namespace}`
             where not event like 'k_%' and
             date(time) >= '{date_begin}') as one
             left join
             (select
             user_id,
             user_merged_id
             from `{project}.kahuna_user_merge.{namespace}_*`) as two
             on one.user_id = two.user_merged_id)))))
             group by 1, 2, 3, 4)
             
             
             select
             (cast(user_dates.userid as string)) as user_id,
             user_dates.device_type,
             (user_dates.begin_onboarding) as begin_onboarding,
             sum(case when date_diff(date(user_session.session_start_time), user_dates.end_onboarding, day) <= 0 then user_session.session_minutes else 0 end) as onboarding,
             sum(case when (date(user_session.session_start_time) > user_dates.end_onboarding and date(user_session.session_start_time) <= user_dates.end_retention) then user_session.session_minutes else 0 end) as return_period, 
             sum(user_session.session_minutes) as session_minutes,
             count(distinct(case when date_diff(date(user_session.session_start_time), user_dates.end_onboarding, day) <= 0 then date(user_session.session_start_time) else null end)) as unique_days,
             max(case when date_diff(date(user_session.session_start_time), user_dates.end_onboarding, day) <= 0 then user_session.user_session_id else 0 end) as session_count
             from user_dates, user_session
             where user_dates.userid = user_session.userid
             group by 1, 2, 3", project = Sys.getenv('PROJECT'), namespace = namespace, onboarding = onboarding, retention = retention)
}

query.user_event <- function(namespace=namespace, onboarding=0, retention=1, date_begin='2017-07-01',
                             ignore_events = list('start ios', 'start android', 'sign_in', 'app_start', 'af_install', 'application_launched', 
                                                  'start web', 'start_web', 'app launch', 'app_launch', 'register', 'check_alerts')){
  ignore_events <- paste0('"', ignore_events, '"', collapse = ',')
  glue::glue("with user_dates as (select
                      userid,
                      (first_seen) as begin_onboarding,
                      date_add(first_seen, interval {onboarding} day) as end_onboarding,
                      date_add(first_seen, interval {retention} day) as end_retention
                      from
                      (select
                      *,
                      min(dates_seen) over (partition by userid) as first_seen
                      from
                      (select
                      if(two.user_merged_id is not null, two.user_id, one.user_id) as userid,
                      dates_seen
                      from
                      (select
                      user_id,
                      date(time) as dates_seen
                      from `{project}.kahuna_event.{namespace}`
                      where date(time) >= '{date_begin}'
                      group by 1, 2) as one
                      
                      left join
                      
                      (select
                      user_id,
                      user_merged_id
                      from `{project}.kahuna_user_merge.{namespace}_*`) as two
                      on one.user_id = two.user_merged_id))
                      group by 1, 2, 3, 4),
                      
                      
                      user_events as (select
                      *
                      from
                      (select
                      *,
                      (case when session_flag = 1 then null 
                      else round(timestamp_diff(time, lag_event, second) / 60) end) as session_minutes
                      from
                      (select
                      *,
                      sum(session_flag) over (partition by userid order by time) as user_session_id
                      from
                      (select
                      *,
                      (case when (timestamp_diff(time, lag_event, minute) > 10 or lag_event is null) then 1 else 0 end) as session_flag
                      from
                      (select
                      *,
                      lag(time) over (partition by userid order by time) as lag_event
                      from
                      (select
                      if(two.user_merged_id is not null, two.user_id, one.user_id) as userid,
                      event,
                      time
                      from
                      (select
                      user_id,
                      event,
                      time
                      from `{project}.kahuna_event.{namespace}`
                      where not event like 'k_%' and not
                      event in ({ignore_events}) and not
                      event like '%session%' and not
                      event like '%login%' and not 
                      event like '%logout%' and not 
                      event like '%signup%' and 
                      date(time) >= '{date_begin}') as one
                      left join
                      (select
                      user_id,
                      user_merged_id
                      from `{project}.kahuna_user_merge.{namespace}_*`) as two
                      on one.user_id = two.user_merged_id))))))
                      
                      
                      select
                      *,
                      (session_minutes / event_count) as avg_event_min,
                      (event_count / unique_session_count) as avg_events_session
                      from
                      (select
                      (cast(user_dates.userid as string)) as user_id,
                      user_events.event,
                      (case 
                      when date_diff(date(user_events.time), user_dates.end_onboarding, day) <= 0 then 'onboarding' 
                      when (date(user_events.time) > user_dates.end_onboarding and date(user_events.time) <= user_dates.end_retention) then 'return_period' 
                      else 'ignore' end) as return_period, 
                      count(user_events.user_session_id) as event_count,
                      count(distinct(user_events.user_session_id)) as unique_session_count,
                      sum(user_events.session_minutes) as session_minutes
                      from user_dates, user_events
                      where user_dates.userid = user_events.userid
                      group by 1, 2, 3)
                      where return_period != 'ignore'", project = Sys.getenv('PROJECT'), namespace = namespace, onboarding = onboarding, 
                                                        retention = retention, date_begin = date_begin, ignore_events = ignore_events)
}

query.carousell_user_session <- function(namespace='8tracks', onboarding=0, retention=1, date_begin='2017-07-01'){
  glue::glue("with user_dates as (select
             userid,
             device_type,
             (first_seen) as begin_onboarding,
             date_add(first_seen, interval {onboarding} day) as end_onboarding,
             date_add(first_seen, interval {retention} day) as end_retention
             from
             (select
             *,
             min(dates_seen) over (partition by userid, device_type) as first_seen,
             row_number() over (partition by userid order by dates_seen) as row_number
             from
             (select
             if(two.user_merged_id is not null, two.user_id, one.user_id) as userid,
             device_type,
             dates_seen
             from
             (select
             user_id,
             (case when dev_id like '%-%' then 'android' else 'ios' end) as device_type,
             date(time) as dates_seen
             from `{project}.kahuna_event.{namespace}` 
             where date(time) >= '{date_begin}'
             group by 1, 2, 3) as one
             
             left join
             
             (select
             user_id,
             user_merged_id
             from `{project}.kahuna_user_merge.{namespace}_*`) as two
             on one.user_id = two.user_merged_id))
             where row_number = 1
             group by 1, 2, 3, 4, 5),
             
             
             user_session as (select
             *
             from
             (select
             userid,
             min(time) over (partition by userid, user_session_id) as session_start_time,
             #global_session_id,
             user_session_id,
             round(timestamp_diff(max(time) over (partition by userid, user_session_id), min(time) over (partition by userid, user_session_id), second) / 60) as session_minutes
             from
             (select
             userid,
             time,
             #sum(session_flag) over (order by userid, time) as global_session_id,
             sum(session_flag) over (partition by userid order by time) as user_session_id
             from
             (select
             *,
             (case when (timestamp_diff(time, lag_event, minute) > 10 or lag_event is null) then 1 else 0 end) as session_flag
             from
             (select
             *,
             lag(time) over (partition by userid order by time) as lag_event
             from
             (select
             if(two.user_merged_id is not null, two.user_id, one.user_id) as userid,
             time
             from
             (select
             user_id,
             time
             from `{project}.kahuna_event.{namespace}`
             where not event like 'k_%' and
             date(time) >= '{date_begin}') as one
             left join
             (select
             user_id,
             user_merged_id
             from `{project}.kahuna_user_merge.{namespace}_*`) as two
             on one.user_id = two.user_merged_id)))))
             group by 1, 2, 3, 4),
             
             buyers as (select
             user_id,
             properties.value,
             count(distinct(properties.value)) as total
             from `{project}.kahuna_event.{namespace}`, unnest(properties) as properties
             where event = 'buyer_or_seller'
             group by 1, 2
             having count(distinct(properties.value)) = 1 and properties.value = 'buyer')
             
             
             select
             (cast(user_dates.userid as string)) as user_id,
             user_dates.device_type,
             (user_dates.begin_onboarding) as begin_onboarding,
             sum(case when date_diff(date(user_session.session_start_time), user_dates.end_onboarding, day) <= 0 then user_session.session_minutes else 0 end) as onboarding,
             sum(case when (date(user_session.session_start_time) > user_dates.end_onboarding and date(user_session.session_start_time) <= user_dates.end_retention) then user_session.session_minutes else 0 end) as return_period, 
             sum(user_session.session_minutes) as session_minutes,
             count(distinct(case when date_diff(date(user_session.session_start_time), user_dates.end_onboarding, day) <= 0 then date(user_session.session_start_time) else null end)) as unique_days,
             max(case when date_diff(date(user_session.session_start_time), user_dates.end_onboarding, day) <= 0 then user_session.user_session_id else 0 end) as session_count
             from user_dates, user_session, buyers
             where user_dates.userid = user_session.userid and user_session.userid = buyers.user_id
             group by 1, 2, 3", project = Sys.getenv('PROJECT'), namespace = namespace, onboarding = onboarding, retention = retention)
}

query.carousell_user_event <- function(namespace=namespace, onboarding=0, retention=1, date_begin='2017-07-01',
                             ignore_events = list('start ios', 'start android', 'sign_in', 'app_start', 'af_install', 'application_launched', 
                                                  'start web', 'start_web', 'app launch', 'app_launch', 'register', 'check_alerts')){
  ignore_events <- paste0('"', ignore_events, '"', collapse = ',')
  glue::glue("with user_dates as (select
             userid,
             (first_seen) as begin_onboarding,
             date_add(first_seen, interval {onboarding} day) as end_onboarding,
             date_add(first_seen, interval {retention} day) as end_retention
             from
             (select
             *,
             min(dates_seen) over (partition by userid) as first_seen
             from
             (select
             if(two.user_merged_id is not null, two.user_id, one.user_id) as userid,
             dates_seen
             from
             (select
             user_id,
             date(time) as dates_seen
             from `{project}.kahuna_event.{namespace}`
             where date(time) >= '{date_begin}'
             group by 1, 2) as one
             
             left join
             
             (select
             user_id,
             user_merged_id
             from `{project}.kahuna_user_merge.{namespace}_*`) as two
             on one.user_id = two.user_merged_id))
             group by 1, 2, 3, 4),
             
             
             user_events as (select
             *
             from
             (select
             *,
             (case when session_flag = 1 then null 
             else round(timestamp_diff(time, lag_event, second) / 60) end) as session_minutes
             from
             (select
             *,
             sum(session_flag) over (partition by userid order by time) as user_session_id
             from
             (select
             *,
             (case when (timestamp_diff(time, lag_event, minute) > 10 or lag_event is null) then 1 else 0 end) as session_flag
             from
             (select
             *,
             lag(time) over (partition by userid order by time) as lag_event
             from
             (select
             if(two.user_merged_id is not null, two.user_id, one.user_id) as userid,
             event,
             time
             from
             (select
             user_id,
             event,
             time
             from `{project}.kahuna_event.{namespace}`
             where not event like 'k_%' and not
             event in ({ignore_events}) and not
             event like '%session%' and not
             event like '%login%' and not 
             event like '%logout%' and not 
             event like '%signup%' and 
             date(time) >= '{date_begin}') as one
             left join
             (select
             user_id,
             user_merged_id
             from `{project}.kahuna_user_merge.{namespace}_*`) as two
             on one.user_id = two.user_merged_id)))))),
             
             buyers as (select
             user_id,
             properties.value,
             count(distinct(properties.value)) as total
             from `{project}.kahuna_event.{namespace}`, unnest(properties) as properties
             where event = 'buyer_or_seller'
             group by 1, 2
             having count(distinct(properties.value)) = 1 and properties.value = 'buyer')
             
             
             select
             *,
             (session_minutes / event_count) as avg_event_min,
             (event_count / unique_session_count) as avg_events_session
             from
             (select
             (cast(user_dates.userid as string)) as user_id,
             user_events.event,
             (case 
             when date_diff(date(user_events.time), user_dates.end_onboarding, day) <= 0 then 'onboarding' 
             when (date(user_events.time) > user_dates.end_onboarding and date(user_events.time) <= user_dates.end_retention) then 'return_period' 
             else 'ignore' end) as return_period, 
             count(user_events.user_session_id) as event_count,
             count(distinct(user_events.user_session_id)) as unique_session_count,
             sum(user_events.session_minutes) as session_minutes
             from user_dates, user_events, buyers
             where user_dates.userid = user_events.userid and user_events.userid = buyers.user_id
             group by 1, 2, 3)
             where return_period != 'ignore'", project = Sys.getenv('PROJECT'), namespace = namespace, onboarding = onboarding, 
                                                        retention = retention, date_begin = date_begin, ignore_events = ignore_events)
}

