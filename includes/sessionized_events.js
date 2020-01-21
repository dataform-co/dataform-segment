module.exports = (params) => {
  return publish("segment_sessionized_events", {
    ...params.defaultConfig
  }).query(ctx => `

with user_anon_mapping as (
-- for each anonymous_id, find a user_id mapped to it (if one exists)
select
  anonymous_id,
  any_value(user_id) as user_id
from
  ${ctx.ref(params.segmentSchema, "identifies")}
where
  anonymous_id is not null
group by
  anonymous_id
),

segment_events_combined as (
-- combine page and track tables into a full events table
select * from ${params.segmentSchema, ctx.ref("segment_track_events")}
union all
select * from ${params.segmentSchema, ctx.ref("segment_page_events")}
),

segment_events_mapped as (
-- map anonymous_id to user_id (where possible)
select
  timestamp,
  coalesce(
    segment_events_combined.user_id,
    user_anon_mapping.user_id,
    segment_events_combined.anonymous_id
  ) as user_id,
  context_ip,
  context_page_url,
  context_page_path,
  tracks_info,
  pages_info
from
  segment_events_combined
  left join user_anon_mapping on segment_events_combined.anonymous_id = user_anon_mapping.anonymous_id
),

session_starts as (
-- label the event that starts the session
select
  *,
  coalesce(
    (
      unix_millis(timestamp) - unix_millis(
        lag(timestamp) over (
          partition by user_id
          order by
            timestamp asc
        )
      )
    ) >= ${params.sessionTimeoutMillis},
    true
  ) as session_start_event
from
  segment_events_mapped
),

with_session_index as (
-- add a session_index (users first session = 1, users second session = 2 etc)
select
  *,
  sum(if (session_start_event, 1, 0)) over (
    partition by user_id
    order by
      timestamp asc
  ) as session_index
from
  session_starts
)

-- add a unique session_id to each session
select
  *,
  farm_fingerprint(
    concat(
      cast(session_index as string),
      "|",
      cast(user_id as string)
    )
  ) as session_id
from
  with_session_index

`)
}
