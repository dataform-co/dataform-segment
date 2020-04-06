module.exports = (params) => {
  return publish("segment_sessionized_events", {
    ...params.defaultConfig
  }).query(ctx => `

with segment_events_combined as (
-- combine page and track tables into a full events table
select
  "timestamp",
  user_id,
  anonymous_id,
  track_id,
  null as page_id
from 
  ${params.segmentSchema, ctx.ref("segment_track_events")}
union all
select
  "timestamp",
  user_id,
  anonymous_id,
  null as track_id,
  page_id
from 
  ${params.segmentSchema, ctx.ref("segment_page_events")}
),

segment_events_mapped as (
-- map anonymous_id to user_id (where possible)
select
  "timestamp",
  coalesce(
    segment_events_combined.user_id,
    segment_user_anonymous_map.user_id,
    segment_events_combined.anonymous_id
  ) as user_id,
  track_id,
  page_id
from
  segment_events_combined
  left join ${ctx.ref(params.defaultConfig.schema, "segment_user_map")} as segment_user_anonymous_map
    on segment_events_combined.anonymous_id = segment_user_anonymous_map.anonymous_id
),

session_starts as (
-- label the event that starts the session
select
  *,
  coalesce(
    (
      ${crossdb.timestampDiff(`millisecond`, `"timestamp"`,
      crossdb.windowFunction("lag", "timestamp", false, "user_id", "timestamp asc", " ") // supplying empty frame clause as frame clause is not valid for a lag
      )}
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
  ${crossdb.windowFunction("sum", "case when session_start_event then 1 else 0 end", false, "user_id", '"timestamp" asc', "rows between unbounded preceding and current row")} as session_index
from
  session_starts
)

-- add a unique session_id to each session
select
  *,
  ${crossdb.generateSurrogateKey(["session_index", "user_id"])} as session_id
from
  with_session_index

`)
}
