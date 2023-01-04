const sql = require("./sql")
const segmentCommon = require("./common");

module.exports = (params) => {

  return publish("segment_sessionized_events", {
    ...params.defaultConfig
  }).query(ctx => `

with segment_events_combined as (
-- combine all enabled events tables into one combined events tables
${segmentCommon.enabledEvents(params).map((event) => 
`select
  ${event}_events.timestamp,
  user_id,
  anonymous_id,
  ${segmentCommon.enabledEvents(params).map((event_id) => 
  `${event==event_id ? `` : `null as `}${event_id}_id`).join(`,\n  `)}
from 
  ${params.segmentSchema, ctx.ref(`segment_${event}_events`)} as ${event}_events`).join(`\nunion all \n`)}
),

segment_events_mapped as (
-- map anonymous_id to user_id (where possible)
select
  segment_events_combined.timestamp,
  coalesce(
    segment_events_combined.user_id,
    segment_user_anonymous_map.user_id,
    segment_events_combined.anonymous_id
  ) as user_id,
  ${segmentCommon.enabledEvents(params).map((event) => `${event}_id`).join(`,\n  `)}
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
      ${sql.timestampDiff(`millisecond`,
      sql.windowFunction(
      "lag",
        "timestamp",
        false,
        {
          partitionFields: ["user_id"],
          orderFields: ["timestamp asc"]
        }
      ),
      `
    segment_events_mapped.timestamp `
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
  ${sql.windowFunction(
      "sum",
        "case when session_start_event then 1 else 0 end",
        false,
        {
        partitionFields: ["user_id"],
        orderFields: ['session_starts.timestamp asc'],
        frameClause: "rows between unbounded preceding and current row"
        }
    )} as session_index
from
  session_starts
)

-- add a unique session_id to each session
select
  *,
  ${sql.surrogateKey(["session_index", "user_id"])} as session_id
from
  with_session_index

`)
}
