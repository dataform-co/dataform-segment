const segmentCommon = require("./common");

module.exports = (params) => {

  return publish("segment_sessionized_screens", {
    ...params.defaultConfig
  }).query(ctx => `

-- annotate screen records with session details
select
  segment_sessionized_events.timestamp,
  segment_sessionized_events.user_id,
  segment_sessionized_events.screen_id,
  segment_sessionized_events.session_index,
  segment_sessionized_events.session_id,
  ${Object.entries(segmentCommon.allScreenFields(params)).map(
      ([key, value]) => `segment_screen_events.${value}`).join(",\n  ")}
from 
  ${params.segmentSchema, ctx.ref("segment_sessionized_events")} as segment_sessionized_events
  left join ${params.segmentSchema, ctx.ref("segment_screen_events")} as segment_screen_events
    on segment_sessionized_events.screen_id = segment_screen_events.screen_id
where
  segment_sessionized_events.screen_id is not null

`)
}
