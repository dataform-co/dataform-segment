const segmentCommon = require("./common");

module.exports = (params) => {

  return publish("segment_sessionized_tracks", {
    ...params.defaultConfig
  }).query(ctx => `

-- annotate track records with session details
select
  segment_sessionized_events.timestamp,
  segment_sessionized_events.user_id,
  segment_sessionized_events.track_id,
  segment_sessionized_events.session_index,
  segment_sessionized_events.session_id,
  ${Object.entries(segmentCommon.allTrackFields(params)).map(
      ([key, value]) => `segment_track_events.${value}`).join(",\n  ")}
from 
  ${params.segmentSchema, ctx.ref("segment_sessionized_events")} as segment_sessionized_events
  left join ${params.segmentSchema, ctx.ref("segment_track_events")} as segment_track_events
    on segment_sessionized_events.track_id = segment_track_events.track_id
where
  segment_sessionized_events.track_id is not null

`)
}
