const segmentCommon = require("./common");

module.exports = (params) => {
      
  return publish("segment_track_events", {
    ...params.defaultConfig
  }).query(ctx => `

-- format track calls into a format suitable to join with all other events
select distinct
  tracks.timestamp,
  user_id,
  anonymous_id,
  id as track_id,
  ${Object.entries(segmentCommon.allTrackFields(params)).map(
      ([key, value]) => `${key} as ${value}`).join(",\n    ")}
from
  ${ctx.ref(params.segmentSchema, "tracks")} as tracks
`)
}
