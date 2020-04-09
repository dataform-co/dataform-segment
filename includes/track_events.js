const segmentCommon = require("./common");

module.exports = (params) => {

  const customPageFieldsObj = params.customPageFields.reduce((acc, item) => ({...acc, [item]: item }), {});

  const customTrackFieldsObj = params.customTrackFields.reduce((acc, item) => ({...acc, [item]: item }), {});
  
  return publish("segment_track_events", {
    ...params.defaultConfig
  }).query(ctx => `

-- format track calls into a format suitable to join with page calls
select
  tracks.timestamp,
  user_id,
  anonymous_id,
  id as track_id,
  ${Object.entries({...segmentCommon.TRACK_FIELDS, ...segmentCommon.customTrackFieldsObj}).map(
      ([key, value]) => `${key} as ${value}`).join(",\n    ")}
from
  ${ctx.ref(params.segmentSchema, "tracks")} as tracks
`)
}
