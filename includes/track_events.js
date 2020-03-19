const segmentCommon = require("./common");

module.exports = (params) => {

  const customPageFieldsObj = params.customPageFields.reduce((acc, item) => ({...acc, [item]: item }), {});

  const customTrackFieldsObj = params.customTrackFields.reduce((acc, item) => ({...acc, [item]: item }), {});
  
  return publish("segment_track_events", {
    ...params.defaultConfig
  }).query(ctx => `

-- format track calls into a format suitable to join with page calls
select
  timestamp,
  user_id,
  anonymous_id,
  context_ip as ip,
  context_page_url as url,
  context_page_path as path,
  struct(
    id as track_id,
    ${Object.entries({...segmentCommon.TRACK_FIELDS, ...segmentCommon.customTrackFieldsObj}).map(
        ([key, value]) => `${key} as ${value}`).join(",\n    ")}
      ) as tracks_info,
  struct(
    cast(null as string) as page_id,
    ${Object.entries({...segmentCommon.PAGE_FIELDS, ...segmentCommon.customPageFieldsObj}).map(
        ([key, value]) => `cast(null as string) as ${value}`).join(",\n    ")}
  ) as pages_info
from
  ${ctx.ref(params.segmentSchema, "tracks")}
`)
}
