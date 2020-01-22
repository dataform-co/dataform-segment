const segmentCommon = require("../common");

module.exports = (params) => {
  return publish("segment_page_events", {
    ...params.defaultConfig
  }).query(ctx => `

-- format page calls into a format suitable to join with track calls
select
  timestamp,
  user_id,
  anonymous_id,
  context_ip,
  context_page_url,
  context_page_path,
  struct(
    cast(null as string) as track_id, 
    cast(null as string) as ${[...segmentCommon.TRACK_FIELDS, ...params.customTrackFields].join(",\n cast(null as string) as ")}
  ) as tracks_info,
  struct(
      id as page_id,
      ${[...segmentCommon.PAGE_FIELDS, ...params.customPageFields].join(",\n")}
  ) as pages_info
from
  ${ctx.ref(params.segmentSchema, "pages")}

`)
}
