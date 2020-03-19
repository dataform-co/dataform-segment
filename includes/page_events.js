const segmentCommon = require("./common");

module.exports = (params) => {

  const CUSTOM_PAGE_FIELDS_OBJ = {};
  params.customPageFields.forEach(item => CUSTOM_PAGE_FIELDS_OBJ[item] = item);

  const CUSTOM_TRACK_FIELDS_OBJ = {};
  params.customTrackFields.forEach(item => CUSTOM_PAGE_FIELDS_OBJ[item] = item);

  return publish("segment_page_events", {
    ...params.defaultConfig
  }).query(ctx => `

-- format page calls into a format suitable to join with track calls
select
  timestamp,
  user_id,
  anonymous_id,
  context_ip as ip,
  context_page_url as url,
  context_page_path as path,
  struct(
    cast(null as string) as track_id, 
    ${Object.entries({...segmentCommon.TRACK_FIELDS, ...segmentCommon.CUSTOM_TRACK_FIELDS_OBJ}).map(
            ([key, value]) => `cast(null as string) as ${value}`).join(",\n    ")}
  ) as tracks_info,
  struct(
    id as page_id,
    ${Object.entries({...segmentCommon.PAGE_FIELDS, ...segmentCommon.CUSTOM_PAGE_FIELDS_OBJ}).map(
        ([key, value]) => `${key} as ${value}`).join(",\n    ")}
  ) as pages_info
from
  ${ctx.ref(params.segmentSchema, "pages")}

`)
}
