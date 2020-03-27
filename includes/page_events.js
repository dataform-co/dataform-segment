const segmentCommon = require("./common");

module.exports = (params) => {

  

  const customPageFieldsObj = params.customPageFields.reduce((acc, item) => ({...acc, [item]: item }), {});

  const customTrackFieldsObj = params.customTrackFields.reduce((acc, item) => ({...acc, [item]: item }), {});

  return publish("segment_page_events", {
    ...params.defaultConfig
  }).query(ctx => `

-- format page calls into a format suitable to join with track calls
select
  "timestamp",
  user_id,
  anonymous_id,
  id as page_id,
  ${Object.entries({...segmentCommon.PAGE_FIELDS, ...segmentCommon.customPageFieldsObj}).map(
      ([key, value]) => `${key} as ${value}`).join(",\n    ")}
from
  ${ctx.ref(params.segmentSchema, "pages")}

`)
}
