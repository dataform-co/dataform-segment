const segmentCommon = require("./common");

module.exports = (params) => {

  return publish("segment_page_events", {
    ...params.defaultConfig
  }).query(ctx => `

-- format page calls into a format suitable to join with all other events
select distinct
  pages.timestamp,
  user_id,
  anonymous_id,
  id as page_id,
  ${Object.entries(segmentCommon.allPageFields(params)).map(
      ([key, value]) => `${key} as ${value}`).join(",\n    ")}
from
  ${ctx.ref(params.segmentSchema, "pages")} as pages

`)
}
