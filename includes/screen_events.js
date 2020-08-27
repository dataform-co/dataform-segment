const segmentCommon = require("./common");

module.exports = (params) => {

  return publish("segment_screen_events", {
    ...params.defaultConfig
  }).query(ctx => `

-- format screen calls into a format suitable to join with other events
select distinct
  screens.timestamp,
  user_id,
  anonymous_id,
  id as screen_id,
  ${Object.entries(segmentCommon.allScreenFields(params)).map(
      ([key, value]) => `${key} as ${value}`).join(",\n    ")}
from
  ${ctx.ref(params.segmentSchema, "screens")} as screens

`)
}
