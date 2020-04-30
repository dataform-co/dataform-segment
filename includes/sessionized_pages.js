const segmentCommon = require("./common");

module.exports = (params) => {

  const customPageFieldsObj = params.customPageFields.reduce((acc, item) => ({...acc, [item]: item }), {});

  const customTrackFieldsObj = params.customTrackFields.reduce((acc, item) => ({...acc, [item]: item }), {});
  
  return publish("segment_sessionized_pages", {
    ...params.defaultConfig
  }).query(ctx => `

-- annotate track records with session details
select
  segment_sessionized_events.timestamp,
  segment_sessionized_events.user_id,
  segment_sessionized_events.page_id,
  segment_sessionized_events.session_index,
  segment_sessionized_events.session_id,
  ${Object.entries({...segmentCommon.PAGE_FIELDS, ...customPageFieldsObj}).map(
      ([key, value]) => `segment_page_events.${value}`).join(",\n  ")}
from 
  ${params.segmentSchema, ctx.ref("segment_sessionized_events")} as segment_sessionized_events
  left join ${params.segmentSchema, ctx.ref("segment_page_events")} as segment_page_events
    on segment_sessionized_events.page_id = segment_page_events.page_id
where
  segment_sessionized_events.page_id is not null

`)
}
