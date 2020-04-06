const segmentCommon = require("./common");

module.exports = (params) => {
  return publish("segment_sessions", {
    description: "Sessions contain a combined view of tracks and pages from segment. Each session is a period of sustained activity, with a new session starting after a 30min+ period of inactivity. Each session contains a repeated field of records which are either tracks or pages. Common fields are extracted out into the top level and type specific fields are kept within two structs: records.track and records.page",
    columns: {
      session_id: "Unique identifier of the session",
      session_index: "A session counter for each user. session_index=1 is the first session for that users",
      session_start_timestamp: "Timestamp of the first event in the session",
      session_end_timestamp: "Timestamp of the last event in the session"
    },
    ...params.defaultConfig
  }).query(ctx => `

with first_and_last_page_values as (
select distinct
  session_id,
  ${Object.entries({...segmentCommon.PAGE_FIELDS, ...segmentCommon.customPageFieldsObj}).map(
      ([key, value]) => `${crossdb.windowFunction("first_value", value, true, "session_id", '"timestamp" asc')} as first_${value}`).join(",\n  ")},
  ${Object.entries({...segmentCommon.PAGE_FIELDS, ...segmentCommon.customPageFieldsObj}).map(
      ([key, value]) => `${crossdb.windowFunction("last_value", value, true, "session_id", '"timestamp" asc')} as last_${value}`).join(",\n  ")}
  from
    ${ctx.ref(params.defaultConfig.schema, "segment_sessionized_pages")}
  )

select
  session_id,
  session_index,
  user_id,
  min(segment_sessionized_events.timestamp) as session_start_timestamp,
  max(segment_sessionized_events.timestamp) as session_end_timestamp,
  
  -- stats about the session
  ${ctx.when(global.session.config.warehouse == "bigquery", `struct(\n  `)}
  count(segment_sessionized_events.track_id) as total_tracks,
  count(segment_sessionized_events.page_id) as total_pages,
  ${crossdb.timestampDiff("millisecond", "min(segment_sessionized_events.timestamp)", "max(segment_sessionized_events.timestamp)")}
  ${ctx.when(global.session.config.warehouse == "bigquery", `) as stats`)},

  -- first values in the session for page fields
  ${ctx.when(global.session.config.warehouse == "bigquery", `struct(\n  `)}
  ${Object.entries({...segmentCommon.PAGE_FIELDS, ...segmentCommon.customPageFieldsObj}).map(
      ([key, value]) => `first_and_last_page_values.first_${value}`).join(",\n  ")}
  ${ctx.when(global.session.config.warehouse == "bigquery", `) as first_page_values`)},

  -- last values in the session for page fields
  ${ctx.when(global.session.config.warehouse == "bigquery", `struct(\n  `)}
  ${Object.entries({...segmentCommon.PAGE_FIELDS, ...segmentCommon.customPageFieldsObj}).map(
      ([key, value]) => `first_and_last_page_values.last_${value}`).join(",\n  ")}
  ${ctx.when(global.session.config.warehouse == "bigquery", `) as last_page_values`)}
  
  ${ctx.when(global.session.config.warehouse == "bigquery", `-- repeated array of records
  ,array_agg(
    struct(
      segment_sessionized_events."timestamp",
      struct(
        segment_sessionized_tracks."timestamp",
        segment_sessionized_tracks.track_id,
        ${Object.entries({...segmentCommon.TRACK_FIELDS, ...segmentCommon.customTrackFieldsObj}).map(
      ([key, value]) => `segment_sessionized_tracks.${value}`).join(",\n  ")}
      ) as track,
      struct(
        segment_sessionized_pages."timestamp",
        segment_sessionized_pages.page_id,
        ${Object.entries({...segmentCommon.PAGE_FIELDS, ...segmentCommon.customPageFieldsObj}).map(
      ([key, value]) => `segment_sessionized_pages.${value}`).join(",\n  ")}
      ) as page
    ) order by segment_sessionized_events."timestamp" asc
  ) as records`)}
from
  ${ctx.ref(params.defaultConfig.schema, "segment_sessionized_events")}
  left join first_and_last_page_values
    using(session_id)
  ${ctx.when(global.session.config.warehouse == "bigquery", `
  left join ${ctx.ref(params.defaultConfig.schema, "segment_sessionized_pages")} as segment_sessionized_pages
    using(page_id)
  left join ${ctx.ref(params.defaultConfig.schema, "segment_sessionized_tracks")} as segment_sessionized_tracks
    using(track_id)`)}
group by
  session_id, session_index, user_id ${Object.entries({...segmentCommon.PAGE_FIELDS, ...segmentCommon.customPageFieldsObj}).map(
      ([key, value]) => `, first_${value}`).join(" ")}
  ${Object.entries({...segmentCommon.PAGE_FIELDS, ...segmentCommon.customPageFieldsObj}).map(
      ([key, value]) => `, last_${value}`).join(" ")}
`)
}