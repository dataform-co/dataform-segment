const segmentCommon = require("./common");
const crossdb = require("./crossdb");

module.exports = (params) => {

  return publish("segment_sessions", {
    assertions: {
      uniqueKey: ["session_id"]
    },
    description: "Sessions contain a combined view of tracks and pages from segment. Each session is a period of sustained activity, with a new session starting after a 30min+ period of inactivity. Each session contains a repeated field of records which are either tracks or pages. Common fields are extracted out into the top level and type specific fields are kept within two structs: records.track and records.page",
    columns: {
      session_id: "Unique identifier of the session",
      session_index: "A session counter for each user. session_index=1 is the first session for that users",
      session_start_timestamp: "Timestamp of the first event in the session",
      session_end_timestamp: "Timestamp of the last event in the session"
    },
    ...params.defaultConfig
  }).query(ctx => `

with 

${params.includePages ? 
`first_and_last_page_values as (
select distinct
  session_id,
  ${Object.entries(segmentCommon.allPageFields(params)).map(
      ([key, value]) => `${crossdb.windowFunction({
        func: "first_value",
        value: value,
        ignore_nulls: false,
        partition_fields: "session_id",
        order_fields: 'sessionized_pages.timestamp asc',
        frame_clause: "rows between unbounded preceding and unbounded following",
      })} as first_${value}`).join(",\n  ")},
  ${Object.entries(segmentCommon.allPageFields(params)).map(
      ([key, value]) => `${crossdb.windowFunction({
        func: "last_value",
        value: value,
        ignore_nulls: false,
        partition_fields: "session_id",
        order_fields: 'sessionized_pages.timestamp asc',
        frame_clause: "rows between unbounded preceding and unbounded following",
      })} as last_${value}`).join(",\n  ")}
  from
    ${ctx.ref(params.defaultConfig.schema, "segment_sessionized_pages")} as sessionized_pages
  ),` : ``}

${params.includeScreens ? 
`first_and_last_screen_values as (
select distinct
  session_id,
  ${Object.entries(segmentCommon.allScreenFields(params)).map(
      ([key, value]) => `${crossdb.windowFunction({
        func: "first_value",
        value: value,
        ignore_nulls: false,
        partition_fields: "session_id",
        order_fields: 'sessionized_screens.timestamp asc',
        frame_clause: "rows between unbounded preceding and unbounded following",
      })} as first_${value}`).join(",\n  ")},
  ${Object.entries(segmentCommon.allScreenFields(params)).map(
      ([key, value]) => `${crossdb.windowFunction({
        func: "last_value",
        value: value,
        ignore_nulls: false,
        partition_fields: "session_id",
        order_fields: 'sessionized_screens.timestamp asc',
        frame_clause: "rows between unbounded preceding and unbounded following",
      })} as last_${value}`).join(",\n  ")}
  from
    ${ctx.ref(params.defaultConfig.schema, "segment_sessionized_screens")} as sessionized_screens
  ),` : ``}

output as (

select
  segment_sessionized_events.session_id,
  segment_sessionized_events.session_index,
  segment_sessionized_events.user_id,
  min(segment_sessionized_events.timestamp) as session_start_timestamp,
  max(segment_sessionized_events.timestamp) as session_end_timestamp,
  
  -- stats about the session
  ${ctx.when(global.dataform.projectConfig.warehouse == "bigquery", `struct(\n  `)}
  ${segmentCommon.enabledEvents(params).map((event) => 
    `count(segment_sessionized_events.${event}_id) as total_${event}s`).join(`,\n  `)},
  ${crossdb.timestampDiff("millisecond", "min(segment_sessionized_events.timestamp)", "max(segment_sessionized_events.timestamp)")} as duration_millis
  ${ctx.when(global.dataform.projectConfig.warehouse == "bigquery", `) as stats`)}

  -- first values in the session for page fields
  ${params.includePages ? 
  `, ${ctx.when(global.dataform.projectConfig.warehouse == "bigquery", `struct(\n  `)}
  ${Object.entries(segmentCommon.allPageFields(params)).map(
      ([key, value]) => `first_and_last_page_values.first_${value}`).join(",\n  ")}
  ${ctx.when(global.dataform.projectConfig.warehouse == "bigquery", `) as first_page_values`)},
  -- last values in the session for page fields
  ${ctx.when(global.dataform.projectConfig.warehouse == "bigquery", `struct(\n  `)}
  ${Object.entries(segmentCommon.allPageFields(params)).map(
      ([key, value]) => `first_and_last_page_values.last_${value}`).join(",\n  ")}
  ${ctx.when(global.dataform.projectConfig.warehouse == "bigquery", `) as last_page_values`)}` : `` }

  -- first values in the session for screen fields
  ${params.includeScreens ?
  `, ${ctx.when(global.dataform.projectConfig.warehouse == "bigquery", `struct(\n  `)}
  ${Object.entries(segmentCommon.allScreenFields(params)).map(
      ([key, value]) => `first_and_last_screen_values.first_${value}`).join(",\n  ")}
  ${ctx.when(global.dataform.projectConfig.warehouse == "bigquery", `) as first_screen_values`)},
  -- last values in the session for screen fields
  ${ctx.when(global.dataform.projectConfig.warehouse == "bigquery", `struct(\n  `)}
  ${Object.entries(segmentCommon.allScreenFields(params)).map(
      ([key, value]) => `first_and_last_screen_values.last_${value}`).join(",\n  ")}
  ${ctx.when(global.dataform.projectConfig.warehouse == "bigquery", `) as last_screen_values`)}` : `` }


  ${ctx.when(global.dataform.projectConfig.warehouse == "bigquery", `-- repeated array of records
  ,array_agg(
    struct(
      segment_sessionized_events.timestamp
      ${params.includeTracks ?
        `,struct(
        segment_sessionized_tracks.timestamp,
        segment_sessionized_tracks.track_id,
        ${Object.entries(segmentCommon.allTrackFields(params)).map(
      ([key, value]) => `segment_sessionized_tracks.${value}`).join(",\n        ")}
      ) as track` : ``}
      ${params.includePages ?
      `, struct(
        segment_sessionized_pages.timestamp,
        segment_sessionized_pages.page_id,
        ${Object.entries(segmentCommon.allPageFields(params)).map(
      ([key, value]) => `segment_sessionized_pages.${value}`).join(",\n        ")}
      ) as page` : ``}
      ${params.includeScreens ?
      `, struct(
        segment_sessionized_pages.timestamp,
        segment_sessionized_pages.screen_id,
        ${Object.entries(segmentCommon.allScreenFields(params)).map(
      ([key, value]) => `segment_sessionized_screens.${value}`).join(",\n        ")}
      ) as screen` : ``}
    ) order by segment_sessionized_events.timestamp asc
  ) as records`
  )}
from
  ${ctx.ref(params.defaultConfig.schema, "segment_sessionized_events")} as segment_sessionized_events
  ${params.includePages ? 
  `left join first_and_last_page_values
    using(session_id)` : ``}
  ${params.includeScreens ? 
  `left join first_and_last_screen_values
    using(session_id)` : ``}
  ${ctx.when(global.dataform.projectConfig.warehouse == "bigquery",
  segmentCommon.enabledEvents(params).map((event) => 
    `left join ${ctx.ref(params.defaultConfig.schema, `segment_sessionized_${event}s`)} as segment_sessionized_${event}s
    using(${event}_id)`).join(`\n  `))}
group by
  session_id, session_index, user_id
  ${params.includePages ? 
  `${Object.entries(segmentCommon.allPageFields(params)).map(
      ([key, value]) => `, first_and_last_page_values.first_${value}`).join(" ")}
  ${Object.entries(segmentCommon.allPageFields(params)).map(
      ([key, value]) => `, first_and_last_page_values.last_${value}`).join(" ")}` : ``}

  ${params.includeScreens ? 
  `${Object.entries(segmentCommon.allScreenFields(params)).map(
      ([key, value]) => `, first_and_last_screen_values.first_${value}`).join(" ")}
  ${Object.entries(segmentCommon.allScreenFields(params)).map(
      ([key, value]) => `, first_and_last_screen_values.last_${value}`).join(" ")}` : ``}
  )

select * from output`)
}