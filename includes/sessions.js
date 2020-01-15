module.exports = (params) => {
  return publish("segment_sessions", {
    bigquery: {
      partitionBy: "DATE(session_start_timestamp)"
    },
    description: "Sessions contain a combined view of tracks and pages from segment. Each session is a period of sustained activity, with a new session starting after a 30min+ period of inactivity. Each session contains a repeated field of records which are either tracks or pages. Common fields are extracted out into the top level and type specific fields are kept within two structs: records.track and records.page",
    columns: {
      session_id: "Unique identifies of the session",
      session_index: "A session counter for each user. session_index=1 is the first session for that users",
      session_start_timestamp: "Timestamp of the first event in the session",
      session_end_timestamp: "Timestamp of the last event in the session",
      context_ip: "The IP address the session came from"
    },
    ...params.defaultConfig
  }).query(ctx => `
select
  session_id,
  session_index,
  min(timestamp) as session_start_timestamp,
  max(timestamp) as session_end_timestamp,
  any_value(context_ip) as context_ip,
  any_value(user_id) as user_id,
  struct(
    count(tracks_info.track_id) as total_tracks,
    count(pages_info.page_id) as total_pages,
    timestamp_diff(max(timestamp), min(timestamp), millisecond) as duration_millis
  ) as stats,
  array_agg(
    struct(
      timestamp,
      context_page_url,
      context_page_path,
      tracks_info as track,
      pages_info as page
    )
  ) as records
from
  ${ctx.ref(params.defaultConfig.schema, "segment_sessionized_events")}
group by
  session_id,
  session_index
`)
}