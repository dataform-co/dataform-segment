module.exports = (params) => {
  return publish("segment_track_events", {
    ...params.defaultConfig
  }).query(ctx => `
SELECT
  timestamp,
  user_id,
  anonymous_id,
  context_ip,
  context_page_url,
  context_page_path,
  STRUCT(
    id as track_id,
    ${[...common.TRACK_FIELDS, ...params.customTrackFields].join(",\n")}
  ) AS tracks_info,
  STRUCT(
    cast(null as string) AS page_id,
    cast(null as string) as ${[...common.PAGE_FIELDS, ...params.customPageFields].join(",\n cast(null as string) as ")}
  ) AS pages_info
FROM
  ${ctx.ref(params.segmentSchema, "tracks")}
`)
}
