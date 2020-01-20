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
    NULL AS page_id,
    NULL AS ${[...common.PAGE_FIELDS, ...params.customPageFields].join(",\n NULL AS ")}
  ) AS pages_info
FROM
  ${ctx.ref(params.segmentSchema, "tracks")}
`)
}
