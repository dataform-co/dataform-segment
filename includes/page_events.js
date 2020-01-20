module.exports = (params) => {
  return publish("segment_page_events", {
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
  NULL AS track_id, 
  NULL AS ${[...common.TRACK_FIELDS, ...params.customTrackFields].join(",\n NULL AS ")}
) AS tracks_info,
STRUCT(
    id as page_id,
    ${[...common.PAGE_FIELDS, ...params.customPageFields].join(",\n")}
) AS pages_info
FROM
  ${ctx.ref(params.segmentSchema, "pages")}
`)
}
