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
  cast(null as string) AS track_id, 
  cast(null as string) as ${[...common.TRACK_FIELDS, ...params.customTrackFields].join(",\n cast(null as string) as ")}
) AS tracks_info,
STRUCT(
    id as page_id,
    ${[...common.PAGE_FIELDS, ...params.customPageFields].join(",\n")}
) AS pages_info
FROM
  ${ctx.ref(params.segmentSchema, "pages")}
`)
}
