module.exports = (params) => {
  return publish("segment_track_events", {
    ...params.defaultConfig
  }).query(ctx => `

-- format track calls into a format suitable to join with page calls
select
  timestamp,
  user_id,
  anonymous_id,
  context_ip,
  context_page_url,
  context_page_path,
  struct(
    id as track_id,
    ${[...segment_common.TRACK_FIELDS, ...params.customTrackFields].join(",\n")}
  ) as tracks_info,
  struct(
    cast(null as string) as page_id,
    cast(null as string) as ${[...segment_common.PAGE_FIELDS, ...params.customPageFields].join(",\n cast(null as string) as ")}
  ) as pages_info
from
  ${ctx.ref(params.segmentSchema, "tracks")}

`)
}
