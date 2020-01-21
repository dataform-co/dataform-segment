module.exports = (params) => {
  return publish("segment_users", {
    ...params.defaultConfig
  }).query(ctx => `
select
  user_id,
  min(timestamp) AS first_seen_at,
  ${params.customUserFields.map(f=>`array_agg(${f} ignore nulls order by timestamp desc)[safe_offset(0)] as ${f}`).join(",\n  ")}
from
  ${ctx.ref(params.segmentSchema, "identifies")}
group by
  1
`)
}
