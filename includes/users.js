module.exports = (params) => {
  return publish("segment_users", {
    description: "Users aggregates all identifies calls to give a table with one row per user_id. Identify calls without only an anonymous_id are mapped to the user where possible.",
    columns: {
      user_id: "Unique identifier of the user",
      first_seen_at: "First time this user was seen"
    },
    ...params.defaultConfig
  }).query(ctx => `

select
    coalesce(
    identifies.user_id,
    segment_user_anonymous_map.user_id,
    identifies.anonymous_id
  ) as user_id,
  min(timestamp) AS first_seen_at,
  ${params.customUserFields.map(f=>`array_agg(${f} ignore nulls order by timestamp desc)[safe_offset(0)] as ${f}`).join(",\n  ")}
from
  ${ctx.ref(params.segmentSchema, "identifies")} as identifies
  left join ${ctx.ref(params.defaultConfig.schema, "segment_user_map")} as segment_user_anonymous_map
    on identifies.anonymous_id = segment_user_anonymous_map.anonymous_id
group by
  user_id

`)
}
