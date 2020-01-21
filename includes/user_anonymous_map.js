module.exports = (params) => {
  return publish("segment_user_anonymous_map", {
    ...params.defaultConfig
  }).query(ctx => `

-- for each anonymous_id, find a user_id mapped to it (if one exists)
select
  anonymous_id,
  any_value(user_id) as user_id
from
  ${ctx.ref(params.segmentSchema, "identifies")}
where
  anonymous_id is not null
group by
  anonymous_id

`)
}
