module.exports = (params) => {
  return publish("users", {
    ...params.defaultConfig
  }).query(ctx => `
select
  user_id,
  ARRAY_AGG(
    email ignore nulls
    order by
      timestamp desc
  ) [safe_offset(0)] as email,
  min(timestamp) AS first_identified
from
  ${ctx.ref(params.segmentSchema, "identifies")}
group by
  1
`)
}
