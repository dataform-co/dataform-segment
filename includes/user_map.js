module.exports = (params) => {
  return publish("segment_user_map", {
    ...params.defaultConfig
  }).query(ctx => `

-- for each anonymous_id, find a user_id mapped to it (if one exists)
with anonymous_id_user_id_pairs as (
select
  anonymous_id,
  user_id
from
  (
  select
    anonymous_id,
    user_id
  from
    ${ctx.ref(params.segmentSchema, "tracks")}
  union all
  select
    anonymous_id,
    user_id
  from
    ${ctx.ref(params.segmentSchema, "pages")}
  union all
  select
    anonymous_id,
    user_id
  from
    ${ctx.ref(params.segmentSchema, "identifies")}
  )
group by
  1,2
)

select
  anonymous_id,
  any_value(user_id) as user_id
from
  anonymous_id_user_id_pairs
where
  anonymous_id is not null
group by
  anonymous_id

`)
}
