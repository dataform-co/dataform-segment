module.exports = (params) => {
  return publish("segment_user_map", {
    ...params.defaultConfig
  }).query(ctx => `

-- for each anonymous_id, find a user_id mapped to it (if one exists)
with anonymous_id_user_id_pairs as (
select distinct
  anonymous_id,
  user_id,
  timestamp
from
  (
  select
    anonymous_id,
    user_id,
    timestamp
  from
    ${ctx.ref(params.segmentSchema, "tracks")}
  union all
  select
    anonymous_id,
    user_id,
    timestamp
  from
    ${ctx.ref(params.segmentSchema, "pages")}
  union all
  select
    anonymous_id,
    user_id,
    timestamp
  from
    ${ctx.ref(params.segmentSchema, "identifies")}
  ) as combined
)

select distinct
  anonymous_id,
  last_value(user_id) over (partition by anonymous_id order by timestamp asc rows between unbounded preceding and unbounded following) as user_id
from
  anonymous_id_user_id_pairs
where
  anonymous_id is not null
  and user_id is not null

`)
}
