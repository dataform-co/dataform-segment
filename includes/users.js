const sql = require("@dataform/sql")();

let USER = `coalesce(
  identifies.user_id,
  segment_user_anonymous_map.user_id,
  segment_user_anonymous_map.anonymous_id
)`;

module.exports = (params) => {
  return publish("segment_users", {
    assertions: {
      uniqueKey: ["user_id"]
    },
    description: "Users aggregates all identifies calls to give a table with one row per user_id. Identify calls without only an anonymous_id are mapped to the user where possible.",
    columns: {
      user_id: "Unique identifier of the user",
      first_seen_at: "First time this user was seen"
    },
    ...params.defaultConfig
  }).query(ctx => `

select distinct
  ${USER} as user_id,
  ${sql.windowFunction(
        "first_value",
        "identifies.timestamp",
        true,
        {
          partitionFields: [USER],
          orderFields: ["identifies.timestamp asc"],
          frameClause: "rows between unbounded preceding and unbounded following"
        }
      )} as first_seen_at
  ${params.customUserFields.length ? `,` : ``}
  ${params.customUserFields.map(f => `${sql.windowFunction(
        "first_value",
        f,
        true,
        {
          partitionFields: [USER],
          orderFields: ["identifies.timestamp desc"],
          frameClause: "rows between unbounded preceding and unbounded following",
        }
      )} as ${f}`).join(",\n  ")}
from
  ${ctx.ref(params.defaultConfig.schema, "segment_user_map")} as segment_user_anonymous_map
  left join ${ctx.ref(params.segmentSchema, "identifies")} as identifies
    on identifies.anonymous_id = segment_user_anonymous_map.anonymous_id

`)
}
