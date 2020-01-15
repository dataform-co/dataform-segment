module.exports = (params) => {
  return publish("segment_sessionized_events", {
    ...params.defaultConfig
  }).query(ctx => `
WITH user_anon_mapping AS (
  SELECT
    anonymous_id,
    ANY_VALUE(user_id) as user_id
  FROM
    ${ctx.ref(params.segmentSchema, "identifies")}
  WHERE
    anonymous_id IS NOT NULL
  GROUP BY
    1
),
segment_events_combined AS (
  SELECT
    timestamp,
    user_id,
    anonymous_id,
    context_ip,
    context_page_url,
    context_page_path,
    STRUCT(id as track_id, event) AS tracks_info,
    STRUCT(
      NULL AS page_id,
      NULL AS url,
      NULL AS referrer,
      NULL AS url_hash,
      NULL AS title,
      NULL as name,
      NULL as search,
      NULL as path,
      NULL as category,
      NULL as context_campaign_content,
      NULL as context_campaign_medium,
      NULL as context_campaign_source,
      NULL as context_campaign_name,
      NULL as context_campaign_term,
      NULL as context_campaign_keyword
    ) AS pages_info
  FROM
    ${ctx.ref(params.segmentSchema, "tracks")}
  where
    true
    and timestamp > "2019-01-01"
  UNION ALL
  SELECT
    timestamp,
    user_id,
    anonymous_id,
    context_ip,
    context_page_url,
    context_page_path,
    STRUCT(NULL AS track_id, NULL AS event) AS tracks_info,
    STRUCT(
      id,
      url,
      referrer,
      url_hash,
      title,
      name,
      search,
      path,
      category,
      context_campaign_content,
      context_campaign_medium,
      context_campaign_source,
      context_campaign_name,
      context_campaign_term,
      context_campaign_keyword
    ) AS pages_info
  FROM
    ${ctx.ref(params.segmentSchema, "pages")}
  where
    true
    and timestamp > "2019-01-01"
),
segment_events_mapped AS (
  SELECT
    timestamp,
    COALESCE(
      segment_events_combined.user_id,
      user_anon_mapping.user_id,
      segment_events_combined.anonymous_id
    ) as user_id,
    context_ip,
    context_page_url,
    context_page_path,
    tracks_info,
    pages_info
  FROM
    segment_events_combined
    LEFT JOIN user_anon_mapping ON segment_events_combined.anonymous_id = user_anon_mapping.anonymous_id
),
session_starts AS (
  SELECT
    *,
    COALESCE(
      (
        UNIX_MILLIS(timestamp) - UNIX_MILLIS(
          LAG(timestamp) OVER (
            PARTITION BY user_id
            ORDER BY
              timestamp ASC
          )
        )
      ) >= ${params.sessionTimeoutMillis},
      TRUE
    ) AS session_start_event
  FROM
    segment_events_mapped
),
with_session_index AS (
  SELECT
    *,
    SUM(IF (session_start_event, 1, 0)) OVER (
      PARTITION BY user_id
      ORDER BY
        timestamp ASC
    ) AS session_index
  FROM
    session_starts
),
session_id AS (
  SELECT
    *,
    farm_fingerprint(
      CONCAT(
        CAST(session_index AS STRING),
        "|",
        CAST(user_id AS STRING),
        "|",
        CAST(DATE(timestamp) AS STRING)
      )
    ) AS session_id
  FROM
    with_session_index
)
SELECT
  *
FROM
  session_id
`)
}
