const segment = require("../");

const {
  sessions,
  sessionizedEvents,
  users,
  pages,
  tracks
} = segment({
  segmentSchema: "javascript",
  sessionTimeoutMillis: 30 * 60 * 1000,
  defaultConfig: {
    schema: "segment_package_example",
    tags: ["segment"],
    type: "view"
  },
  customPageFields: ["url_hash", "category"],
  customTrackFields: [],
  customUserFields: ["email", "name", "company_name", "created_at"],
});

// Override the sessions and user table type to "table".
sessions.type("table").config({
  bigQuery: {
    partitionBy: "date(session_start_timestamp)"
  }
});

users.type("table").config({
  bigQuery: {
    partitionBy: "date(first_seen_at)"
  }
});
