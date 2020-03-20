const segment = require("../");

const segmentModels = segment({
  segmentSchema: "javascript",
  sessionTimeoutMillis: 30 * 60 * 1000,
  defaultConfig: {
    schema: "segment_package_example",
    tags: ["segment"],
    type: "view"
  },
  customPagesTable: { schema: "javascript", name: "pages" },
  customPageFields: ["url_hash", "category"],
  customUserFields: ["email", "name", "company_name", "created_at"],
});

// Override the sessions and user table type to "table".
segmentModels.sessions.type("table").config({
  bigQuery: {
    partitionBy: "date(session_start_timestamp)"
  }
});

segmentModels.users.type("table").config({
  bigQuery: {
    partitionBy: "date(first_seen_at)"
  }
});
