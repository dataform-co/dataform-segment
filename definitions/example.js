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

// Override the sessions table type to "table".
sessions.type("table");
