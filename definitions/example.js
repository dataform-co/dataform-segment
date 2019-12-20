const segment = require("../");

const {
  sessions,
  sessionizedEvents,
  users
} = segment({
  segmentSchema: "javascript",
  sessionTimeoutMillis: 30 * 60 * 1000,
  defaultConfig: {
    schema: "segment_package_example",
    tags: ["segment"],
    type: "view"
  }
});

// Override the sessions table type to "table".
sessions.type("table");
