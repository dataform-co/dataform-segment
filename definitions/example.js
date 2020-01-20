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
  customTrackFields: [] // TODO: why isn't it possible to leave this out?
});

// Override the sessions table type to "table".
sessions.type("table");
