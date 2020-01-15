const settings = {
  database: "tada-analytics",
  segmentSchema: "javascript",
  sessionTimeoutMillis: 30 * 60 * 1000,
  outputSchema: "segment_package",
  tags: ["segment"]
}

const users = require("./includes/users");
const sessionizedEvents = require("./includes/sessionized_events");
const sessions = require("./includes/sessions");

module.exports = (params) => {

  const {
    defaultConfig,
    segmentSchema
  } = params;
  // Declare the source segment tables.
  const identifies = declare({
    ...defaultConfig,
    schema: segmentSchema,
    name: "identifies"
  });

  const pages = declare({
    ...defaultConfig,
    schema: segmentSchema,
    name: "pages"
  });

  const tracks = declare({
    ...defaultConfig,
    schema: segmentSchema,
    name: "tracks"
  });

  // Publish and return datasets.
  return {
    identifies,
    pages,
    tracks,
    users: users(params),
    sessionizedEvents: sessionizedEvents(params),
    sessions: sessions(params),
  }
}
