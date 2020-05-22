const users = require("./includes/users");
const sessionizedEvents = require("./includes/sessionized_events");
const sessions = require("./includes/sessions");
const pageEvents = require("./includes/page_events");
const sessionizedPages = require("./includes/sessionized_pages");
const trackEvents = require("./includes/track_events");
const sessionizedTracks = require("./includes/sessionized_tracks");
const userMap = require("./includes/user_map");

module.exports = (params) => {

  params = {
    segmentSchema: "javascript", // schema that Segment writes tables into
    sessionTimeoutMillis: 30 * 60 * 1000, // Session timeout in milliseconds
    customPageFields: [], // list of custom fields to extract from the pages table
    customUserFields: [], // list of custom fields to extract from the identifies table
    customTrackFields: [], // list of custom fields to extract from the tracks table
    declareSources: true,
    ...params
  };

  const {
    defaultConfig,
    segmentSchema,
    declareSources
  } = params;

  let identifies, pages, tracks;
  
  if (declareSources) {
    identifies = declare({
      ...defaultConfig,
      schema: segmentSchema,
      name: "identifies"
    });

    pages = declare({
      ...defaultConfig,
      schema: segmentSchema,
      name: "pages"
    });

    tracks = declare({
      ...defaultConfig,
      schema: segmentSchema,
      name: "tracks"
    });
  }

  // Publish and return datasets.
  return {
    identifies,
    pages,
    tracks,
    users: users(params),
    sessionizedEvents: sessionizedEvents(params),
    sessions: sessions(params),
    pageEvents: pageEvents(params),
    sessionizedPages: sessionizedPages(params),
    trackEvents: trackEvents(params),
    sessionizedTracks: sessionizedTracks(params),
    userMap: userMap(params),
  }
}
