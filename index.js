const users = require("./includes/users");
const sessionizedEvents = require("./includes/sessionized_events");
const sessions = require("./includes/sessions");
const pageEvents = require("./includes/page_events");
const sessionizedPages = require("./includes/sessionized_pages");
const trackEvents = require("./includes/track_events");
const sessionizedTracks = require("./includes/sessionized_tracks");
const screenEvents = require("./includes/screen_events");
const sessionizedScreens = require("./includes/sessionized_screens");
const userMap = require("./includes/user_map");

module.exports = (params) => {

    params = {
      segmentSchema: "javascript", // schema that Segment writes tables into
      sessionTimeoutMillis: 30 * 60 * 1000, // Session timeout in milliseconds
      customPageFields: [], // list of custom fields to extract from the pages table
      customUserFields: [], // list of custom fields to extract from the identifies table
      customTrackFields: [], // list of custom fields to extract from the tracks table
      customScreenFields: [], // list of custom fields to extract from the tracks table
      declareSources: true,
      includeTracks: true,
      includePages: true,
      includeScreens: false,
      ...params
    };

    const {
      defaultConfig,
      segmentSchema,
      declareSources,
      includeTracks,
      includePages,
      includeScreens
    } = params;

    let identifies, pages, tracks, screens;

    if (declareSources) {
      identifies = declare({
        ...defaultConfig,
        schema: segmentSchema,
        name: "identifies"
      });

      if (includePages) {
        pages = declare({
          ...defaultConfig,
          schema: segmentSchema,
          name: "pages"
        });
      }

      if (includeTracks) {
        tracks = declare({
          ...defaultConfig,
          schema: segmentSchema,
          name: "tracks"
        });
      }

      if (includeScreens) {
        screens = declare({
          ...defaultConfig,
          schema: segmentSchema,
          name: "screens"
        });
      }

    }

    // Publish and return datasets.
    let result = {
      identifies,
      users: users(params),
      sessionizedEvents: sessionizedEvents(params),
      sessions: sessions(params),
      userMap: userMap(params),
    };
    
    if (includePages) {
      result = {
        ...result,
        pages,
        pageEvents: pageEvents(params),
        sessionizedPages: sessionizedPages(params)
      };
    }

    if (includeTracks) {
      result = {
        ...result,
        tracks,
        trackEvents: trackEvents(params),
        sessionizedTracks: sessionizedTracks(params)
      };
    }

    if (includeScreens) {
      result = {
        ...result,
        screens,
        screenEvents: screenEvents(params),
        sessionizedScreens: sessionizedScreens(params)
      };
    }

    return result;
}