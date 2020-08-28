// List of standard fields shared across the rest of the project

// From here: https://segment.com/docs/connections/spec/page/
let PAGE_FIELDS = {
  url: "url",
  referrer: "referrer",
  title: "title",
  name: "name",
  search: "search",
  path: "path",
  context_campaign_content: "utm_content",
  context_campaign_medium: "utm_medium",
  context_campaign_source: "utm_source",
  context_campaign_name: "utm_campaign",
  context_campaign_term: "utm_term"
};

// From here: https://segment.com/docs/connections/spec/track/
let TRACK_FIELDS = {
  event: "event"
};

// From here: https://segment.com/docs/connections/spec/screen/
let SCREEN_FIELDS = {
  name: "name"
};

function allPageFields(params) {
  const customPageFieldsObj = params.customPageFields.reduce((acc, item) => ({
    ...acc,
    [item]: item
  }), {});
  return {
    ...PAGE_FIELDS,
    ...customPageFieldsObj
  };
}

function allTrackFields(params) {
  const customTrackFieldsObj = params.customTrackFields.reduce((acc, item) => ({
    ...acc,
    [item]: item
  }), {});
  return {
    ...TRACK_FIELDS,
    ...customTrackFieldsObj
  };
}

function allScreenFields(params) {
  const customScreenFieldsObj = params.customScreenFields.reduce((acc, item) => ({
    ...acc,
    [item]: item
  }), {});
  return {
    ...SCREEN_FIELDS,
    ...customScreenFieldsObj
  };
}

function enabledEvents(params) {
  const events = [];

  if (params.includeTracks) {
    events.push("track");
  }
  if (params.includePages) {
    events.push("page");
  }
  if (params.includeScreens) {
    events.push("screen");
  }

  return events;
}

module.exports = {
  allTrackFields,
  allPageFields,
  allScreenFields,
  enabledEvents
}
