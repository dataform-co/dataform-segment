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
    context_campaign_term: "utm_term"};

// From here: https://segment.com/docs/connections/spec/track/
let TRACK_FIELDS = {
  event: "event"
};

module.exports = {
  PAGE_FIELDS,
  TRACK_FIELDS
}