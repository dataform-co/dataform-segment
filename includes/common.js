// List of standard fields shared across the rest of the project

// From here: https://segment.com/docs/connections/spec/page/
let PAGE_FIELDS = [
    "url",
    "referrer",
    "title",
    "name",
    "search",
    "path",
    "context_campaign_content",
    "context_campaign_medium",
    "context_campaign_source",
    "context_campaign_name",
    "context_campaign_term",
    "context_campaign_keyword"];

// From here: https://segment.com/docs/connections/spec/track/
let TRACK_FIELDS = [
  "event"
];

module.exports = {
  PAGE_FIELDS,
  TRACK_FIELDS
}