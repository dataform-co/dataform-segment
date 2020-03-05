Common data models for segment data such as `sessions` and a user roll up table built from `identifies`.

## Supported warehouses

- BigQuery

*If you would like us to add support for another warehouse, please get in touch via [email](mailto:team@dataform.co) or [Slack](https://slack.dataform.co/)*

## Installation

Add the package to your `package.json` file in your Dataform project. You can find the most up to package version on the [releases page](https://github.com/dataform-co/dataform-segment/releases).

## Configure the package

Create a new JS file in your `definitions/` folder and create the segment tables with the following example:

```js
const segment = require("dataform-segment");

segment({
  // The name of your segment schema.
  segmentSchema: "javascript",
  // The timeout for splitting sessions in milliseconds.
  sessionTimeoutMillis: 30 * 60 * 1000,
  // Default configuration applied to all produced datasets.
  defaultConfig: {
    schema: "dataform_segment",
    tags: ["segment"],
    type: "view"
  },
  // list of custom fields to extract from the pages table
  customPageFields: ["url_hash", "category"],
  // list of custom fields to extract from the identifies table
  customUserFields: ["email", "name", "company_name", "created_at"],
  // list of custom fields to extract from the tracks table
  customerTrackFields: ["browser_type"]
  
});
```

For more advanced uses cases, see the [example.js](https://github.com/dataform-co/dataform-segment/blob/master/definitions/example.js).

## Data models

This primary outputs of this package are the following data models (configurable as tables or views).

### `segment_sessions`

Contains a combined view of tracks and pages from segment. Each session is a period of sustained activity, with a new session starting after a 30min+ period of inactivity. Each session contains a repeated field of records which are either tracks or pages. Common fields are extracted out into the top level and type specific fields are kept within two structs: `records.track` and `records.page`.

### `segment_users`

Aggregates all identifies calls to give a table with one row per user_id. Identify calls with only an anonymous_id are mapped to the matching user_id where possible.

![](https://dataform.sirv.com/dataform-segment-dag-4.png?profile=WebP)
