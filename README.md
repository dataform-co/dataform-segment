Common data models for segment data such as sessions, and user roll up tables built from identifies.

## Supported warehouses

- BigQuery

## Installation

Add the following to your `package.json` file in your Dataform project:
```json
{
    "dataform-segment": "git+https://github.com/dataform-co/dataform-segment.git#4e1e51526177bcc5e655d63c8d8fec971492f206"
}
```

Previous releases can be installed from the [releases page](releases).

## Configure the package

Create a new JS file in your `definitions/` folder and create the segment tables with the following example:

```js
const segment = require("dataform-segment");

segment({
  // The name of your segment scheme.
  segmentSchema: "javascript",
  // The timeout for splitting sessions in milliseconds.
  sessionTimeoutMillis: 30 * 60 * 1000,
  // Default configuration applied to all produced datasets.
  defaultConfig: {
    schema: "dataform_segment",
    tags: ["segment"],
    type: "table"
  }
});
```

For more advanced uses cases, see the [example.js](blob/master/definitions/example.js).

## Tables

This package creates the following tables.

### `segment_sessions`

Sessions contain a combined view of tracks and pages from segment. Each session is a period of sustained activity, with a new session starting after a 30min+ period of inactivity. Each session contains a repeated field of records which are either tracks or pages. Common fields are extracted out into the top level and type specific fields are kept within two structs: `records.track` and `records.page`.

### `segment_users`

TODO: Dan


