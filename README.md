# Dataform: Segment

Common data models for segment data such as sessions, and user roll up tables built from identifies.

## Install the package

TODO

## Configure the package

Create a new JS file in your `definitions/` folder and create the segment tables with the following example:

```js
const segment = require("@dataform/segment");

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