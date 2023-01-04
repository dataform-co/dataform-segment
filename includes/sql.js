const getDialect = () => {
  const dataformWarehouse = global.dataform.projectConfig.warehouse;
  if (!dataformWarehouse) {
    return "standard";
  }
  return {
    bigquery: "standard",
    redshift: "redshift",
    postgres: "postgres",
    snowflake: "snowflake",
    sqldatawarehouse: "mssql",
  }[dataformWarehouse];
};

const timestampDiff = (datePart, start, end) => {
  const dialect = getDialect();
  if (dialect === "snowflake" || dialect === "redshift") {
    return `datediff(${datePart}, ${start}, ${end})`;
  }
  return `timestamp_diff(${end}, ${start}, ${datePart})`;
};

const windowFunction = (
  name,
  value,
  ignoreNulls = false,
  windowSpecification
) => {
  const dialect = getDialect();
  const partitionFieldsAsString = windowSpecification.partitionFields
    ? [...windowSpecification.partitionFields].join(`, `)
    : "";
  const orderFieldsAsString = windowSpecification.orderFields
    ? [...windowSpecification.orderFields].join(`, `)
    : "";

  if (
    dialect === "standard" ||
    dialect === "mssql" ||
    dialect === "snowflake"
  ) {
    return `${name}(${value} ${ignoreNulls ? `ignore nulls` : ``}) over (${
      windowSpecification.partitionFields
        ? `partition by ${partitionFieldsAsString}`
        : ``
    } ${
      windowSpecification.orderFields ? `order by ${orderFieldsAsString}` : ``
    } ${
      windowSpecification.frameClause ? windowSpecification.frameClause : ``
    })`;
  }

  // For some window functions in Redshift, a frame clause is always required
  const requiresFrame = [
    "avg",
    "count",
    "first_value",
    "last_value",
    "max",
    "min",
    "nth_value",
    "stddev_samp",
    "stddev_pop",
    "stddev",
    "sum",
    "variance",
    "var_samp",
    "var_pop",
  ].includes(name.toLowerCase());

  if (dialect === "redshift") {
    return `${name}(${value} ${ignoreNulls ? `ignore nulls` : ``}) over (${
      windowSpecification.partitionFields
        ? `partition by ${partitionFieldsAsString}`
        : ``
    } ${
      windowSpecification.orderFields ? `order by ${orderFieldsAsString}` : ``
    } ${
      windowSpecification.orderFields
        ? windowSpecification.frameClause
          ? windowSpecification.frameClause
          : requiresFrame
          ? `rows between unbounded preceding and unbounded following`
          : ``
        : ``
    })`;
  }

  if (dialect === "postgres") {
    return `${name}(${value}) over (${
      windowSpecification.partitionFields
        ? `partition by ${partitionFieldsAsString}`
        : ``
    } ${windowSpecification.orderFields || ignoreNulls ? `order by` : ``} ${
      ignoreNulls ? `case when ${value} is not null then 0 else 1 end asc` : ``
    } ${orderFieldsAsString && ignoreNulls ? `,` : ``} ${orderFieldsAsString} ${
      windowSpecification.orderFields
        ? windowSpecification.frameClause
          ? windowSpecification.frameClause
          : requiresFrame
          ? `rows between unbounded preceding and unbounded following`
          : ``
        : ``
    })`;
  }
};

const surrogateKey = (columnNames) => {
  const dialect = getDialect();
  const columnsAsStrings = columnNames.map((id) => this.asString(id)).join(`,`);
  if (dialect === "standard") {
    return this.asString(`farm_fingerprint(concat(${columnsAsStrings}))`);
  }
  return this.asString(`md5(concat(${columnsAsStrings}))`);
};

module.exports = {
  timestampDiff,
  windowFunction,
  surrogateKey,
};
