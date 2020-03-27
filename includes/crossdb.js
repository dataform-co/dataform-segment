function typeString(warehouse) {
  return ({
    bigquery: "string",
    redshift: "varchar",
    postgres: "varchar",
    snowflake: "varchar",
    sqldatawarehouse: "string",
  })[warehouse || session.config.warehouse];
}

function timestampDiff(date_part, start_timestamp, end_timestamp, warehouse) {
  return ({
    bigquery: `timestamp_diff(${end_timestamp}, ${start_timestamp}, ${date_part})`,
    redshift: `datediff(${date_part}, ${start_timestamp}, ${end_timestamp})`,
    postgres: `datediff(${date_part}, ${start_timestamp}, ${end_timestamp})`,
    snowflake: `datediff(${date_part}, ${start_timestamp}, ${end_timestamp})`,
    sqldatawarehouse: `datediff(${date_part}, ${start_timestamp}, ${end_timestamp})`
  })[warehouse || session.config.warehouse];
}

function generateSurrogateKey(id_array, warehouse) {
  return ({
    bigquery: `cast(farm_fingerprint(concat(${id_array.map((id) => (`cast(${id} as ${typeString()})`)).join(`,`)})) as ${typeString()})`,
    redshift: `md5(concat(${id_array.map((id) => (`cast(${id} as ${typeString()})`)).join(`,`)}))`,
    postgres: `md5(concat(${id_array.map((id) => (`cast(${id} as ${typeString()})`)).join(`,`)}))`,
    snowflake: `md5(concat(${id_array.map((id) => (`cast(${id} as ${typeString()})`)).join(`,`)}))`,
    sqldatawarehouse: `hashbytes("md5", (concat(${id_array.map((id) => (`cast(${id} as ${typeString()})`)).join(`,`)})))`,
  })[warehouse || session.config.warehouse];
}

module.exports = {
  timestampDiff,
  generateSurrogateKey
}