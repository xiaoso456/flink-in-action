
create table print_sink (
    event_id STRING,
    event_type_id STRING,
    event_type STRING,
    event_name STRING,
    event_log_time TIMESTAMP,
    event_description STRING,
    asset_id STRING,
    asset_name STRING,
    asset_is_subject STRING,
    asset_ip STRING
) WITH (
      'connector' = 'print'
);