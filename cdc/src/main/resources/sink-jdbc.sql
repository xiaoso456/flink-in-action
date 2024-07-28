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
    asset_ip STRING,
    PRIMARY KEY (event_id,asset_id) NOT ENFORCED
)WITH (
     'connector' = 'jdbc',
     'url' = 'jdbc:mysql://192.168.229.128:32710/my_database',
     'table-name' = 'event_asset_readonly',
     'username' = 'root',
     'password' = 'jpKSovB6mB',
     'driver' = 'com.mysql.jdbc.Driver'
);
