CREATE TABLE event_asset_link (
    event_id STRING,
    asset_id STRING,
    PRIMARY KEY (event_id,asset_id) NOT ENFORCED
)WITH (
      'connector' = 'mysql-cdc',
      'hostname' = '192.168.229.128',
      'port' = '32710',
      'database-name' ='my_database',
      'table-name' = 'event_asset_link',
      'username' = 'root',
      'password' = 'jpKSovB6mB',
      'server-time-zone' = '+08:00'
);
