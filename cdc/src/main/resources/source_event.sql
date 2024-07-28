CREATE TABLE event (
    id STRING,
    type_id STRING,
    name STRING,
    type STRING,
    description STRING,
    log_time TIMESTAMP,
    PRIMARY KEY (id) NOT ENFORCED
)WITH (
      'connector' = 'mysql-cdc',
      'hostname' = '192.168.229.128',
      'port' = '32710',
      'database-name' ='my_database',
      'table-name' = 'event',
      'username' = 'root',
      'password' = 'jpKSovB6mB',
      'server-time-zone' = '+08:00'
);
