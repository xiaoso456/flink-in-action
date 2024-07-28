CREATE TABLE asset (
    id STRING,
    name STRING,
    is_subject STRING,
    ip STRING,
    PRIMARY KEY (id) NOT ENFORCED
)WITH (
      'connector' = 'mysql-cdc',
      'hostname' = '192.168.229.128',
      'port' = '32710',
      'database-name' ='my_database',
      'table-name' = 'asset',
      'username' = 'root',
      'password' = 'jpKSovB6mB',
      'server-time-zone' = '+08:00'
);
