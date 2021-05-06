CREATE TABLE base_community (
    id INT,
    exch_station_id INT,
    cmt_name STRING
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://192.168.18.60:3306/epoo_cloud_base',
  'driver' = 'com.mysql.cj.jdbc.Driver',
  'username' = 'root',
  'password' = '123456',
  'table-name' = 'base_community'
)