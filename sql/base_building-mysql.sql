CREATE TABLE base_building (
    id INT,
    community_id INT,
    building STRING
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://192.168.18.60:3306/epoo_cloud_base',
  'driver' = 'com.mysql.cj.jdbc.Driver',
  'username' = 'root',
  'password' = '123456',
  'table-name' = 'base_building'
)