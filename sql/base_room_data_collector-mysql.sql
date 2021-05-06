CREATE TABLE base_room_data_collector (
    id INT,
    enterprise_id INT,
    number STRING,
    building_id INT
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://192.168.18.60:3306/epoo_cloud_base',
  'driver' = 'com.mysql.cj.jdbc.Driver',
  'username' = 'root',
  'password' = '123456',
  'table-name' = 'base_room_data_collector'
)