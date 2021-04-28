CREATE TABLE data_room (
    id INT,
    collector_id INT,
    collector_number STRING,
    enterprise_id INT,
    data_time TIMESTAMP,
    temperature FLOAT,
    humidity FLOAT
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://192.168.18.60:3306/test',
  'driver' = 'com.mysql.cj.jdbc.Driver',
  'username' = 'root',
  'password' = '123456',
  'table-name' = 'data_room'
)