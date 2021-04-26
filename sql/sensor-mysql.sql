CREATE TABLE sensor_data (
  id STRING,
  `time` TIMESTAMP,
  temp FLOAT
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://192.168.18.60:3306/test',
  'driver' = 'com.mysql.cj.jdbc.Driver',
  'username' = 'root',
  'password' = '123456',
  'table-name' = 'sensor_temp'
)