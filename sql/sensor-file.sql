CREATE TABLE sensor_data (
  id STRING,
  `timestamp` BIGINT,
  temperature DOUBLE
) WITH (
 'connector' = 'filesystem',
 'path' = 'file:///D:\\workspace_java\\FlinkDemo\\data\\sensor.txt',
 'format' = 'csv',
 'csv.ignore-parse-errors' = 'true',
 'csv.allow-comments' = 'true'
)