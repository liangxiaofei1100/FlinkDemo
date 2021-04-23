package com.alex.flink.sink;

import com.alex.flink.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * @author liangxiaofei
 * @date 2021/3/12 15:10
 */
public class JdbcSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataSource = environment.readTextFile("data/sensor.txt");
        DataStream<SensorReading> dataStream = dataSource
                .map((MapFunction<String, SensorReading>) s -> {
                    String[] strings = s.split(",");
                    return new SensorReading(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
                });

        String url = "jdbc:mysql://192.168.18.60:3306/test?useUnicode=true&characterEncoding=UTF8&serverTimezone=Asia/Shanghai";
        JdbcConnectionOptions jdbcConnectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUrl(url).withUsername("root").withPassword("123456").build();
        String sql = "insert into sensor_temp (id, time, temp) values(?,?,?)";

        // JDBC是批量写入，默认的JdbcExecutionOptions中设置是5000条写入一次。如果没有达到批次量，程序结束时也会写入。
        dataStream.addSink(JdbcSink.sink(sql, new JdbcStatementBuilder<SensorReading>() {
            @Override
            public void accept(PreparedStatement preparedStatement, SensorReading sensorReading) throws SQLException {
                System.out.println("jdbc sink");
                preparedStatement.setString(1, sensorReading.getId());
                preparedStatement.setTimestamp(2, new Timestamp(sensorReading.getTimestamp()));
                preparedStatement.setDouble(3, sensorReading.getTemperature());
            }
        }, jdbcConnectionOptions));

        dataStream.print("sensor");
        environment.execute();
    }
}
