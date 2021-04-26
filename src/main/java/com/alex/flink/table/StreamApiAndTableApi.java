package com.alex.flink.table;

import com.alex.flink.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author liangxiaofei
 * @date 2021/4/25 15:16
 */
public class StreamApiAndTableApi {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        //从文件读取
        DataStream<String> inputStream = environment.readTextFile("data/sensor.txt");
        // 转换为对象
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
        });

        // 创建表环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        // 基于数据流创建一张表
        Table table = tableEnvironment.fromDataStream(dataStream);
        // 调用table api进行转换操作
        Table resultTable = table.select("id, temperature").where("id = 'sensor1'");
        // 执行sql
        tableEnvironment.createTemporaryView("sensor", table);
        String sql = "select id, temperature from sensor where id = 'sensor1'";
        Table resultSqlTable = tableEnvironment.sqlQuery(sql);

        // 打印
        tableEnvironment.toAppendStream(resultTable, Row.class).print("result");
        tableEnvironment.toAppendStream(resultSqlTable, Row.class).print("result");

        environment.execute();
    }
}
