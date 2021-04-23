package com.alex.flink.transform;

import com.alex.flink.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liangxiaofei
 * @date 2021/3/11 16:11
 */
public class RollingAggregationTransform {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataSource = environment.readTextFile("data/sensor.txt");
        DataStream<SensorReading> dataStream = dataSource
                .map((MapFunction<String, SensorReading>) s -> {
                    String[] strings = s.split(",");
                    return new SensorReading(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
                })
                .keyBy((KeySelector<SensorReading, Object>) SensorReading::getId)
                .maxBy("temperature");

        dataStream.print();

        environment.execute();
    }

}
