package com.alex.flink.transform;

import com.alex.flink.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liangxiaofei
 * @date 2021/3/11 17:07
 */
public class ReduceTransform {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataSource = environment.readTextFile("data/sensor.txt");
        DataStream<SensorReading> dataStream = dataSource
                .map((MapFunction<String, SensorReading>) s -> {
                    String[] strings = s.split(",");
                    return new SensorReading(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
                });

        KeyedStream<SensorReading, Object> keyedStream = dataStream.keyBy((KeySelector<SensorReading, Object>) SensorReading::getId);
        SingleOutputStreamOperator<SensorReading> resultStream = keyedStream.reduce((ReduceFunction<SensorReading>) (value1, value2) -> {
                    if (value1.getTemperature() > value2.getTimestamp()) {
                        return new SensorReading(value1.getId(), value1.getTimestamp(), value1.getTemperature());
                    } else {
                        return new SensorReading(value2.getId(), value2.getTimestamp(), value2.getTemperature());
                    }
                }
        );

        resultStream.print();

        environment.execute();
    }

}
