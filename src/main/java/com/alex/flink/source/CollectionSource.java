package com.alex.flink.source;

import com.alex.flink.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author liangxiaofei
 * @date 2021/3/11 10:02
 */
public class CollectionSource {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从集合中读取数据
        DataStream<SensorReading> dataStream = environment.fromCollection(Arrays.asList(
                new SensorReading("sensor1", 0L, 10d),
                new SensorReading("sensor2", 0L, 20d),
                new SensorReading("sensor3", 0L, 30d)));
        dataStream.print("data");

        // 从元素读取数据
        DataStream<Integer> integerDataStream = environment.fromElements(1, 2, 3, 4, 5);
        integerDataStream.print("int");

        environment.execute();
    }
}
