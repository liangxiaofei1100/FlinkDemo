package com.alex.flink.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * @author liangxiaofei
 * @date 2021/3/12 14:10
 */
public class KafkaSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataSource = environment.readTextFile("data/sensor.txt");
        dataSource.addSink(new FlinkKafkaProducer<>("192.168.0.140:9092", "sensorTopic", new SimpleStringSchema()));

        environment.execute();
    }
}
