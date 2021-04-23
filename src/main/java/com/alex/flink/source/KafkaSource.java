package com.alex.flink.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author liangxiaofei
 * @date 2021/3/11 10:29
 */
public class KafkaSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.0.140:9092");

        // ./kafka-console-producer.sh --broker-list localhost:9092 --topic sensor
        DataStream<String> dataSource = environment.addSource(new FlinkKafkaConsumer<>("sensor", new SimpleStringSchema(), properties));
        dataSource.print();


        environment.execute();
    }

}
