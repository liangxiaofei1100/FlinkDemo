package com.alex.flink.window;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author liangxiaofei
 * @date 2021/3/23 9:45
 */
public class EventTimeWindow {

    public static void main(String[] args) {

        Configuration configuration = new Configuration();
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        DataStream<String> dataSource = environment.readTextFile("data/sensor.txt");
        dataSource.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(10)) {
            @Override
            public long extractTimestamp(String element) {
                return 0;
            }
        });
    }
}
