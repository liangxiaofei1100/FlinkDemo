package com.alex.flink.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author liangxiaofei
 * @date 2021/3/11 11:09
 */
public class BaseTransform {
    private static Logger logger = LoggerFactory.getLogger(BaseTransform.class);

    public static void main(String[] args) throws Exception {
        logger.info("Show log");
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
//        environment.setParallelism(1);

        DataStream<String> dataSource = environment.readTextFile("data/sensor.txt");
        DataStream<Integer> dataStream = dataSource.map(new MapFunction<String, Integer>() {
            public Integer map(String s) throws Exception {
                return s.length();
            }
        });
        dataStream.print("map");

        DataStream<String> dataStream2 = dataSource.flatMap(new FlatMapFunction<String, String>() {
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] words = s.split(",");
                for (String w : words) {
                    collector.collect(w);
                }
            }
        });
        dataStream2.print("flatMap");

        DataStream<String> dataStream4 = dataSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s.startsWith("sensor1");
            }
        });
        dataStream4.print("filter");

        environment.execute();
    }
}
