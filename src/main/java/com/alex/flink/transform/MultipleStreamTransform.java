package com.alex.flink.transform;

import com.alex.flink.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author liangxiaofei
 * @date 2021/3/12 9:31
 */
public class MultipleStreamTransform {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataSource = environment.readTextFile("data/sensor.txt");
        DataStream<SensorReading> dataStream = dataSource
                .map((MapFunction<String, SensorReading>) s -> {
                    String[] strings = s.split(",");
                    return new SensorReading(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
                });

        // 分流操作，按照温度值20度为界，分为两条流。
        // 新版本的Flink去掉了split操作
//        dataStream.process(new ProcessFunction<SensorReading, Object>() {
//            @Override
//            public void processElement(SensorReading value, Context ctx, Collector<Object> out) throws Exception {
//
//            }
//        });

        // 合流操作
        DataStream<Tuple2<String, Double>> warningStream = dataStream.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading sensorReading) throws Exception {
                return new Tuple2<>(sensorReading.getId(), sensorReading.getTemperature());
            }
        });

        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedStreams = warningStream.connect(dataStream);
        connectedStreams.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> value) throws Exception {
                return null;
            }

            @Override
            public Object map2(SensorReading value) throws Exception {
                return null;
            }
        });

        // 联合多条流
        DataStream<String> unionStream = dataSource.union(dataSource);

        environment.execute();
    }
}
