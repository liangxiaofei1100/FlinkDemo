package com.alex.flink.transform;

import com.alex.flink.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liangxiaofei
 * @date 2021/3/12 10:34
 */
public class RichFunctionTransform {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataSource = environment.readTextFile("data/sensor.txt");
        DataStream<SensorReading> dataStream = dataSource
                .map((MapFunction<String, SensorReading>) s -> {
                    String[] strings = s.split(",");
                    return new SensorReading(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
                });
        DataStream<Tuple2<String, Double>> dataStream2 = dataStream.map(new MyRichFunction());
        dataStream2.print();
        environment.execute();
    }

    public static class MyRichFunction extends RichMapFunction<SensorReading, Tuple2<String, Double>> {

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化，一般是定义状态，建立数据库连接
            System.out.println("open");
        }

        @Override
        public void close() throws Exception {
            // 一般是关闭连接，清空状态
            System.out.println("close");
        }

        @Override
        public Tuple2<String, Double> map(SensorReading sensorReading) throws Exception {
            return new Tuple2<>(sensorReading.getId(), sensorReading.getTemperature());
        }
    }
}
