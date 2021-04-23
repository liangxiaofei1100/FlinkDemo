package com.alex.flink.source;

import com.alex.flink.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 * @author liangxiaofei
 * @date 2021/3/11 10:45
 */
public class CustomerSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStream<SensorReading> dataSource = environment.addSource(new MySensorSource());
        dataSource.print();


        environment.execute();
    }

    public static class MySensorSource implements SourceFunction<SensorReading> {

        // 定义一个标志位，用于控制数据的产生
        private boolean running = true;

        public void run(SourceContext<SensorReading> ctx) throws Exception {
            // 定义一个随机数发生器
            Random random = new Random();
            int sensorNumber = 10;
            // 设置sensorNumber个初始温度
            HashMap<String, Double> sensorTemperatureMap = new HashMap<String, Double>();
            for (int i = 1; i <= sensorNumber; i++) {
                sensorTemperatureMap.put("sensor_" + i, random.nextGaussian() * 10 + 60);
            }

            while (running) {
                for (String sensorId : sensorTemperatureMap.keySet()) {
                    Double t = sensorTemperatureMap.get(sensorId);
                    t += random.nextGaussian();
                    sensorTemperatureMap.put(sensorId, t);

                    SensorReading sensorReading = new SensorReading();
                    sensorReading.setId(sensorId);
                    sensorReading.setTimestamp(System.currentTimeMillis());
                    sensorReading.setTemperature(t);
                    ctx.collect(sensorReading);
                }
            }
        }

        public void cancel() {
            running = false;
        }
    }
}
