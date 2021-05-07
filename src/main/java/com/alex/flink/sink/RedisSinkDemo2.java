package com.alex.flink.sink;

import com.alex.flink.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * @author liangxiaofei
 * @date 2021/5/7 14:50
 */
public class RedisSinkDemo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStream<String> dataSource = environment.readTextFile("data/sensor.txt");
        DataStream<SensorReading> dataStream = dataSource
                .map((MapFunction<String, SensorReading>) s -> {
                    String[] strings = s.split(",");
                    return new SensorReading(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
                });
        dataStream.addSink(new SensorReadingRedisSink());
        dataStream.print();
        environment.execute();
    }

    public static class SensorReadingRedisSink extends RichSinkFunction<SensorReading> {
        private JedisPool jedisPool;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            jedisPool = new JedisPool("192.168.18.61");
        }

        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.connect();
                String id = value.getId();
                int seconds = 1000;
                jedis.setex("sensor-t:" + id, seconds, String.valueOf(value.getTemperature()));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void close() throws Exception {
            super.close();
            jedisPool.close();
        }
    }
}
