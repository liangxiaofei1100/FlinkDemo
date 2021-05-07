package com.alex.flink.sink;

import com.alex.flink.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author liangxiaofei
 * @date 2021/3/12 14:10
 */
public class RedisSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataSource = environment.readTextFile("data/sensor.txt");
        DataStream<SensorReading> dataStream = dataSource
                .map((MapFunction<String, SensorReading>) s -> {
                    String[] strings = s.split(",");
                    return new SensorReading(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
                });
        FlinkJedisConfigBase config = new FlinkJedisPoolConfig.Builder().setHost("192.168.18.61").setPort(6379).build();
        dataStream.addSink(new RedisSink<>(config, new RedisMapper<SensorReading>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                // 定义保存数据到redis的命令，存成hash表 hset sensor_temp id temperature
                return new RedisCommandDescription(RedisCommand.HSET, "sensor_temp");
            }

            @Override
            public String getKeyFromData(SensorReading s) {
                return s.getId();
            }

            @Override
            public String getValueFromData(SensorReading s) {
                return s.getTemperature().toString();
            }
        }));

        environment.execute();
    }
}
