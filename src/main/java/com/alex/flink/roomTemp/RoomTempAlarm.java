package com.alex.flink.roomTemp;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Duration;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

/**
 * @author liangxiaofei
 * @date 2021/4/7 10:21
 */
public class RoomTempAlarm {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.18.62:9092");
        properties.setProperty("group.id", "flinkTest");

        DataStream<RoomTempData> dataStream = environment.addSource(new FlinkKafkaConsumer<>("gatherData", new KafkaDeserializationSchema<RoomTempData>() {
            private final ObjectMapper objectMapper = new ObjectMapper();

            @Override
            public TypeInformation<RoomTempData> getProducedType() {
                return TypeInformation.of(RoomTempData.class);
            }

            @Override
            public boolean isEndOfStream(RoomTempData nextElement) {
                return false;
            }

            @Override
            public RoomTempData deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                String jsonString = new String(record.value());

                Map<String, String> map = objectMapper.readValue(jsonString, new TypeReference<Map<String, String>>() {
                });
                String clientSn = map.get("clientSn");
                // 800表示室内温度和湿度
                String type = map.get("type");
                // 数据格式，温度-湿度，例如：18.4-62.5
                String data = map.get("data");
                String source = map.get("source");

                String[] dataArray = data.split("-");
                Float temperature = Float.valueOf(dataArray[0]);
                Float humidity = Float.valueOf(dataArray[1]);

                RoomTempData roomTempData = new RoomTempData();
                roomTempData.setClientSn(clientSn);
                roomTempData.setTemperature(temperature);
                roomTempData.setHumidity(humidity);
                roomTempData.setDataTime(new Date(record.timestamp()));
                return roomTempData;
            }
        }, properties));

        dataStream.assignTimestampsAndWatermarks(WatermarkStrategy.<RoomTempData>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((SerializableTimestampAssigner<RoomTempData>) (element, recordTimestamp) -> {
                    System.out.println("time->" + element.getDataTime());
                    return element.getDataTime().getTime();
                }));

        FlinkJedisConfigBase config = new FlinkJedisPoolConfig.Builder().setHost("192.168.18.61").setPort(6379).build();
        dataStream.addSink(new RedisSink<>(config, new RedisMapper<RoomTempData>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                // 定义保存数据到redis的命令，存成hash表 hset sensor_temp id temperature
                return new RedisCommandDescription(RedisCommand.SET, null);
            }

            @Override
            public String getKeyFromData(RoomTempData s) {
                return "roomData:1001:" + s.getClientSn();
            }

            @Override
            public String getValueFromData(RoomTempData s) {
                ObjectMapper objectMapper = new ObjectMapper();
                try {
                    return objectMapper.writeValueAsString(s);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }

                throw new RuntimeException();
            }


        }));

        dataStream.print("result");
        environment.execute();
    }

}
