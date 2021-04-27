package com.alex.flink.common;

import com.alex.flink.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liangxiaofei
 * @date 2021/4/27 10:27
 */
public class SensorReadingStream {
    public static DataStream<SensorReading> getDemoStream(StreamExecutionEnvironment environment) {
        //从文件读取
        DataStream<String> inputStream = environment.readTextFile("data/sensor.txt");
        // 转换为对象
        return inputStream.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
        });
    }
}
