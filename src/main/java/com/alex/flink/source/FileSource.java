package com.alex.flink.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liangxiaofei
 * @date 2021/3/11 10:17
 */
public class FileSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //从文件读取
        DataStream<String> dataStream = environment.readTextFile("data/sensor.txt");
        dataStream.print();

        environment.execute();
    }
}
