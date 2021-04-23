package com.alex.flink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author liangxiaofei
 * @date 2021/3/8 17:02
 */
public class StreamWordCount2 {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        environment.setParallelism(8);

        // 从socket中读取数据
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
        DataStream<String> dataStream = environment.socketTextStream(host, port);
        DataStream<Tuple2<String, Integer>> result = dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String word, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = word.split("[ ,.;!?]");
                for (String w : words) {
                    collector.collect(new Tuple2<String, Integer>(w, 1));
                }
            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
            public Object getKey(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.f0;
            }
        }).sum(1);

        result.print();

        // 执行任务
        environment.execute();
    }
}
