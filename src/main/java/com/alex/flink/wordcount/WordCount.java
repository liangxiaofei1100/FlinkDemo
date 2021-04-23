package com.alex.flink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author liangxiaofei
 * @date 2021/3/8 15:59
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        // 从文件中读取数据
        String filePath = "data/hello.txt";
        DataSet<String> dataSource = environment.readTextFile(filePath);
        DataSet<Tuple2<String, Integer>> result = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String word, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = word.split("[ ,.;!?]");
                for (String w : words) {
                    collector.collect(new Tuple2<String, Integer>(w, 1));
                }
            }
        }).groupBy(0).sum(1);
        result.print();
    }
}
