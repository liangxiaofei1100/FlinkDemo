package com.alex.flink.window;

import com.alex.flink.beans.SensorReading;
import com.alex.flink.common.SensorReadingStream;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author liangxiaofei
 * @date 2021/3/12 16:05
 */
public class TimeWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStream<SensorReading> dataStream = SensorReadingStream.getDemoStream(environment);

        // 开窗测试
        KeyedStream<SensorReading, String> keyedStream = dataStream.keyBy((KeySelector<SensorReading, String>) SensorReading::getId);

        // 滚动时间窗口
        // 滑动时间窗口
        // 回话窗口
        // 滚动计数窗口
        // 滑动计数窗口

        DataStream<Integer> resultStream = keyedStream
//                .window(SlidingProcessingTimeWindows.of(Time.minutes(10), Time.minutes(5)));
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {

                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(SensorReading value, Integer accumulator) {
                        return accumulator + 1;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                });
        resultStream.print("TimeWindowDemo");

        resultStream = keyedStream
//                .window(SlidingProcessingTimeWindows.of(Time.minutes(10), Time.minutes(5)));
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .apply(new WindowFunction<SensorReading, Integer, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<SensorReading> input, Collector<Integer> out) throws Exception {

                    }
                });
        resultStream.print();

        environment.execute();
    }
}
