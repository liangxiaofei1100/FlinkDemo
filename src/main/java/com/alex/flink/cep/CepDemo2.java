package com.alex.flink.cep;

import com.alex.flink.beans.SensorReading;
import com.alex.flink.common.SensorReadingStream;
import com.alex.flink.util.WatermarkUtil;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @author liangxiaofei
 * @date 2021/3/19 15:28
 */
public class CepDemo2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStream<SensorReading> dataStream = SensorReadingStream.getDemoStream(environment);
        KeyedStream<SensorReading, String> keyedStream = dataStream.keyBy(SensorReading::getId);
        // 打印
        WatermarkUtil.printTime(keyedStream);

        Pattern<SensorReading, SensorReading> pattern = Pattern.<SensorReading>begin("start").times(2).within(Time.seconds(10));

        OutputTag<SensorReading> timeoutTag = new OutputTag<SensorReading>("timeout") {
        };
        PatternStream<SensorReading> patternStream = CEP.pattern(keyedStream, pattern);
        DataStream<SensorReading> timeoutStream = patternStream.flatSelect(timeoutTag, new PatternFlatTimeoutFunction<SensorReading, SensorReading>() {
            @Override
            public void timeout(Map<String, List<SensorReading>> pattern, long timeoutTimestamp, Collector<SensorReading> out) throws Exception {
                List<SensorReading> list = pattern.get("start");
                System.out.println("timeout " + list);
                out.collect(list.get(0));
            }
        }, new PatternFlatSelectFunction<SensorReading, SensorReading>() {
            @Override
            public void flatSelect(Map<String, List<SensorReading>> pattern, Collector<SensorReading> out) throws Exception {
                List<SensorReading> list = pattern.get("start");
                System.out.println("normal " + list);
            }
        }).getSideOutput(timeoutTag);
        timeoutStream.print("result");

        dataStream.print("data");
        environment.execute();
    }
}
