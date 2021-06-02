package com.alex.flink.cep;

import com.alex.flink.beans.SensorReading;
import com.alex.flink.common.SensorReadingStream;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;

/**
 * @author liangxiaofei
 * @date 2021/3/19 15:28
 */
public class CepDemo1 {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStream<SensorReading> dataStream = SensorReadingStream.getDemoStream(environment);
        KeyedStream<SensorReading, String> keyedStream = dataStream.keyBy(SensorReading::getId);

        Pattern<SensorReading, SensorReading> pattern = Pattern.<SensorReading>begin("start").where(new SimpleCondition<SensorReading>() {
            @Override
            public boolean filter(SensorReading value) throws Exception {
                return value.getTemperature() > 11;
            }
        });

        PatternStream<SensorReading> patternStream = CEP.pattern(keyedStream, pattern);
        SingleOutputStreamOperator<SensorReading> resultStream = patternStream.select(new PatternSelectFunction<SensorReading, SensorReading>() {
            @Override
            public SensorReading select(Map<String, List<SensorReading>> pattern) throws Exception {
                List<SensorReading> list = pattern.get("start");
                return list.get(0);
            }
        });

        resultStream.print("result");

        dataStream.print("data");
        environment.execute();
    }
}
