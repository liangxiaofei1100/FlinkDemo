package com.alex.flink.process;

import com.alex.flink.beans.SensorReading;
import com.alex.flink.common.SensorReadingStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author liangxiaofei
 * @date 2021/5/10 13:37
 */
public class SideOutputDemo {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStream<SensorReading> dataStream = SensorReadingStream.getDemoStream(environment);

        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("lowTemp"){};

        SingleOutputStreamOperator<SensorReading> warningDataStream = dataStream.process(new SideOutputProcess());
        warningDataStream.print("high-temp");
        warningDataStream.getSideOutput(outputTag).print("low-temp");

        environment.execute();
    }

    private static class SideOutputProcess extends ProcessFunction<SensorReading, SensorReading> {
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("lowTemp"){};
        @Override
        public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
            Double temperature = value.getTemperature();
            if (temperature > 20) {
                out.collect(value);
            } else {
                ctx.output(outputTag, value);
            }
        }
    }
}
