package com.alex.flink.state;

import com.alex.flink.beans.SensorReading;
import com.alex.flink.common.SensorReadingStream;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Tuple3;

/**
 * @author liangxiaofei
 * @date 2021/4/27 11:20
 */
public class KeyedStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStream<SensorReading> dataStream = SensorReadingStream.getDemoStream(environment);

        SingleOutputStreamOperator<Tuple3<String, Double, Double>> tempChangeStream = dataStream.keyBy(SensorReading::getId).flatMap(new TempChangeWarning(2d));
        tempChangeStream.print();

        environment.execute();
    }

    // 温度变化报警
    private static class TempChangeWarning extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {
        // 温度跳变阈值
        private final double threshold;
        // 上一次温度值
        private ValueState<Double> lastTempState;

        public TempChangeWarning(double threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<>("lastTempState", Double.class));
        }

        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            Double lastTemp = lastTempState.value();
            Double nowTemp = value.getTemperature();

            if (lastTemp != null) {
                double diff = nowTemp - lastTemp;
                if (Math.abs(diff) >= threshold) {
                    out.collect(new Tuple3<>(value.getId(), lastTemp, nowTemp));
                }
            }

            // 更新状态
            lastTempState.update(nowTemp);
        }
    }
}
