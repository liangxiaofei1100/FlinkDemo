package com.alex.flink.process;

import com.alex.flink.beans.SensorReading;
import com.alex.flink.common.SensorReadingStream;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 监控温度传感器的温度值，如果温度在10秒内连续上升，则报警
 *
 * @author liangxiaofei
 * @date 2021/5/10 10:25
 */
public class ProcessFunctionDemo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStream<SensorReading> dataStream = SensorReadingStream.getDemoStream(environment);
        SingleOutputStreamOperator<String> warningDataStream = dataStream.keyBy(SensorReading::getId).process(new TempContinueIncreaseWarningProcessFunction(10));
        warningDataStream.print();

        environment.execute();
    }

    private static class TempContinueIncreaseWarningProcessFunction extends KeyedProcessFunction<String, SensorReading, String> {
        // 统计的时间间隔
        private int intervalSecond;
        private ValueState<Double> lastTempState;
        private ValueState<Long> timerState;

        public TempContinueIncreaseWarningProcessFunction(int intervalSecond) {
            this.intervalSecond = intervalSecond;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<>("lastTempState", Double.class));
            timerState = getRuntimeContext().getState(new ValueStateDescriptor<>("timerState", Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            Double lastTemp = lastTempState.value();
            Long timerTs = timerState.value();

            // 温度上升，并且没有定时器
            if (timerTs == null && lastTemp != null && value.getTemperature() > lastTemp) {
                long ts = ctx.timerService().currentWatermark() + intervalSecond * 1000;
                ctx.timerService().registerEventTimeTimer(ts);
                timerState.update(ts);
            }

            // 如果温度下降，删除定时器
            else if (timerTs != null && value.getTemperature() < lastTemp) {
                ctx.timerService().deleteEventTimeTimer(timerTs);
                timerState.clear();
            }

            // 更新温度值
            lastTempState.update(value.getTemperature());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            out.collect("传感器报警" + ctx.getCurrentKey() + "温度值连续" + intervalSecond + "秒上升");
            timerState.clear();
        }

        @Override
        public void close() throws Exception {
            super.close();
            lastTempState.clear();
            timerState.clear();
        }
    }
}
