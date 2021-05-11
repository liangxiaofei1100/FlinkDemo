package com.alex.flink.process;

import com.alex.flink.beans.SensorReading;
import com.alex.flink.common.SensorReadingStream;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author liangxiaofei
 * @date 2021/5/10 9:58
 */
public class ProcessFunctionDemo1 {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStream<SensorReading> dataStream = SensorReadingStream.getDemoStream(environment);
        dataStream.keyBy(SensorReading::getId).process(new MyProcessFunction());
    }

    private static class MyProcessFunction extends KeyedProcessFunction<String, SensorReading, Integer> {

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<Integer> out) throws Exception {
            Long timestamp = ctx.timestamp();
            ctx.getCurrentKey();
//            ctx.output(null,null);
            TimerService timerService = ctx.timerService();
            timerService.currentProcessingTime();
            timerService.currentWatermark();
            timerService.registerEventTimeTimer(timestamp + 1000);
            timerService.registerProcessingTimeTimer(timestamp + 1000);
            timerService.deleteEventTimeTimer(timestamp + 1000);
            timerService.deleteProcessingTimeTimer(timestamp + 1000);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
            super.onTimer(timestamp, ctx, out);

        }
    }
}
