package com.alex.flink.util;

import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author liangxiaofei
 * @date 2021/6/2 9:22
 */
public class WatermarkUtil {
    public static <T, KEY> void printTime(KeyedStream<T, KEY> stream) {
        stream.process(new KeyedProcessFunction<KEY, T, Object>() {
            @Override
            public void processElement(T value, Context ctx, Collector<Object> out) throws Exception {
                long watermark = ctx.timerService().currentWatermark();
                System.out.println("watermark: " + watermark);

                long processingTime = ctx.timerService().currentProcessingTime();
                System.out.println("processingTime: " + processingTime);

                Long timestamp = ctx.timestamp();
                System.out.println("timestamp: " + timestamp);
            }
        });
    }
}
