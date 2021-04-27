package com.alex.flink.state;

import com.alex.flink.beans.SensorReading;
import com.alex.flink.common.SensorReadingStream;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liangxiaofei
 * @date 2021/4/27 10:55
 */
public class KeyedState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        DataStream<SensorReading> dataStream = SensorReadingStream.getDemoStream(environment);

        SingleOutputStreamOperator<Integer> countStream = dataStream
                .keyBy((KeySelector<SensorReading, String>) SensorReading::getId)
                .map(new KeyCountMapper());
        countStream.print();

        environment.execute();
    }

    //
    public static class KeyCountMapper extends RichMapFunction<SensorReading, Integer> {
        private ValueState<Integer> countState;

        // 其他类型状态的声明
        private ListState<String> listState;
        private MapState<String, Double> mapState;
        private ReducingState<SensorReading> reducingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<>("countState", Integer.class));
            listState = getRuntimeContext().getListState(new ListStateDescriptor<>("listState", String.class));
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("mapState", String.class, Double.class));
            reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<>("reduceState",null,SensorReading.class));
        }

        @Override
        public Integer map(SensorReading value) throws Exception {
            Integer count = countState.value();
            if (count == null) {
                count = 0;
            }

            count++;
            countState.update(count);

            // list state
            Iterable<String> list = listState.get();
            listState.add(value.getId());
            // map state
            Double d = mapState.get(value.getId());
            mapState.put(value.getId(), value.getTemperature());

            return count;
        }
    }
}
