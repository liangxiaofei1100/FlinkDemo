package com.alex.flink.state;

import com.alex.flink.beans.SensorReading;
import com.alex.flink.common.SensorReadingStream;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liangxiaofei
 * @date 2021/4/27 10:20
 */
public class OperatorState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        DataStream<SensorReading> dataStream = SensorReadingStream.getDemoStream(environment);

        // 定义一个有状态的map操作，统计当前分区数据个数
        SingleOutputStreamOperator<Integer> countStream = dataStream.map(new CountMapper());
        countStream.print();

        environment.execute();
    }

    public static class CountMapper implements MapFunction<SensorReading, Integer>, CheckpointedFunction {
        // 定义一个本地变量，作为算子状态
        private int localCount = 0;
        private ListState<Integer> countPerPartition;

        @Override
        public Integer map(SensorReading value) throws Exception {
            localCount++;
            return localCount;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            // the keyed state is always up to date anyways
            // just bring the per-partition state in shape
            countPerPartition.clear();
            countPerPartition.add(localCount);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            countPerPartition = context.getOperatorStateStore()
                    .getListState(new ListStateDescriptor<>("perPartitionCount", Integer.class));
            // initialize the "local count variable" based on the operator state
            for (Integer l : countPerPartition.get()) {
                localCount += l;
            }
        }
    }
}
