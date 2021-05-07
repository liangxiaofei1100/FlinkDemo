package com.alex.flink.cache;

import com.alex.flink.beans.DataRoomV1;
import com.alex.flink.beans.DataRoomV2;
import com.alex.flink.mapper.MapFunctionWithDataBase;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author liangxiaofei
 * @date 2021/5/6 16:54
 */
public class CacheApi {
    static Logger logger = LoggerFactory.getLogger(CacheApi.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        String sql = FileUtils.readFileToString(new File("sql/data_room-mysql.sql"), "utf-8");
        TableResult tableResult = tableEnvironment.executeSql(sql);
        tableResult.print();

        Table table = tableEnvironment.sqlQuery("select * from data_room limit 1000");
        DataStream<DataRoomV1> dataStream = tableEnvironment.toAppendStream(table, DataRoomV1.class);
        dataStream.print();

        DataStream<DataRoomV2> dataStream2 = dataStream
                .keyBy(DataRoomV1::getCollector_number)
                .map(new DataRoomMapper("com.mysql.cj.jdbc.Driver",
                        "jdbc:mysql://192.168.18.60:3306/epoo_cloud_base",
                        "root",
                        "123456"));
        dataStream2.print();

        environment.execute();
    }

    public static class DataRoomMapper extends MapFunctionWithDataBase<DataRoomV1, DataRoomV2> {
        /**
         * 采集器ID
         */
        private ValueState<Integer> collectorIdState;
        /**
         * 楼栋ID
         */
        private ValueState<Integer> buildIdState;

        public DataRoomMapper(String driver, String url, String username, String password) {
            super(driver, url, username, password);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.minutes(10))
                    .build();
            // 采集器ID
            ValueStateDescriptor<Integer> collectorIdStateDes = new ValueStateDescriptor<>("collectorIdState", Integer.class);
            collectorIdStateDes.enableTimeToLive(ttlConfig);
            this.collectorIdState = getRuntimeContext().getState(collectorIdStateDes);

            // 楼栋ID
            ValueStateDescriptor<Integer> buildIdStateDes = new ValueStateDescriptor<>("buildIdState", Integer.class);
            buildIdStateDes.enableTimeToLive(ttlConfig);
            this.buildIdState = getRuntimeContext().getState(buildIdStateDes);
        }

        @Override
        public DataRoomV2 map(DataRoomV1 value) throws Exception {
            String collector_number = value.getCollector_number();

            Integer collectorId = collectorIdState.value();
            Integer buildId = buildIdState.value();
            if (collectorId == null) {
                logger.info("Get collector info from database, collector number: " + collector_number);
                // 从数据库读取数据
                String sql = "select * from base_room_data_collector where number = ?";
                try (Connection connection = getDataSource().getConnection()) {
                    try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                        preparedStatement.setString(1, collector_number);
                        try (ResultSet resultSet = preparedStatement.executeQuery()) {
                            if (resultSet.next()) {
                                collectorId = resultSet.getInt("id");
                                collectorIdState.update(collectorId);

                                buildId = resultSet.getInt("building_id");
                                buildIdState.update(buildId);
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            DataRoomV2 dataRoomV2 = new DataRoomV2();
            dataRoomV2.setId(value.getId());
            dataRoomV2.setCollector_number(collector_number);
            dataRoomV2.setData_time(value.getData_time());
            dataRoomV2.setTemperature(value.getTemperature());
            dataRoomV2.setHumidity(value.getHumidity());
            dataRoomV2.setCollector_id(collectorId);
            dataRoomV2.setBuilding_id(buildId);
            return dataRoomV2;
        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }
}
