package com.alex.flink.table;

import com.alex.flink.beans.DataRoom;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.File;

/**
 * @author liangxiaofei
 * @date 2021/5/6 16:14
 */
public class JoinApi {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        String sql = FileUtils.readFileToString(new File("sql/data_room-mysql.sql"), "utf-8");
        tableEnvironment.executeSql(sql);

        sql = FileUtils.readFileToString(new File("sql/base_room_data_collector-mysql.sql"), "utf-8");
        tableEnvironment.executeSql(sql);

        sql = FileUtils.readFileToString(new File("sql/base_building-mysql.sql"), "utf-8");
        tableEnvironment.executeSql(sql);

        sql = FileUtils.readFileToString(new File("sql/base_community-mysql.sql"), "utf-8");
        tableEnvironment.executeSql(sql);

        Table table = tableEnvironment.sqlQuery("select a.id ," +
                "    a.collector_number ,\n" +
                "    a.data_time ,\n" +
                "    a.temperature ,\n" +
                "    a.humidity, " +
                "b.id as collector_id, " +
                "b.enterprise_id, " +
                "b.building_id, " +
                "c.community_id, " +
                "d.exch_station_id as station_id " +
                "from data_room a left join base_room_data_collector b " +
                "on a.collector_number = b.number " +
                "left join base_building c " +
                "on b.building_id = c.id " +
                "left join base_community d " +
                "on c.community_id = d.id ");
        // 将结果转换为java对象
        DataStream<Tuple2<Boolean, DataRoom>> dataStream = tableEnvironment.toRetractStream(table, DataRoom.class);
        dataStream.print();

        environment.execute();
    }
}
