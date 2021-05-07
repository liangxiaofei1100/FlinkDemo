package com.alex.flink.table;

import com.alex.flink.beans.DataRoomV1;
import com.alex.flink.beans.DataRoomV2;
import org.apache.commons.io.FileUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.File;

/**
 * @author liangxiaofei
 * @date 2021/4/28 14:17
 */
public class MysqlApi2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        String sql = FileUtils.readFileToString(new File("sql/data_room-mysql.sql"), "utf-8");
        TableResult tableResult = tableEnvironment.executeSql(sql);
        tableResult.print();

        Table table = tableEnvironment.sqlQuery("select * from data_room");
        // 将结果转换为java对象
        DataStream<DataRoomV1> dataStream = tableEnvironment.toAppendStream(table, DataRoomV1.class);
        dataStream.print();
        environment.execute();
    }
}
