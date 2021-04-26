package com.alex.flink.table;

import org.apache.commons.io.FileUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.io.File;

/**
 * @author liangxiaofei
 * @date 2021/4/25 16:44
 */
public class CommonApi {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        // 流处理
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        // 批处理
        EnvironmentSettings batchEnvironmentSettings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment batchTableEnvironment = TableEnvironment.create(batchEnvironmentSettings);

        // 表的创建
        String sql = FileUtils.readFileToString(new File("sql/sensor-file.sql"), "utf-8");
        TableResult tableResult = tableEnvironment.executeSql(sql);
        tableResult.print();

        Table table = tableEnvironment.sqlQuery("select * from sensor_data");
        tableEnvironment.toAppendStream(table, Row.class).print();

        environment.execute();
    }
}
