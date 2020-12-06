package com.hug.api.table;

import com.hug.api.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @ClassName: TableTest_CommonApi
 * @Description:
 * @Author: hug on 2020/10/27 18:06 (星期二)
 * @Version: 1.0
 */
public class TableTest2_CommonApi {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ==================================Old============
        // 基于老版本planner的流处理
        EnvironmentSettings setting = EnvironmentSettings.newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env, setting);

        // 基于老版本planner的批处理
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment batchTableEnvironment = BatchTableEnvironment.create(batchEnv);

        // ==================================Blink===========
        // 基于blink版本planner的流处理
        EnvironmentSettings blinkSetting = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment blinkStreamTableEnvironment = StreamTableEnvironment.create(env, blinkSetting);

        // 基于blink版本planner的批处理
        EnvironmentSettings blinkBatchSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();
        TableEnvironment blinkBatchTableEnvironment = TableEnvironment.create(blinkBatchSettings);

        // 连接外部系统，读取数据，创建表
        // 读取文件
        tableEnv.connect(new FileSystem().path("first/hello.txt"))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable");

        // 查询转换
        // ==简单查询
        // ----Table API
        Table inputTable = tableEnv.from("inputTable");
        Table resultTable = inputTable.select("id,temp").filter("id === 'sensor_2'");
        // ----SQL
        Table resultSqlTable = tableEnv.sqlQuery("select id, temp from inputTable where id = 'sensor_2'");

        // ==聚合统计
        // ----Table API
        Table aggTable = inputTable.groupBy("id").select("id,id.count as count");
        // ----SQL
        Table aggSqlTable = tableEnv.sqlQuery("select id, count(id) as cnt from inputTable group by id");

        // 输出结果表到文件
        tableEnv.connect(new FileSystem().path("first/output.txt"))
                .withFormat(new OldCsv())
                .withSchema(
                        new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temp", DataTypes.DOUBLE())

                )
                .createTemporaryTable("outputTable");

        resultTable.insertInto("outputTable");

        env.execute();
    }
}
