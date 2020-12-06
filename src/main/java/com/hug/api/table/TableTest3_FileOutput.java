package com.hug.api.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @ClassName: TableTest_FileOutput
 * @Description:
 * @Author: hug on 2020/10/27 18:36 (星期二)
 * @Version: 1.0
 */
public class TableTest3_FileOutput {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 读取文件数据
        tableEnv.connect(new FileSystem().path("first/hello.txt"))
                .withFormat(new OldCsv())
                .withSchema(
                        new Schema()
                                .field("id", DataTypes.STRING())
                                .field("ts", DataTypes.BIGINT())
                                .field("temp", DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable");

        // 查询转换
        // == 简单查询
        // ---- Table API
        Table inputTable = tableEnv.from("inputTable");
        Table resultTable = inputTable.select("id,temp").filter("id = 'sensor_2'");

        // ---- SQL
        Table sqlResultTable = tableEnv.sqlQuery("select id,count(id) as cnt from inputTable group by id");

        // == 聚合统计
        // ---- Table API
        Table aggTable = inputTable.groupBy("id").select("id,id.count as count");
        // ---- SQL
        Table aggSqlTable = tableEnv.sqlQuery("select id,count(id) as cnt from inputTable group by id");

        // 输出结果到文件
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
