package com.hug.api.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.DataType;

/**
 * @ClassName: TableTest4_KafkaPipeline
 * @Description:
 * @Author: hug on 2020/10/27 19:14 (星期二)
 * @Version: 1.0
 */
public class TableTest4_KafkaPipeline {
    public static void main(String[] args) {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 读取kafka数据
        tableEnv
                .connect(new Kafka()
                        .version("0.11")
                        .topic("sensor")
                        .property("zookeeper.connect", "hadoop102:2181")
                        .property("bootstrap.servers", "hadoop102:9092")
                )
                .withFormat(new Csv())
                .withSchema(
                        new Schema()
                                .field("id", DataTypes.STRING())
                                .field("ts", DataTypes.BIGINT())
                                .field("temp", DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable");

        // 查询转换
        Table inputTable = tableEnv.from("inputTable");
        Table resultTable = inputTable.select("id,temp").filter("id = 'sensor_2'");

        // 输出结果到kafka
        tableEnv
                .connect(new Kafka()
                        .version("0.11")
                        .topic("sensor")
                        .property("zookeeper.connect", "hadoop102:2181")
                        .property("bootstrap.servers", "hadoop102:9092")
                )
                .withFormat(new Csv())
                .withSchema(
                        new Schema()
                                .field("id", DataTypes.STRING())
                                .field("temperature", DataTypes.DOUBLE())
                )
                .createTemporaryTable("outputTable");
        resultTable.insertInto("outputTable");

    }
}
