package com.hug.api.table;

import com.hug.api.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @ClassName: TableTest_SimpleExample
 * @Description:
 * @Author: hug on 2020/10/27 10:54 (星期二)
 * @Version: 1.0
 */
public class TableTest1_SimpleExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputStream = env.readTextFile("first/hello.txt");

        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        // 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 基于DataStream创建表
        Table dataTable = tableEnv.fromDataStream(dataStream);
        // 简单转换
        Table resultTable = dataTable.select("id,temp").where("id = 'sensor_2'");
//        Table resultTable = dataTable.select("id,temp").filter("id = 'sensor_2'");

        // SQL
        tableEnv.createTemporaryView("sensor", dataTable);
        Table sqlResultTable = tableEnv.sqlQuery("select id,temp from sensor where id = 'sensor_2'");

        // 转换成流打印输出
        tableEnv.toAppendStream(resultTable, Row.class).print("result");
//        tableEnv.toAppendStream(sqlResultTable, Row.class).print("sql");

        env.execute();
    }
}
