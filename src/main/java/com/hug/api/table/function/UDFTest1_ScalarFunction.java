package com.hug.api.table.function;

import com.hug.api.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

/**
 * @ClassName: UDFTest1_ScalarFunction
 * @Description:
 * @Author: hug on 2020/10/28 18:22 (星期三)
 * @Version: 1.0
 */
public class UDFTest1_ScalarFunction {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 读取文件
        DataStreamSource<String> inputStream = env.readTextFile("hello.txt");
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map((MapFunction<String, SensorReading>) value -> {
            String[] split = value.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });

        // DataStream ==> Table
        Table sensorTable = tableEnv.fromDataStream(dataStream);

        // table api
        MyHashCode myHashCode = new MyHashCode(21);
        tableEnv.registerFunction("hashCode", myHashCode);
        Table resultTable = sensorTable.select("id,ts,temp");

        // sql
        tableEnv.createTemporaryView("sensor", dataStream);
        Table resultSqlTable = tableEnv.sqlQuery("select id,ts,temp,hashCode(id) as hc from sensor");

        tableEnv.toAppendStream(resultTable, Row.class).print("api");
        tableEnv.toAppendStream(resultSqlTable,Row.class).print("sql");

        env.execute();
    }

    // 自定义标量函数，求String的Hash值
    public static class MyHashCode extends ScalarFunction{
        private int factor = 3;

        public MyHashCode(int factor) {
            this.factor = factor;
        }

        public int eval(String str) {
            return str.hashCode() * factor;
        }
    }
}
