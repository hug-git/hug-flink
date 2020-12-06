package com.hug.api.table.function;

import com.hug.api.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.elasticsearch.common.collect.Tuple;

/**
 * @ClassName: UDFTest2_TableFunction
 * @Description:
 * @Author: hug on 2020/10/28 19:02 (星期三)
 * @Version: 1.0
 */
public class UDFTest2_TableFunction {
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

        Table sensorTable = tableEnv.fromDataStream(dataStream);

        // table api
        tableEnv.registerFunction("split",new MySplit("_"));
        Table resultTable = sensorTable.joinLateral("split(id) as (word,length)")
                .select("id,ts,temp,word,length");
        // sql
        tableEnv.createTemporaryView("sensor", sensorTable);
        Table resultSqlTable = tableEnv.sqlQuery(
                "select id,ts,temp,word,length from sensor," +
                        "lateral table(split(id)) as si(word,length)"
        );

        tableEnv.toAppendStream(resultTable, Row.class).print("api");
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("sql");

        env.execute();
    }

    // TableFunction 拆分String ==> (word,length)
    public static class MySplit extends TableFunction<Tuple2<String, Integer>>{
        private String separator = ",";

        public MySplit(String separator) {
            this.separator = separator;
        }

        public void eval(String str) {
            String[] split = str.split(separator);
            for (String s : split) {
                collect(new Tuple2<String,Integer>(s,s.length()));
            }
        }
    }
}
