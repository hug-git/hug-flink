package com.hug.api.table.function;

import com.hug.api.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

/**
 * @ClassName: UDFTest3_AggregateFunction
 * @Description:
 * @Author: hug on 2020/10/28 19:21 (星期三)
 * @Version: 1.0
 */
public class UDFTest3_AggregateFunction {
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
        tableEnv.registerFunction("avgTemp", new AvgTemp());
        Table resultTable = sensorTable.groupBy("id")
                .aggregate("avgTemp(temp) as avgTemp")
                .select("id,avgTemp");
        // sql
        tableEnv.createTemporaryView("sensor", sensorTable);
        Table resultSqlTable = tableEnv.sqlQuery(
                "select id,avgTemp(temp) from sensor group by id"
        );

        tableEnv.toRetractStream(resultTable, Row.class).print("api");
        tableEnv.toRetractStream(resultSqlTable, Row.class).print("sql");
        env.execute();
    }

    public static class AvgTemp extends AggregateFunction<Double, Tuple2<Double,Integer>> {

        @Override
        public Double getValue(Tuple2<Double, Integer> accumulator) {
            return Math.round(accumulator.f0 * 100D / accumulator.f1)/100D;
        }

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0,0);
        }

        public void accumulate(Tuple2<Double, Integer> accumulator, Double temp) {
            accumulator.f0 += temp;
            accumulator.f1 += 1;
        }
    }
}
