package com.hug.api.table;

import com.hug.api.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @ClassName: TableTest5_TimeAndWindow
 * @Description:
 * @Author: hug on 2020/10/27 19:29 (星期二)
 * @Version: 1.0
 */
public class TableTest5_TimeAndWindow {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 获取数据
        DataStreamSource<String> inputStream = env.readTextFile("hello.txt");
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream
                .map((MapFunction<String, SensorReading>) value -> {
                    String[] split = value.split(",");
                    return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(SensorReading element) {
                        return element.getTs();
                    }
                });

        // DataStream ==> Table
//        Table table = tableEnv.fromDataStream(inputStream, "id,ts as time,temp,pt.proctime");
        Table sensorTable = tableEnv.fromDataStream(dataStream, "id,ts.rowtime as times,temp");
//        sensorTable.printSchema();
        // 窗口聚合
        // == Group Windows
        // ---- Table API
        Table resultTable = sensorTable
                .window(Tumble.over("10.seconds").on("times").as("tw"))
                .groupBy("id,tw")
                .select("id,id.count,temp.avg,tw.end");
        // ---- SQL
        tableEnv.createTemporaryView("sensor",sensorTable);
        Table sqlResultTable = tableEnv.sqlQuery(
                "select id,count(id) as cnt,avg(temp) as avgTemp,tumble_end(times,interval '10' second) " +
                        "from sensor group by id,tumble(times,interval '10' second)"
        );

//        tableEnv.toRetractStream(resultTable, Row.class).print("api");
//        tableEnv.toRetractStream(sqlResultTable, Row.class).print("sql");

        // == Over Windows
        // ---- Table API
        Table overResultTable = sensorTable
                .window(Over.partitionBy("id").orderBy("times").preceding("2.rows").as("ow"))
                .select("id,times,id.count over ow,temp.avg over ow");
        // ---- SQL
        Table overResultSqlTable = tableEnv.sqlQuery(
                "select id,times,count(id) over ow,avg(temp) over ow from sensor " +
                        "window ow as (partition by id order by times rows between 2 preceding and current " +
                        "row)"
        );

        tableEnv.toAppendStream(overResultTable, Row.class).print("api");
        tableEnv.toAppendStream(overResultSqlTable, Row.class).print("sql");

        env.execute();
    }
}
