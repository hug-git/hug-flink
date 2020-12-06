package com.hug.api.transform;

import com.hug.api.bean.SensorReading;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransFormTest1_MapFilterKeyMax {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream = env.readTextFile("first/hello.txt");
        SingleOutputStreamOperator<SensorReading> mapStream = dataStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });
        mapStream.print("map -- ");
        SingleOutputStreamOperator<SensorReading> filterStream = mapStream.filter(value -> value.getTemp() > 0 && value.getTemp() < 100);
        filterStream.print("filter -- ");
        KeyedStream<SensorReading, Tuple> keyedStream = filterStream.keyBy("id");
        SingleOutputStreamOperator<SensorReading> maxStream = keyedStream.maxBy("temp");
        maxStream.print("max -- ");

        dataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] split = value.split(",");
                for (String s : split) {
                    out.collect(s);
                }
            }
        }).print("flatMap -- ");

        env.execute();
    }
}
