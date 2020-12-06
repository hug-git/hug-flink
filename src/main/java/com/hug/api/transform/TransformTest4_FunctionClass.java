package com.hug.api.transform;

import com.hug.api.bean.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: FunctionClassTest
 * @Description:
 * @Author: hug on 2020/10/23 20:27 (星期五)
 * @Version: 1.0
 */
public class TransformTest4_FunctionClass {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> dataStream = env.readTextFile("first/hello.txt");

        SingleOutputStreamOperator<SensorReading> mapStream = dataStream.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });

        mapStream.map(new MyRichMapper()).print();

        env.execute();
    }

    private static class MyRichMapper extends RichMapFunction<SensorReading, Integer> {
        @Override
        public Integer map(SensorReading value) throws Exception {
            System.out.println("-map-");
            return getRuntimeContext().getIndexOfThisSubtask();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("-open-");
            super.open(parameters);
        }

        @Override
        public void close() throws Exception {
            System.out.println("-close-");
            super.close();
        }
    }


}
