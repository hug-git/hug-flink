package com.hug.api.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest6_Shuffle {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("first/hello.txt");

        inputStream.print("input");

        DataStream<String> resultStream = inputStream.shuffle();
        resultStream.print("shuffle");

        env.execute();
    }
}
