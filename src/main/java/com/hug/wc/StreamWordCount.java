package com.hug.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        DataStreamSource<String> inputDataStream = env.readTextFile("D:\\temp\\week.txt");

        String[] infos = new String[]{"--host","hadoop102","--port","7777"};
        ParameterTool parameterTool = ParameterTool.fromArgs(infos);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
        DataStreamSource<String> inputDataStream = env.socketTextStream(host, port);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCount = inputDataStream
                .flatMap(new WordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1);
        wordCount.print();

        env.execute();
    }
}
