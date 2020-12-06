package com.hug.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment executionEnv = ExecutionEnvironment.getExecutionEnvironment();

        // 从文件读取数据，用一个DataSet来存储
        String inputPath = "D:\\temp\\week.txt";
        DataSource<String> inputDataSet = executionEnv.readTextFile(inputPath);

        // 对每一行数据切分，统计每个单词
        AggregateOperator<Tuple2<String, Integer>> sum = inputDataSet.flatMap(new MyFlatMapper())
                .groupBy(0)
                .sum(1);
        sum.print();

    }

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String,Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}


