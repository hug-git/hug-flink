package com.hug.api.process;

import com.hug.api.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName: ProcessFunTest_KeyedProcessFunction
 * @Description:
 * @Author: hug on 2020/10/26 20:37 (星期一)
 * @Version: 1.0
 */
public class ProcessFunTest1_KeyedProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> inputStream = env.socketTextStream("hadoop102", 7777);

        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map((MapFunction<String, SensorReading>) value -> {
            String[] split = value.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });

        // KeyedProcessFunction
        dataStream.keyBy("id")
                .process( new MyKeyedProcess() )
                .print();

        env.execute();
    }

    public static class MyKeyedProcess extends KeyedProcessFunction<Tuple,SensorReading,Integer> {

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<Integer> out) throws Exception {
            out.collect(1); // 主流输出
//            ctx.output(); // 侧输出流

            ctx.timerService().currentProcessingTime();
            // 注册定时器
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000L);
            ctx.timerService().deleteProcessingTimeTimer(value.getTs() * 1000L);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
            System.out.println("timer" + timestamp);
            out.collect(10);
            ctx.getCurrentKey();
        }
    }
}
