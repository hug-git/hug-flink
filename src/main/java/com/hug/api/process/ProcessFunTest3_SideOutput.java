package com.hug.api.process;

import com.hug.api.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @ClassName: ProcessFunTest_SideOutput
 * @Description:
 * @Author: hug on 2020/10/26 21:20 (星期一)
 * @Version: 1.0
 */
public class ProcessFunTest3_SideOutput {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> inputStream = env.socketTextStream("hadoop102", 7777);

        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map((MapFunction<String, SensorReading>) value -> {
            String[] split = value.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });

        // 定义一个输出流标签，表示低温流
        OutputTag<SensorReading> lowTempTag = new OutputTag<SensorReading>("lowTemp"){};

        // 以30度为界，划分流
        SingleOutputStreamOperator<SensorReading> highTempStream = dataStream
                .process(new ProcessFunction<SensorReading, SensorReading>() {
                    @Override
                    public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
                        if (value.getTemp() > 30) {
                            out.collect(value);
                        } else {
                            ctx.output(lowTempTag, value);
                        }
                    }
                });

        DataStream<SensorReading> lowTempStream = highTempStream.getSideOutput(lowTempTag);

        highTempStream.print("high");
        lowTempStream.print("low");

        env.execute();
    }
}
