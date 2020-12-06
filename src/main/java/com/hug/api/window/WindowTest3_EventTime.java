package com.hug.api.window;

import com.hug.api.bean.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @ClassName: WindowTest_EventTime
 * @Description:
 * @Author: hug on 2020/10/25 16:23 (星期日)
 * @Version: 1.0
 */
public class WindowTest3_EventTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputStream = env.socketTextStream("hadoop102", 7777);

        SingleOutputStreamOperator<SensorReading> dataStream = inputStream
                .map(line -> {
                    String[] split = line.split(",");
                    return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
                })
                // 设置时间戳和watermark
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(SensorReading element) {
                        return element.getTs() * 1000L;
                    }
                });

                // 分组开窗聚合，统计15秒内最大温度以及最近一次数据的时间戳
                        dataStream
                .keyBy("id")
                .timeWindow(Time.seconds(15))
                .reduce(new ReduceFunction<SensorReading>() {
                    @Override
                    public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                        return new SensorReading(value1.getId(), value2.getTs(), Math.max(value1.getTemp(), value2.getTemp()));
                    }
                })
                .print();

        env.execute();
    }
}
