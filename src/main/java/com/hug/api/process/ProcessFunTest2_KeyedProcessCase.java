package com.hug.api.process;

import com.hug.api.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName: ProcessFunTest_KeyedProcessCase
 * @Description:
 * @Author: hug on 2020/10/26 21:03 (星期一)
 * @Version: 1.0
 */
public class ProcessFunTest2_KeyedProcessCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> inputStream = env.socketTextStream("hadoop102", 7777);

        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map((MapFunction<String, SensorReading>) value -> {
            String[] split = value.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });

        // 监控传感器温度，10秒内连续上升则报警
        dataStream
                .keyBy("id")
                .process(new TempIncreaseWarning(10))
                .print();

        env.execute();
    }

    public static class TempIncreaseWarning extends KeyedProcessFunction<Tuple,SensorReading,String> {
        // 时间间隔
        private Integer interval;
        // 状态
        private ValueState<Double> lastTempState;
        private ValueState<Long> timerTsState;

        public TempIncreaseWarning(Integer interval) {
            this.interval = interval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTemp",Double.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTs", Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            // 抽取上一次的温度值和定时器时间戳
            Double lastTemp = lastTempState.value();
            Long timerTs = timerTsState.value();

            // 更新状态
            lastTempState.update(value.getTemp());

            // 不是第一个数据，且温度上升
            if (timerTs == null && lastTemp !=null && value.getTemp() > lastTemp) {
                long ts = ctx.timerService().currentProcessingTime() + interval * 1000L;
                ctx.timerService().registerProcessingTimeTimer(ts);
                // 保存定时器时间戳到状态
                timerTsState.update(ts);
            }
            // 若温度值下降，则删除定时器，重新开始
            else if (lastTemp !=null && value.getTemp() < lastTemp && timerTs != null) {
                ctx.timerService().deleteProcessingTimeTimer(timerTs);
                // 清除
                timerTsState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，报警
            out.collect("传感器" + ctx.getCurrentKey() + "温度连续" + interval + "秒上升：报警");
            // 清除
            timerTsState.clear();
        }
    }
}
