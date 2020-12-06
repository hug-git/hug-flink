package com.hug.api.state;

import com.hug.api.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: StateTest_AppCase
 * @Description:
 * @Author: hug on 2020/10/26 19:31 (星期一)
 * @Version: 1.0
 */
public class StateTest2_AppCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputStream = env.socketTextStream("hadoop102", 7777);

        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        // 分组，针对每一个sensor判断前后两次温度差值
        SingleOutputStreamOperator<Tuple3<String, Double, Double>> warningStream = dataStream
                .keyBy("id")
                .flatMap(new TempChangeWarning(10.0));

        warningStream.print();

        env.execute();
    }


    private static class TempChangeWarning extends RichFlatMapFunction<SensorReading, Tuple3<String, Double,Double>> {
        // 阈值
        private Double threshold;
        // 上一个温度
        private ValueState<Double> lastTempState;

        public TempChangeWarning(double threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTemp", Double.class));
        }

        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            // 获取状态
            Double lastTemp = lastTempState.value();
            // 若不是第一个数据
            if (lastTemp != null) {
                // 差值大于阈值，则输出报警
                double diff = Math.abs(value.getTemp() - lastTemp);
                if (diff > threshold) {
                    out.collect(new Tuple3<String,Double, Double>(value.getId(),lastTemp,value.getTemp()));
                }
            }

            // 更新温度状态
            lastTempState.update(value.getTemp());
        }
    }
}
