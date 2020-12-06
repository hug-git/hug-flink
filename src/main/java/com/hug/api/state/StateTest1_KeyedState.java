package com.hug.api.state;

import com.hug.api.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

/**
 * @ClassName: StateTest_KeyedState
 * @Description:
 * @Author: hug on 2020/10/26 10:23 (星期一)
 * @Version: 1.0
 */
public class StateTest1_KeyedState {
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

        dataStream
                .keyBy("id")
                .map(new MyKeyedMapper())
                .print();

        env.execute();
    }

    // 算子状态
    private static class MyMapper implements MapFunction<SensorReading, Integer>, ListCheckpointed<Integer> {
        // 定义一个属性作为算子状态
        Integer count = 0;

        @Override
        public Integer map(SensorReading value) throws Exception {
            count++;
            return count;
        }

        // 做快照
        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(count);
        }

        // 恢复
        @Override
        public void restoreState(List<Integer> state) throws Exception {
            for (Integer num : state) {
                count += num;
            }
        }
    }

    // 键控状态
    private static class MyKeyedMapper extends RichMapFunction<SensorReading, Integer> {
        // 数据类型
        ValueState<Integer> countState;
        ListState<Integer>  myListState;
        ReducingState<SensorReading> myReducingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("count", Integer.class));
            myListState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("myList", Integer.class));
            myReducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<SensorReading>("myReduce", new ReduceFunction<SensorReading>() {
                @Override
                public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                    return new SensorReading(value1.getId(),value2.getTs(), Math.max(value1.getTemp(),value2.getTemp()));
                }
            }, SensorReading.class));
            super.open(parameters);
        }

        @Override
        public Integer map(SensorReading value) throws Exception {
            Integer count = countState.value();
            count++;
            countState.update(count);
            myListState.add(count);
            myReducingState.add(new SensorReading("sensor_7",1603439651297L,32.0));
            return count;
        }
    }

}
