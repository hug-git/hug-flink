package com.hug.api.sink;

import com.hug.api.bean.SensorReading;
import com.hug.api.utils.MyPropertiesUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

/**
 * @ClassName: SinkTest_Kafka
 * @Description:
 * @Author: hug on 2020/10/24 9:39 (星期六)
 * @Version: 1.0
 */
public class SinkTest1_Kafka {
    public static void main(String[] args) throws Exception {
        Properties properties = MyPropertiesUtil.getProperties("config.properties");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> dataStream = env.readTextFile("first/hello.txt");

        SingleOutputStreamOperator<String> sensorStream = dataStream.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2])).toString();
        });

        DataStreamSink kafkaStream = sensorStream.addSink(
                new FlinkKafkaProducer011<String>("sensor", new SimpleStringSchema(),properties)
        );

        env.execute();

    }
}
