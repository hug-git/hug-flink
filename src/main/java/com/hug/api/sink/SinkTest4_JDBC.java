package com.hug.api.sink;

import com.hug.api.bean.SensorReading;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @ClassName: SinkTest_JDBC
 * @Description:
 * @Author: hug on 2020/10/25 10:55 (星期日)
 * @Version: 1.0
 */
public class SinkTest4_JDBC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> dataStream = env.readTextFile("first/hello.txt");

        SingleOutputStreamOperator<SensorReading> sensorStream = dataStream.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });

        sensorStream.addSink(new MyJdbcSink());

        env.execute();
    }

    public static class MyJdbcSink extends RichSinkFunction<SensorReading> {
        // 定义连接
        Connection connection = null;
        PreparedStatement insertStmt = null;
        PreparedStatement updateStmt = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 创建连接
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/flink", "root", "123456");
            // 创建预编译语句
            insertStmt = connection.prepareStatement("insert into sensor_temp(id,temp) values (?,?)");
            updateStmt = connection.prepareStatement("update sensor_temp set temp = ? where id = ?");
        }

        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            // 直接update，没有更新则insert
            updateStmt.setDouble(1,value.getTemp());
            updateStmt.setString(2,value.getId());
            updateStmt.execute();
            if (updateStmt.getUpdateCount() == 0) {
                insertStmt.setString(1,value.getId());
                insertStmt.setDouble(2,value.getTemp());
                insertStmt.execute();
            }
        }

        @Override
        public void close() throws Exception {
            // 清理工作
            insertStmt.close();
            updateStmt.close();
            connection.close();
        }
    }
}
