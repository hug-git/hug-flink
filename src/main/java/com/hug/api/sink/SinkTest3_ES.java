package com.hug.api.sink;

import com.hug.api.bean.SensorReading;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @ClassName: SinkTest_ES
 * @Description:
 * @Author: hug on 2020/10/24 10:55 (星期六)
 * @Version: 1.0
 */
public class SinkTest3_ES {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> dataStream = env.readTextFile("first/hello.txt");

        SingleOutputStreamOperator<SensorReading> sensorStream = dataStream.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });

        // es配置
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102",9200));

        sensorStream.addSink(new ElasticsearchSink.Builder<SensorReading>(httpHosts,new MyEsSinkFun()).build());

        env.execute();
    }

    public static class MyEsSinkFun implements ElasticsearchSinkFunction<SensorReading> {
        @Override
        public void process(SensorReading sensorReading, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
            // 将要写入es的数据包装成Map或JsonObject
            HashMap<String , String> dataSource = new HashMap<>();
            dataSource.put("id", sensorReading.getId());
            dataSource.put("temp", sensorReading.getTemp() + "");
            dataSource.put("ts", sensorReading.getTs().toString());

            // 创建indexRequest
            IndexRequest indexRequest = Requests.indexRequest()
                    .index("sensor")
                    .type("_doc")
                    .source(dataSource);

            // 发送http请求
            requestIndexer.add(indexRequest);
        }
    }
}
