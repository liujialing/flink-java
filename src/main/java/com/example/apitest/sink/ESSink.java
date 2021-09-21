package com.example.apitest.sink;

import com.example.com.example.apitest.bean.SensorSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

public class ESSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream = env.readTextFile("/Users/jialing/project/flink-java/src/main/resources/sensor.txt");

        SingleOutputStreamOperator<SensorSource> mapStream = dataStream.map(new MapFunction<String, SensorSource>() {

            public SensorSource map(String s) throws Exception {
                String[] array = s.split(",");
                return new SensorSource(array[0], Long.valueOf(array[1]), Double.valueOf(array[2]));
            }
        });

        ArrayList<HttpHost> httpHosts = new ArrayList<HttpHost>();
        httpHosts.add(new HttpHost("localhost", 9200));

        mapStream.addSink(new ElasticsearchSink.Builder<SensorSource>(httpHosts, new MyEsSinkFunction()).build());

        env.execute();
    }

    private static class MyEsSinkFunction implements ElasticsearchSinkFunction<SensorSource> {
        public void process(SensorSource sensorSource, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
            HashMap<String, String> dataSource = new HashMap<String, String>();
            dataSource.put("id", sensorSource.getId());
            dataSource.put("temp", String.valueOf(sensorSource.getTemperature()));
            dataSource.put("ts", String.valueOf(sensorSource.getTimestamp()));

            IndexRequest indexRequest = Requests.indexRequest()
                    .index("sensor")
                    .type("readingdata")
                    .source(dataSource);

            requestIndexer.add(indexRequest);
        }
    }
}
