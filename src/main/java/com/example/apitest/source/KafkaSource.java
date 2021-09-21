package com.example.apitest.source;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class KafkaSource {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");

        DataStreamSource<String> dataSource = env.addSource(new FlinkKafkaConsumer011<String>("sensor", new SimpleStringSchema(), properties));

        dataSource.print();

        env.execute();
    }
}
