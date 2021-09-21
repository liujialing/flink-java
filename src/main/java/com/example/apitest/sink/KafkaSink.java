package com.example.apitest.sink;

import com.example.com.example.apitest.bean.SensorSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Collections;

public class KafkaSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream = env.readTextFile("/Users/jialing/project/flink-java/src/main/resources/sensor.txt");

        SingleOutputStreamOperator<String> mapStream = dataStream.map(new MapFunction<String, String>() {

            public String map(String s) throws Exception {
                String[] array = s.split(",");
                return new SensorSource(array[0], Long.valueOf(array[1]), Double.valueOf(array[2])).toString();
            }
        });

        mapStream.addSink(new FlinkKafkaProducer011<String>("localhost:9092", "sinktest", new SimpleStringSchema()));

        env.execute();
    }
}
