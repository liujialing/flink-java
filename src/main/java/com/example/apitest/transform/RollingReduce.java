package com.example.apitest.transform;

import com.example.com.example.apitest.bean.SensorSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RollingReduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> inputSource = env.readTextFile("/Users/jialing/project/flink-java/src/main/resources/sensor.txt");

        KeyedStream<SensorSource, Tuple> keyedStream = inputSource.map(line -> {
            String[] fields = line.split(",");
            return new SensorSource(fields[0], new Long(fields[1]), new Double(fields[2]));
        }).keyBy("id");

        DataStream<SensorSource> reduceData = keyedStream.reduce((oldVal, newVal) -> {
            return new SensorSource(oldVal.getId(), newVal.getTimestamp(),
                    Math.max(oldVal.getTemperature(), newVal.getTemperature()));
        });

        reduceData.print();

        env.execute();

    }
}
