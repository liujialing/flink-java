package com.example.apitest.transform;

import com.example.com.example.apitest.bean.SensorSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

public class Partition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream = env.readTextFile("/Users/jialing/project/flink-java/src/main/resources/sensor.txt");

        SingleOutputStreamOperator<SensorSource> mapStream = dataStream.map(new MapFunction<String, SensorSource>() {
            @Override
            public SensorSource map(String s) throws Exception {
                String[] array = s.split(",");
                return new SensorSource(array[0], Long.valueOf(array[1]), Double.valueOf(array[2]));
            }
        });

        mapStream.print("map");

        DataStream<SensorSource> shuffleStream = mapStream.shuffle();

        shuffleStream.print("shuffle");

        mapStream.keyBy("id").print("keyby");

        mapStream.global().print("global");

        env.execute();
    }
}
