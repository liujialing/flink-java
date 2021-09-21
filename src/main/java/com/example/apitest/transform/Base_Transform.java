package com.example.apitest.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Base_Transform {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> inputSource = env.readTextFile("/Users/jialing/project/flink-java/src/main/resources/sensor.txt");

        SingleOutputStreamOperator<Integer> mapSource = inputSource.map(new MapFunction<String, Integer>() {
            public Integer map(String s) throws Exception {
                return s.length();
            }
        });

        SingleOutputStreamOperator<String> flatData = inputSource.flatMap(new FlatMapFunction<String, String>() {
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] stringArr = s.split(",");
                for (String ele : stringArr) {
                    collector.collect(ele);
                }
            }
        });

        SingleOutputStreamOperator<String> filterData = inputSource.filter(new FilterFunction<String>() {
            public boolean filter(String s) throws Exception {
                return s.startsWith("source 1");
            }
        });

        mapSource.print();
        flatData.print();
        filterData.print();

        env.execute();
    }
}
