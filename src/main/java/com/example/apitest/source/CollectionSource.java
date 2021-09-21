package com.example.apitest.source;

import com.example.com.example.apitest.bean.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class CollectionSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<SensorSource> dataSource = env.fromCollection(Arrays.asList(
                new SensorSource("source 1", 1345678990L, 35.9),
                new SensorSource("source 2", 1345678991L, 36.5),
                new SensorSource("source 3", 1345678992L, 35.8)
        ));

        dataSource.print();

        env.execute();

    }
}
