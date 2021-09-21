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

public class MultipleStream {
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

        SplitStream splitStream = mapStream.split(new OutputSelector<SensorSource>() {
              @Override
              public Iterable<String> select(SensorSource sensorSource) {
                  return sensorSource.getTemperature() > 36 ? Collections.singletonList("high") : Collections.singletonList("low");
              }
        });

        DataStream highstream = splitStream.select("high");
        DataStream lowStream = splitStream.select("low");
        DataStream allStream = splitStream.select("low", "high");

        highstream.print();

        DataStream highTempWarning = highstream.map(new MapFunction<SensorSource, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorSource o) throws Exception {
                return new Tuple2<>(o.getId(), o.getTemperature());
            }
        });

        ConnectedStreams connectStream = highTempWarning.connect(lowStream);

        DataStream coMapStream = connectStream.map(new CoMapFunction<Tuple2<String, Double>, SensorSource, Object>() {

            @Override
            public Object map1(Tuple2<String, Double> value) throws Exception {
                return new Tuple3(value.f0, value.f1, "hot warning");
            }

            @Override
            public Object map2(SensorSource sensorSource) throws Exception {
                return new Tuple2<>(sensorSource.getId(), "normal");
            }
        });

        coMapStream.print();

        DataStream unionStream = highstream.union(lowStream, allStream);
        unionStream.print();

        env.execute();
    }
}
