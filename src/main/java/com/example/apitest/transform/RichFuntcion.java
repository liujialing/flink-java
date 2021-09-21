package com.example.apitest.transform;

import com.example.com.example.apitest.bean.SensorSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.security.auth.login.AccountLockedException;

public class RichFuntcion {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream = env.readTextFile("/Users/jialing/project/flink-java/src/main/resources/sensor.txt");

        DataStream<SensorSource> mapStream = dataStream.map(new MapFunction<String, SensorSource>() {
            @Override
            public SensorSource map(String s) throws Exception {
                String[] array = s.split(",");
                return new SensorSource(array[0], Long.valueOf(array[1]), Double.valueOf(array[2]));
            }
        });

        DataStream<Tuple2<String, Integer>> riskMapStream = mapStream.map(new MyMapper());

        riskMapStream.print();

        env.execute();
    }

    public static class MyMapper extends RichMapFunction<SensorSource, Tuple2<String, Integer>>  {

        @Override
        public Tuple2<String, Integer> map(SensorSource sensorSource) throws Exception {
            return new Tuple2<>(sensorSource.getId(), getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open");
        }
    }
}
