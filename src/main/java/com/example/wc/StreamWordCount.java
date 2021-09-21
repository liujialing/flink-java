package com.example.wc;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(4);

        // DataStream<String> dataStream = env.readTextFile("/Users/jialing/project/flink-java/src/main/resources/wordcount.txt");

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        DataStream<String> dataStream = env.socketTextStream(host, port);

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = dataStream.flatMap(new WordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1);
        result.print();

        env.execute();
    }
}
