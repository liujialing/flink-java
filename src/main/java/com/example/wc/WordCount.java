package com.example.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> dataSet = env.readTextFile("/Users/jialing/project/flink-java/src/main/resources/wordcount.txt");
        DataSet<Tuple2<String, Integer>> resultSet = dataSet.flatMap(new MyFlatMapper()).groupBy(0).sum(1);
        resultSet.print();
    }

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {

        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = s.split(" ");
            for (String ele : words) {
                collector.collect(new Tuple2<String, Integer>(ele, 1));
            }
        }
    }
}
