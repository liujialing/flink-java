package com.example.apitest.source;

import com.example.com.example.apitest.bean.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class UDF {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<SensorSource> dataSource = env.addSource(new SourceFunction<SensorSource>() {
            private Boolean running = true;
            public void run(SourceContext<SensorSource> sourceContext) throws Exception {
                final Random random = new Random();
                Map<String, Double> tempMap = new HashMap();
                for (int i = 1; i < 11; i++) {
                    String sensorId = "sensor_" + i;
                    tempMap.put(sensorId, random.nextGaussian() * 0.2 + 36);
                }
                while (running) {
                    for (Map.Entry<String, Double> ele : tempMap.entrySet()) {
                        Double temp = ele.getValue() + random.nextGaussian() * 0.2;
                        tempMap.put(ele.getKey(), temp);
                        sourceContext.collect(new SensorSource(ele.getKey(), System.currentTimeMillis(), temp));
                    }

                }
            }

            public void cancel() {
                running = false;
            }
        });

        dataSource.print();

        env.execute();
    }
}
