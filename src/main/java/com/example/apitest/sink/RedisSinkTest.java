package com.example.apitest.sink;

import com.example.com.example.apitest.bean.SensorSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class RedisSinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream = env.readTextFile("/Users/jialing/project/flink-java/src/main/resources/sensor.txt");

        SingleOutputStreamOperator<SensorSource> mapStream = dataStream.map(new MapFunction<String, SensorSource>() {

            public SensorSource map(String s) throws Exception {
                String[] array = s.split(",");
                return new SensorSource(array[0], Long.valueOf(array[1]), Double.valueOf(array[2]));
            }
        });

        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost")
                .setPort(6379)
                .build();

        mapStream.addSink(new RedisSink<SensorSource>(config, new RedisMapper<SensorSource>() {
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET, "sensor_temp");
            }

            public String getKeyFromData(SensorSource sensorSource) {
                return sensorSource.getId();
            }

            public String getValueFromData(SensorSource sensorSource) {
                return String.valueOf(sensorSource.getTemperature());
            }
        }));

        env.execute();
    }
}
