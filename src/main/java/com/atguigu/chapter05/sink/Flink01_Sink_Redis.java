package com.atguigu.chapter05.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.ArrayList;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/2 10:29
 */
public class Flink01_Sink_Redis {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));
       
       
        FlinkJedisPoolConfig redisConf = new FlinkJedisPoolConfig.Builder()
            .setHost("hadoop162")
            .setPort(6379)
            .setMaxTotal(100)
            .setMaxIdle(10)
            .setMinIdle(10)
            .setTimeout(10 * 1000)
            .build();
        
    
        env
            .fromCollection(waterSensors)
            .keyBy(WaterSensor::getId)
            .sum("vc")
            .addSink(new RedisSink<>(redisConf, new RedisMapper<WaterSensor>() {
                // 返回命令描述符 set hset sadd
                @Override
                public RedisCommandDescription getCommandDescription() {
                    // 如果不是hash和zset则第二个参数无效
                    return new RedisCommandDescription(RedisCommand.SET , "random");
                }
    
                // 从数据源中获取key  如果是hash, 则是内部的key
                @Override
                public String getKeyFromData(WaterSensor data) {
                    return data.getId();
                }
                
                // 返回要写入到redis的具体数据
                @Override
                public String getValueFromData(WaterSensor data) {
                    return JSON.toJSONString(data);
                }
            }));
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
/*
redis: string set list hash zset

string
key         value
sensor_1    {"":"", ...}


 */