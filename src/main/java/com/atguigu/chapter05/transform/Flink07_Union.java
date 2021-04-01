package com.atguigu.chapter05.transform;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/1 15:01
 */
public class Flink07_Union {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        
       
        DataStreamSource<String> first = env.fromElements("a", "b", "c", "d");
        DataStreamSource<String> second = env.fromElements("1", "2", "10", "20", "30");
        /*
        union:
            1. 流的数据类型必须一致
            2. 可以把多个流合成一个流
         */
        DataStream<String> uds = first.union(second);
        
        uds.print();
    
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
