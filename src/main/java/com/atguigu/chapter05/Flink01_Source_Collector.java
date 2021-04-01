package com.atguigu.chapter05;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/1 11:36
 */
public class Flink01_Source_Collector {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
    
//        List<Integer> list = Arrays.asList(1, 10, 20, 30, 40, 50);
    
//        DataStreamSource<Integer> stream = env.fromCollection(list);
        DataStreamSource<Integer> stream = env.fromElements(1, 2, 3, 4, 50, 60, 70);
    
        stream.print();
    
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
