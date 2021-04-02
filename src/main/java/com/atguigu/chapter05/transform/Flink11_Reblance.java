package com.atguigu.chapter05.transform;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/2 9:45
 */
public class Flink11_Reblance {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        env
            .fromElements(10,20,30,40,50,60,70,80)
            .keyBy(x -> x % 2)
            .map(x -> x)
            .rescale()
            .print();
       
       
        env.execute();
    }
}
