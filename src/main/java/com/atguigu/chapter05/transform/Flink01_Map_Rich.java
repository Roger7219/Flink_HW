package com.atguigu.chapter05.transform;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/1 15:01
 */
public class Flink01_Map_Rich {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        
        DataStreamSource<Integer> stream = env.fromElements(1, 2, 3, 1,1,1,1,11);
        stream
            .map(new RichMapFunction<Integer, Integer>() {
                @Override
                public void open(Configuration parameters) throws Exception {
                    // 与外部存储建立连接
                    System.out.println("open ...");
                }
    
                @Override
                public Integer map(Integer value) throws Exception {
                    return value * value;
                }
    
                @Override
                public void close() throws Exception {
                    System.out.println("close ...");
                }
            })
            .print();
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
}
