package com.atguigu.chapter05.transform;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/1 15:01
 */
public class Flink06_Connect {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        
        // 1. connect是把两个流连成一个流, 两个流的数据类型可以不一样
        // 2. 连接不是真正的合成一个流, 内部是分别处理
        DataStreamSource<String> first = env.fromElements("a", "b", "c", "d");
        DataStreamSource<Integer> second = env.fromElements(1, 2, 10, 20, 30);
        
        ConnectedStreams<String, Integer> cs = first.connect(second);
        
        SingleOutputStreamOperator<String> result = cs
            .process(new CoProcessFunction<String, Integer, String>() {
                @Override
                public void processElement1(String value, Context ctx, Collector<String> out) throws Exception {
                    out.collect(value + " a");
                }
                
                @Override
                public void processElement2(Integer value, Context ctx, Collector<String> out) throws Exception {
                    out.collect(value + " b");
                }
            });
        result.print();
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
