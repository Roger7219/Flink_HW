package com.atguigu.chapter05.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/1 15:01
 */
public class Flink02_FlatMap {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        
        DataStreamSource<Integer> stream = env.fromElements(1, 2, 3, 4);
        // 流中包含平方和三次方
        // flatMap: 1->N 可以替换掉map和filter
        stream
            .flatMap(new FlatMapFunction<Integer, Integer>() {
                @Override
                public void flatMap(Integer value, Collector<Integer> out) throws Exception {
//                    out.collect(value * value);
//                    out.collect(value * value * value);
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
