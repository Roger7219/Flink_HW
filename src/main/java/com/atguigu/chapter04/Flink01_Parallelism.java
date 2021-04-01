package com.atguigu.chapter04;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/1 10:16
 */
public class Flink01_Parallelism {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        env.disableOperatorChaining();
        
        env
            .socketTextStream("hadoop162", 9999) // socket的并行度只能是1
            .flatMap(new FlatMapFunction<String, String>() {
                @Override
                public void flatMap(String value, Collector<String> out) throws Exception {
                    for (String word : value.split(" ")) {
                        out.collect(word);
                    }
                }
            })
            .map(new MapFunction<String, Tuple2<String, Long>>() {
                @Override
                public Tuple2<String, Long> map(String value) throws Exception {
                    return Tuple2.of(value, 1L);
                }
            })
            .filter(new FilterFunction<Tuple2<String, Long>>() {
                @Override
                public boolean filter(Tuple2<String, Long> value) throws Exception {
                    return true;
                }
            })
            .keyBy(value -> value.f0)
            .sum(1)
            .print();
        
    
        env.execute();
    }
}
/*
有四个地方设置算子的并行度:
1. 配置文件
2. 提交应用的通过参数
3. 在env中设置
4. 在每个算子上设置

操作链的优化:
    1. 算子是one-to-one
    2. 并行度必须一样

 .startNewChain() 开启一个新的操作, 不和前面优化在一起
 .disableChaining() 对这个算子禁用操作连: 前后都不会优化在一起
 
 env.disableOperatorChaining(); 在整个 job禁用操作链
 */