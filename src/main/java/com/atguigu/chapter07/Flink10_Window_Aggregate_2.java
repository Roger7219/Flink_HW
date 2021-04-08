package com.atguigu.chapter07;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/6 11:23
 */
public class Flink10_Window_Aggregate_2 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        
        env
            .fromElements(Tuple2.of("a", 5L), Tuple2.of("a", 6L), Tuple2.of("a", 4L))
            .keyBy(t -> t.f0)
            .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            .aggregate(new AggregateFunction<Tuple2<String, Long>, Tuple2<Long, Long>, Double>() {
                @Override
                public Tuple2<Long, Long> createAccumulator() {
                    return Tuple2.of(0L, 0L);
                }
                
                @Override
                public Tuple2<Long, Long> add(Tuple2<String, Long> value, Tuple2<Long, Long> acc) { //acc :(和, 个数)
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    return Tuple2.of(acc.f0 + value.f1, acc.f1 + 1L);
                }
                
                @Override
                public Double getResult(Tuple2<Long, Long> acc) {
                    return acc.f0 * 1.0 / acc.f1;
                }
                
                @Override
                public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                    return null;
                }
            }
            )
            .print();
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

