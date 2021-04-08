package com.atguigu.chapter07;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/6 11:23
 */
public class Flink10_Window_Aggregate {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        
        env
            .socketTextStream("hadoop162", 9999)
            .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                @Override
                public void flatMap(String line,
                                    Collector<Tuple2<String, Long>> out) throws Exception {
                    for (String word : line.split(" ")) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                }
            })
            .keyBy(t -> t.f0)
            .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            //针对的是一次流（读取的一次数据）
                .aggregate(
                new AggregateFunction<Tuple2<String, Long>, Long, Long>() {
                    // 针对每个key的每个窗口: 第一个元素进来的时候触发
                    @Override
                    public Long createAccumulator() {
                        System.out.println("createAccumulator...");
                        return 0L;
                    }
    
                    // 针对每个Key, 每进来一个元素, 触发一次
                    @Override
                    public Long add(Tuple2<String, Long> value, Long accumulator) {
                        System.out.println("add...");
                        return accumulator + 1;
                    }
    
                    // 返回最终的结果: 窗口关闭的时候, 调用这个方法, 返回值, 发送到下游
                    @Override
                    public Long getResult(Long accumulator) {
                        System.out.println("getResult...");
                        return accumulator;
                    }
    
                    // 合并两个类计算器的值, 一般不需要实现: 只有在session window才会调用
                    @Override
                    public Long merge(Long acc1, Long acc2) {
                        System.out.println("merge...");
                        return acc1 + acc2;
                    }
                },
                new ProcessWindowFunction<Long, String, String, TimeWindow>() {
                    @Override
                    public void process(String s,
                                        Context context,
                                        Iterable<Long> elements,
                                        Collector<String> out) throws Exception {
                        out.collect(s + "   " + elements.iterator().next());
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

