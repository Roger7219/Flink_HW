package com.atguigu.chapter07;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Date;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/6 11:23
 */
public class Flink01_Window_Tumbling {
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
                //每一个key对应一个窗口
            .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                @Override
                public void process(String key,  // 窗口属于的key的值
                                    Context ctx, // 上下文对象: 获取到窗口的开始时间和结束时间
                                    Iterable<Tuple2<String, Long>> elements,  // 这个窗口的所有数据
                                    Collector<String> out) throws Exception {
                    Date start = new Date(ctx.window().getStart());
                    Date end = new Date(ctx.window().getEnd());
    
                    ArrayList<String> strings = new ArrayList<>();
                    for (Tuple2<String, Long> ele : elements) {
                        strings.add(ele.f0);
                    }
                    out.collect("key=" + key + ", w_start=" + start + ", w_end=" + end + ", data=" + strings);
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

