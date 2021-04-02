package com.atguigu.chapter06;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/2 15:41
 */
public class Flink01_Project_UV {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(4);
        
        env
            .readTextFile("input/UserBehavior.csv")
            .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                @Override
                public void flatMap(String line,
                                    Collector<Tuple2<String, Long>> out) throws Exception {
                    String[] data = line.split(",");
                    UserBehavior ub = new UserBehavior(Long.valueOf(data[0]),
                                                       Long.valueOf(data[1]),
                                                       Integer.valueOf(data[2]),
                                                       data[3],
                                                       Long.valueOf(data[4]));
                    if ("pv".equalsIgnoreCase(ub.getBehavior())) {
                        out.collect(Tuple2.of("uv", ub.getUserId()));
                    }
                }
            })
            .keyBy(t -> t.f0)
            .process(new KeyedProcessFunction<String, Tuple2<String, Long>, Long>() {
                Set<Long> uids = new HashSet<>();
                
                @Override
                public void processElement(Tuple2<String, Long> value,
                                           Context ctx,
                                           Collector<Long> out) throws Exception {
                    /*int pre = uids.size();
                    uids.add(value.f1);
                    int post = uids.size();
                    if (post > pre) {
                        
                        out.collect((long) uids.size());
                    }*/
                    
                    if (uids.add(value.f1)) {
                        out.collect((long) uids.size());
                    }
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
