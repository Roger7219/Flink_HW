package com.atguigu.chapter06;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/2 15:41
 */
public class Flink01_Project_PV_2 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(4);
        
        // 1. 读数据
        
        // 2. 对数进行处理
        // 2.1. 封装pojo
        // 2.2 去重
        // 3.3 map成 (pv, 1)
        // 3.4 keyBy
        // 3.5 集合
    
        env
            .readTextFile("input/UserBehavior.csv")
            .map(line -> {
                String[] data = line.split(",");
                return new UserBehavior(Long.valueOf(data[0]),
                                        Long.valueOf(data[1]),
                                        Integer.valueOf(data[2]),
                                        data[3],
                                        Long.valueOf(data[4]));
            })
            .keyBy(UserBehavior::getBehavior)
            .process(new KeyedProcessFunction<String, UserBehavior, Long>() {
                long sum = 0;
                @Override
                public void processElement(UserBehavior value,
                                           Context ctx,
                                           Collector<Long> out) throws Exception {
                    if ("pv".equalsIgnoreCase(ctx.getCurrentKey())) {
                        sum ++;
                        out.collect(sum);
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
