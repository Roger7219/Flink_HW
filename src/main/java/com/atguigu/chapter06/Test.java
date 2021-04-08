package com.atguigu.chapter06;

import com.atguigu.bean.MarketingUserBehavior;
import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * ClassName Test
 * Description:
 * Create by Chenjiazhe
 * Date 2021/4/6 0006 上午 8:34
 */

/**
    当有用到用两个Key来进行分组时，可以用这种方法：
    1 将两个Key拼接成一个字符串 或 元组，再进行Keyby
    2 在运用process方法进行集中处理

 */

public class Test {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        env.addSource(new Flink02_Project_App_Stats_With_Channel.MarketSource())
                .keyBy(a-> a.getChannel() + "_" + a.getBehavior())
                .process(new KeyedProcessFunction<String, MarketingUserBehavior, Tuple2<String,Long>>() {
                    HashMap<String,Long> hm = new HashMap<>();
                    @Override
                    public void processElement(MarketingUserBehavior value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                        if (!hm.containsKey(ctx.getCurrentKey())){
                            hm.put(ctx.getCurrentKey(),0L);
                        }
                        hm.put(ctx.getCurrentKey(),hm.get(ctx.getCurrentKey())+1L);
                        out.collect(Tuple2.of(ctx.getCurrentKey(), hm.get(ctx.getCurrentKey())));

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
