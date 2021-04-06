package com.atguigu.chapter06;

import com.atguigu.bean.MarketingUserBehavior;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Random;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/6 9:28
 */
public class Flink02_Project_App_Stats_With_Channel {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
    
        /*env
            .addSource(new MarketSource())
            .map(mub -> Tuple2.of(mub.getChannel() + "_" + mub.getBehavior(), 1L))
            .returns(Types.TUPLE(Types.STRING, Types.LONG))
            .keyBy(f -> f.f0)
            .sum(1)
            .print();
            */
        
        env
            .addSource(new MarketSource())
            
            .keyBy(mub -> mub.getChannel() + "_" + mub.getBehavior())
            .process(new KeyedProcessFunction<String, MarketingUserBehavior, Tuple2<String, Long>>() {
                HashMap<String, Long> map = new HashMap<>();
                
                @Override
                public void processElement(MarketingUserBehavior value,
                                           Context ctx,
                                           Collector<Tuple2<String, Long>> out) throws Exception {
                    if (!map.containsKey(ctx.getCurrentKey())) {
                        map.put(ctx.getCurrentKey(), 0L);
                    }
    
                    map.put(ctx.getCurrentKey(), map.get(ctx.getCurrentKey()) + 1L);
    
                    out.collect(Tuple2.of(ctx.getCurrentKey(), map.get(ctx.getCurrentKey())));
                }
            })
            .print();
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static class MarketSource implements SourceFunction<MarketingUserBehavior> {
        
        // 生成数据
        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            Random random = new Random();
            String[] behaviors = {"install", "update", "uninstall", "download"};
            String[] channels = {"appstore", "huawei", "xiaomi", "oppo", "vivo"};
            while (true) {
                MarketingUserBehavior data = new MarketingUserBehavior((long) random.nextInt(2000),
                                                                       behaviors[random.nextInt(behaviors.length)],
                                                                       channels[random.nextInt(channels.length)],
                                                                       System.currentTimeMillis());
                ctx.collect(data);
                Thread.sleep(1500);
                
            }
        }
        
        // 调用这个方法, 可以停止source
        @Override
        public void cancel() {
        
        }
    }
}
