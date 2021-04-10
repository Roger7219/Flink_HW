package com.atguigu.chapter08;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/9 15:21
 */
public class Flink01_Project_High_UV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env
            .readTextFile("input/UserBehavior.csv")
            .map(line -> { // 对数据切割, 然后封装到POJO中
                String[] split = line.split(",");
                return new UserBehavior(Long.valueOf(split[0]), Long.valueOf(split[1]), Integer.valueOf(split[2]), split[3], Long.valueOf(split[4]));
            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                        @Override
                        public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                            return element.getTimestamp() * 1000;
                        }
                    })
            )
            .filter(behavior -> "pv".equals(behavior.getBehavior()))
            .keyBy(UserBehavior::getBehavior)
            .window(TumblingEventTimeWindows.of(Time.minutes(10)))
            .process(new ProcessWindowFunction<UserBehavior, String, String, TimeWindow>() {
                
                private MapState<Long, Object> userIdState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    userIdState = getRuntimeContext()
                        .getMapState(new MapStateDescriptor<Long, Object>("userIdState", Long.class, Object.class));
                }
                
                @Override
                public void process(String key,
                                    Context ctx,
                                    Iterable<UserBehavior> elements,
                                    Collector<String> out) throws Exception {
                    // 计算每小时的uv
                    
                    // 0. 计算每个窗口的uv的时候, 应该先清空状态. 因为同一个key开说,所有的窗口共用同一个状态
                    userIdState.clear();
                    // 1. 把这个窗口内的uid存入到状态中
                    for (UserBehavior ub : elements) {
                        userIdState.put(ub.getUserId(), new Object());
                    }
                    
                    // 2. 计算窗口的uv, 有多少个key他的uv就是多少
                    long sum = 0L;
                    for (Long uid : userIdState.keys()) {
                        sum++;
                    }
                    String msg = "w_start=" + ctx.window().getStart()
                        + ", w_end=" + ctx.window().getEnd()
                        + " 的uv是: " + sum;
                    
                    out.collect(msg);
                    
                }
            })
            .print();
    
        env.execute();
    }
}
