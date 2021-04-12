package com.atguigu.chapter08;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
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
 * ClassName Flink01_Test
 * Description:
 * Create by Chenjiazhe
 * Date 2021/4/11 0011 下午 3:02
 */
public class Flink01_Test {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.readTextFile("input/UserBehavior.csv")
                .map(x -> {
                    String[] data = x.split(",");
                    return new UserBehavior(Long.valueOf(data[0]), Long.valueOf(data[1]), Integer.valueOf(data[2]), data[3], Long.valueOf(data[4]));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                return element.getTimestamp() * 1000;
                            }
                        })
                )
                .filter(behavior -> "pv".equalsIgnoreCase(behavior.getBehavior()))
                .keyBy(UserBehavior::getBehavior)
                .window(TumblingEventTimeWindows.of(Time.minutes(10)))
                .process(new ProcessWindowFunction<UserBehavior, String, String, TimeWindow>() {

                    private MapState<Long, Object> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Object>("mapState", Long.class, Object.class));
                    }

                    @Override
                    public void process(String s, Context context, Iterable<UserBehavior> elements, Collector<String> out) throws Exception {
                        mapState.clear();
                        for (UserBehavior ele : elements) {
                            mapState.put(ele.getUserId(), new Object());
                        }
                        long sum = 0;
                        for (Long id : mapState.keys()) {
                            sum++;
                        }
                        String msg = "w_start=" + context.window().getStart()
                                + ", w_end=" + context.window().getEnd()
                                + " 的uv是: " + sum;
                        out.collect(msg);

                    }
                }).print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
