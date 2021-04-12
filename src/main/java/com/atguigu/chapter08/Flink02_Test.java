package com.atguigu.chapter08;

import com.atguigu.bean.HotItem;
import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * ClassName Flink01_Test
 * Description:
 * Create by Chenjiazhe
 * Date 2021/4/11 0011 下午 3:02
 */
public class Flink02_Test {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.readTextFile("input/UserBehavior.csv")
                .map(x -> {
                    String[] data = x.split(",");
                    return new UserBehavior(Long.valueOf(data[0]), Long.valueOf(data[1]), Integer.valueOf(data[2]), data[3], Long.valueOf(data[4]));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        }))
                .filter(behavior->"pv".equalsIgnoreCase(behavior.getBehavior()))
                .keyBy(UserBehavior::getItemId)
                .window(SlidingEventTimeWindows.of(Time.hours(2),Time.minutes(30)))
                .aggregate(new AggregateFunction<UserBehavior, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(UserBehavior value, Long accumulator) {
                        return accumulator + 1L;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return null;
                    }
                }
                ,new ProcessWindowFunction<Long, HotItem,Long,TimeWindow>(){
                            @Override
                            public void process(Long aLong, Context context, Iterable<Long> elements, Collector<HotItem> out) throws Exception {
                                out.collect(new HotItem(aLong,elements.iterator().next(),context.window().getEnd()));
                            }
                        })
                .keyBy(HotItem::getWindowEndTime)
                .process(new KeyedProcessFunction<Long, HotItem, String>() {

                    private ValueState<Long> timerState;
                    private ListState<HotItem> hotId;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hotId = getRuntimeContext().getListState(new ListStateDescriptor<HotItem>("HotId", HotItem.class));
                        timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerState", Long.class));

                    }

                    @Override
                    public void processElement(HotItem value, Context ctx, Collector<String> out) throws Exception {
                        hotId.add(value);
                        if (timerState.value() == null) {
                            long timerTime = value.getWindowEndTime() + 10000L;

                            ctx.timerService().registerEventTimeTimer(timerTime);

                            timerState.update(timerTime);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        ArrayList<HotItem> hotItems = new ArrayList<>();
                        for (HotItem hotItem : hotId.get()) {

                            hotItems.add(hotItem);
                        }
                        hotItems.sort((o1, o2) -> o2.getCount().compareTo(o1.getCount()));

                        StringBuilder sb = new StringBuilder();
                        sb.append("-----------------\n");
                        sb.append("窗口结束时间: ").append(timestamp - 10000L);
                        for (int i = 0, count = Math.min(3, hotItems.size()); i < count; i++) {
                            sb.append(hotItems.get(i)).append("\n");
                        }
                        out.collect(sb.toString());

                        hotId.clear();
                        timerState.clear();
                    }
                }).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
