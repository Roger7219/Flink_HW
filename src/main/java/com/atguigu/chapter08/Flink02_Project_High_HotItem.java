package com.atguigu.chapter08;

import com.atguigu.bean.HotItem;
import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/9 15:21
 */
public class Flink02_Project_High_HotItem {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
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
            .keyBy(UserBehavior::getItemId)
            .window(SlidingEventTimeWindows.of(Time.hours(2), Time.minutes(30)))
            .aggregate(
                new AggregateFunction<UserBehavior, Long, Long>() {
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
                    public Long merge(Long acc1, Long acc2) {
                        return acc1 + acc2;
                    }
                },
                new ProcessWindowFunction<Long, HotItem, Long, TimeWindow>() {
                    @Override
                    public void process(Long key,
                                        Context ctx,
                                        Iterable<Long> elements,
                                        Collector<HotItem> out) throws Exception {
                        out.collect(new HotItem(key, elements.iterator().next(), ctx.window().getEnd()));
                    }
                }
            )
            .keyBy(HotItem::getWindowEndTime)
            .process(new KeyedProcessFunction<Long, HotItem, String>() {
                
                private ValueState<Long> timerState;
                private ListState<HotItem> hotItemState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    hotItemState = getRuntimeContext()
                        .getListState(new ListStateDescriptor<HotItem>("hotItemState", HotItem.class));
                    
                    // 单独定义一个状态
                    timerState = getRuntimeContext()
                        .getState(new ValueStateDescriptor<Long>("timerState", Long.class));
                }
                
                @Override
                public void processElement(HotItem ele,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    // 同一个窗口内的数据, 他们是一个一个来的, 需要等到来齐才可以进行排序取topN
                    // 每个都存储到一个集合中: flink的提供的管理状态(ListState)
                    hotItemState.add(ele);
                    
                    // 当第一个进来的时候, 定义一个定时器: 触发时间 ele.getWindowEndTime() + 10s
                    //引入定时器状态是为了找出第一个进来的数据
                    if (timerState.value() == null) {
                        long timerTime = ele.getWindowEndTime() + 10000L;
                        ctx.timerService().registerEventTimeTimer(timerTime);
                        timerState.update(timerTime);
                    }
                    
                }
                
                @Override
                public void onTimer(long timestamp,
                                    OnTimerContext ctx,
                                    Collector<String> out) throws Exception {
                    // 排序取topN
                    ArrayList<HotItem> hotItems = new ArrayList<>();
                    for (HotItem hotItem : hotItemState.get()) {
                        hotItems.add(hotItem);
                    }
                    
                    // 排序
                    hotItems.sort((o1, o2) -> o2.getCount().compareTo(o1.getCount()));
                    
                    StringBuilder sb = new StringBuilder();
                    sb.append("-----------------\n");
                    sb.append("窗口结束时间: ").append(timestamp - 10000L);
                    for (int i = 0, count = Math.min(3, hotItems.size()); i < count; i++) {
                        sb.append(hotItems.get(i)).append("\n");
                    }
                    
                    out.collect(sb.toString());
                    
                    // 等这个排序完成之后, 清空状态, 可以有助于与内存
                    hotItemState.clear();
                    timerState.clear();
                    
                }
            })
            .print();
        
        env.execute();
    }
}
/*
每隔5分钟输出最近1小时内点击量(pv)最多的前N个商品  热门商品

1. 使用滑动窗口: 长度 1h  步长 5m
2. 按照商品id进行分组, 计算每个商品的点击量
3. 其实是每个窗口内每个商品的点击量
4. 如果要计算topN, 所有的数据应该进入同一个通道中
    如果做?
        重新keyBy: 按照窗口的开始或者结束

    然后排序取topN
        应该得到[0,1h)这个窗口的所有商品都到了之后再进行排序和去topN
        
        使用定时器 : 窗口的结束时间+1m

*/