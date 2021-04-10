package com.atguigu.chapter08;

import com.atguigu.bean.ApacheLog;
import com.atguigu.bean.PageCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
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

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.TreeSet;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/9 16:45
 */
public class Flink03_Project_High_HotPage {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        // 创建WatermarkStrategy
        WatermarkStrategy<ApacheLog> wms = WatermarkStrategy
            .<ApacheLog>forBoundedOutOfOrderness(Duration.ofSeconds(60))
            .withTimestampAssigner(new SerializableTimestampAssigner<ApacheLog>() {
                @Override
                public long extractTimestamp(ApacheLog element, long recordTimestamp) {
                    return element.getEventTime();
                }
            });
        
        env
            .readTextFile("input/apache.log")
            .map(line -> {
                String[] data = line.split(" ");
                SimpleDateFormat df = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                return new ApacheLog(data[0],
                                     df.parse(data[3]).getTime(),
                                     data[5],
                                     data[6]);
            })
            .assignTimestampsAndWatermarks(wms)
            .keyBy(ApacheLog::getUrl)
            .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.seconds(5)))
            .aggregate(new AggregateFunction<ApacheLog, Long, Long>() {
                @Override
                public Long createAccumulator() {
                    return 0L;
                }
                
                @Override
                public Long add(ApacheLog value, Long accumulator) {
                    return accumulator + 1L;
                }
                
                @Override
                public Long getResult(Long accumulator) {
                    return accumulator;
                }
                
                @Override
                public Long merge(Long a, Long b) {
                    return a + b;
                }
            }, new ProcessWindowFunction<Long, PageCount, String, TimeWindow>() { // <url, count, endWindow>
                @Override
                public void process(String key,
                                    Context context,
                                    Iterable<Long> elements,
                                    Collector<PageCount> out) throws Exception {
                    out.collect(new PageCount(key, elements.iterator().next(), context.window().getEnd()));
                }
            })
            .keyBy(PageCount::getWindowEnd)
            .process(new KeyedProcessFunction<Long, PageCount, String>() {
                
                private ValueState<TreeSet> pageState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    pageState = getRuntimeContext()
                        .getState(new ValueStateDescriptor<>("pageState", TreeSet.class));
                }
                
                @Override
                public void processElement(PageCount value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    // 找一个能够自动排序的集合: TreeSet 能自动排序, 还能去重
                    
                    // 第一个原来进来的时候创建一个TreeSet集合
                    if (pageState.value() == null) {
                        TreeSet<Object> pageCount = new TreeSet<>((o1, o2) -> {
                            PageCount pc1 = (PageCount) o1;
                            PageCount pc2 = (PageCount) o2;
                            int result = pc2.getCount().compareTo(pc1.getCount());
                            return result == 0 ? 1 : result;  // 不让返回0, 是为了必须treeSet去重
                        });
                        pageState.update(pageCount);
                        // 注册定时器
                        long timerTime = value.getWindowEnd() + 10000L;
                        ctx.timerService().registerEventTimeTimer(timerTime);
                    }
                    
                    TreeSet set = pageState.value();
                    set.add(value);
                    if (set.size() > 3) {
                        set.pollLast(); // 删除set集合的最后一个元素
                    }
                }
                
                @Override
                public void onTimer(long timestamp,
                                    OnTimerContext ctx,
                                    Collector<String> out) throws Exception {
                    TreeSet set = pageState.value();
                    
                    StringBuilder sb = new StringBuilder();
                    sb.append("-----------------\n");
                    sb.append("窗口结束时间: ").append(timestamp - 10000L).append("\n");
                    for (Object o : set) {
                        sb.append(o).append("\n");
                    }
                    
                    out.collect(sb.toString());
                }
            })
            .print();
        
        env.execute();
    }
}
