package com.atguigu.chapter08;

import com.atguigu.bean.AdsClickLog;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/10 9:06
 */
public class Flink04_Project_High_BlackList {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(1);
        // 创建WatermarkStrategy
        WatermarkStrategy<AdsClickLog> wms = WatermarkStrategy
            .<AdsClickLog>forBoundedOutOfOrderness(Duration.ofSeconds(20))
            .withTimestampAssigner(new SerializableTimestampAssigner<AdsClickLog>() {
                @Override
                public long extractTimestamp(AdsClickLog element, long recordTimestamp) {
                    return element.getTimestamp() * 1000L;
                }
            });
    
        OutputTag<String> blackListOutputTag = new OutputTag<String>("blackList", Types.STRING) {};
    
        SingleOutputStreamOperator<String> result = env
//            .readTextFile("input/AdClickLog.csv")
            .socketTextStream("hadoop162", 9999)
            .map(line -> {
                String[] datas = line.split(",");
                return new AdsClickLog(Long.valueOf(datas[0]),
                                       Long.valueOf(datas[1]),
                                       datas[2],
                                       datas[3],
                                       Long.valueOf(datas[4]));
            })
            .assignTimestampsAndWatermarks(wms)
            // 计算每个用户对每个广告的点击量
            .keyBy(ads -> ads.getUserId() + "_" + ads.getAdsId())
            .process(new KeyedProcessFunction<String, AdsClickLog, String>() {
    
                private ValueState<Boolean> warnState;
                private ReducingState<Long> countState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    countState = getRuntimeContext()
                        .getReducingState(new ReducingStateDescriptor<Long>(
                            "state",
                            new ReduceFunction<Long>() {
                                @Override
                                public Long reduce(Long value1, Long value2) throws Exception {
                                    return value1 + value2;
                                }
                            },
                            Long.class
                        ));
    
                    warnState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("warnState", Boolean.class));
                    
                    
                }
                
                @Override
                public void processElement(AdsClickLog log,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    // 每个用户每个广告第一条数据, 注册定时器, 到明天的0:0:0去触发: 清空状态
                    // 如果countState的值是null, 则表示这是第一条
                    if (countState.get() == null) {
                        // 如何找到明天的0:0:0
                        long now = ctx.timestamp() / 1000;
                        LocalDateTime todayTime = LocalDateTime.ofEpochSecond(now, 0, ZoneOffset.ofHours(8));
                        LocalDateTime tomorrowTime = LocalDateTime.of(todayTime.toLocalDate().plusDays(1), LocalTime.of(0, 0, 0));
    
                        ctx.timerService().registerEventTimeTimer(tomorrowTime.toEpochSecond(ZoneOffset.ofHours(8)) * 1000);
                    }
                    
                    if (warnState.value() == null) {
                        countState.add(1L);
                        // 当某个用户对某个广告的点击量超过了99, 把这个用户放入黑名单
                        if (countState.get() > 99) {
                            String msg =
                                "用户: " +
                                    log.getUserId() +
                                    "对广告: " + log.getAdsId() + "" +
                                    " 的点击量是: " + countState.get();
                            ctx.output(blackListOutputTag, msg);
                            warnState.update(true);
                        }else{
                            String msg =
                                "用户: " +
                                    log.getUserId() +
                                    "对广告: " + log.getAdsId() + "" +
                                    " 的点击量是: " + countState.get();
                            out.collect(msg);
                        }
                    }
                }
    
                @Override
                public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                    System.out.println("onTimer....");
                    warnState.clear();
                    countState.clear();
                }
            });
        
        result.print("正常用户");
        
        result.getSideOutput(blackListOutputTag).print("blackList");
    
        env.execute();
        
    }
}
