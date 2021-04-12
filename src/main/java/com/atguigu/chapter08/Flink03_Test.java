package com.atguigu.chapter08;

import com.atguigu.bean.AdsClickLog;
import com.atguigu.bean.HotItem;
import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;

/**
 * ClassName Flink01_Test
 * Description:
 * Create by Chenjiazhe
 * Date 2021/4/11 0011 下午 3:02
 */
public class Flink03_Test {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        OutputTag<String> blackListOutputTag = new OutputTag<String>("blackList", Types.STRING) {
        };

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
                .assignTimestampsAndWatermarks(WatermarkStrategy.<AdsClickLog>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<AdsClickLog>() {
                            @Override
                            public long extractTimestamp(AdsClickLog element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        }))
                .keyBy(x -> x.getUserId() + "_" + x.getAdsId())
                .process(new KeyedProcessFunction<String, AdsClickLog, String>() {

                    private ValueState<Boolean> warnState;
                    private ReducingState<Long> reduceState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        this.reduceState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Long>("reduceState", new ReduceFunction<Long>() {
                            @Override
                            public Long reduce(Long value1, Long value2) throws Exception {
                                return value1 + value2;
                            }
                        }, Long.class));
                        ReducingState<Long> reduceState = this.reduceState;

                        warnState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("warnState", Boolean.class));
                    }

                    @Override
                    public void processElement(AdsClickLog value, Context ctx, Collector<String> out) throws Exception {
                        if (reduceState.get() == null) {
                            long now = ctx.timestamp() + 1000;
                            LocalDateTime todayTime = LocalDateTime.ofEpochSecond(now, 0, ZoneOffset.ofHours(8));
                            LocalDateTime torrowTime = LocalDateTime.of(todayTime.toLocalDate().plusDays(1), LocalTime.of(0, 0, 0));
                            ctx.timerService().registerEventTimeTimer(torrowTime.toEpochSecond(ZoneOffset.ofHours(8)) * 1000);
                        }
                        if (warnState.value() == null) {
                            reduceState.add(1L);
                            if (reduceState.get() > 99) {
                                String msg =
                                        "用户: " +
                                                value.getUserId() +
                                                "对广告: " + value.getAdsId() + "" +
                                                " 的点击量是: " + reduceState.get();
                                ctx.output(blackListOutputTag, msg);
                            } else {
                                String msg =
                                        "用户: " +
                                                value.getUserId() +
                                                "对广告: " + value.getAdsId() + "" +
                                                " 的点击量是: " + reduceState.get();
                                out.collect(msg);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        warnState.clear();
                        reduceState.clear();
                    }
                });
        result.print("正常用户");

        result.getSideOutput(blackListOutputTag).print("blackList");

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
