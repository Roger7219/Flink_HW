package com.atguigu.chapter08;

import com.atguigu.bean.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/10 10:30
 */
public class Flink05_Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 创建WatermarkStrategy
        WatermarkStrategy<LoginEvent> wms = WatermarkStrategy
                .<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                    @Override
                    public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                        return element.getEventTime();
                    }
                });
        KeyedStream<LoginEvent, Long> loginEventLongKeyedStream = env
                .readTextFile("input/LoginLog.csv")
                .map(line -> {
                    String[] data = line.split(",");
                    return new LoginEvent(Long.valueOf(data[0]),
                            data[1],
                            data[2],
                            Long.parseLong(data[3]) * 1000L);
                })
                .assignTimestampsAndWatermarks(wms)
                .keyBy(LoginEvent::getUserId);
        Pattern<LoginEvent, LoginEvent> fail = Pattern.<LoginEvent>begin("fail")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equalsIgnoreCase(value.getEventType());
                    }
                })
                .timesOrMore(2).consecutive()
                .until(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "success".equalsIgnoreCase(value.getEventType());
                    }
                })
                .within(Time.seconds(2));

        PatternStream<LoginEvent> patternStream = CEP.pattern(loginEventLongKeyedStream, fail);

        SingleOutputStreamOperator<String> select = patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> map) throws Exception {
                return map.get("fail").toString();
            }
        });

        select.print();

        env.execute();

    }
}
