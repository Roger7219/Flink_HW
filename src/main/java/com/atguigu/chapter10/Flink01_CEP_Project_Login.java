package com.atguigu.chapter10;

import com.atguigu.bean.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/12 9:02
 */
public class Flink01_CEP_Project_Login {
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
        KeyedStream<LoginEvent, Long> loginKS = env
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
        
        // 1. 定义模式  2s每连续两次失败
        Pattern<LoginEvent, LoginEvent> failPattern = Pattern
            .<LoginEvent>begin("fail")
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
            
        
        // 2. 把模式运用在流上
        PatternStream<LoginEvent> ps = CEP.pattern(loginKS, failPattern);
        
        // 3. 从模式流中取出匹配到的结果
        ps
            .select(new PatternSelectFunction<LoginEvent, String>() {
                @Override
                public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
                    return pattern.get("fail").toString();
                }
            })
            .print();
    
        env.execute();
        
    }
}
