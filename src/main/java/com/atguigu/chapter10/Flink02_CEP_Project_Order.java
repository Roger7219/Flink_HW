package com.atguigu.chapter10;

import com.atguigu.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/12 9:02
 */
public class Flink02_CEP_Project_Order {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        
        WatermarkStrategy<OrderEvent> wms = WatermarkStrategy
            .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(20))
            .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                @Override
                public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                    return element.getEventTime();
                }
            });
        KeyedStream<OrderEvent, Long> orderKS = env
            .readTextFile("input/OrderLog.csv")
            .map(line -> {
                String[] datas = line.split(",");
                return new OrderEvent(
                    Long.valueOf(datas[0]),
                    datas[1],
                    datas[2],
                    Long.parseLong(datas[3]) * 1000);
            
            })
            .assignTimestampsAndWatermarks(wms)
            .keyBy(OrderEvent::getOrderId);
    
        Pattern<OrderEvent, OrderEvent> pattern = Pattern
            .<OrderEvent>begin("create")
            .where(new SimpleCondition<OrderEvent>() {
                @Override
                public boolean filter(OrderEvent value) throws Exception {
                    return "create".equalsIgnoreCase(value.getEventType());
                }
            })
            .next("pay")
            .where(new SimpleCondition<OrderEvent>() {
                @Override
                public boolean filter(OrderEvent value) throws Exception {
                    return "pay".equalsIgnoreCase(value.getEventType());
                }
            })
            .within(Time.minutes(15));
    
        PatternStream<OrderEvent> ps = CEP.pattern(orderKS, pattern);
    
        SingleOutputStreamOperator<String> normalStream = ps.select(
            new OutputTag<String>("late") {},
            new PatternTimeoutFunction<OrderEvent, String>() {
                @Override
                public String timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
                    return pattern.toString();
                }
            },
            new PatternSelectFunction<OrderEvent, String>() {
                @Override
                public String select(Map<String, List<OrderEvent>> pattern) throws Exception {
                    return pattern.toString();
                }
            }
        );
        normalStream.print("正常");
        normalStream.getSideOutput(new OutputTag<String>("late") {}).print("迟到数据");
    
        env.execute();
        
    }
}
