package com.atguigu.chapter08;

import com.atguigu.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/10 11:06
 */
public class Flink06_Project_High_Order {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(1);
        // 创建WatermarkStrategy
        WatermarkStrategy<OrderEvent> wms = WatermarkStrategy
            .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(20))
            .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                @Override
                public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                    return element.getEventTime();
                }
            });
        env
//            .readTextFile("input/OrderLog.csv")
            .socketTextStream("hadoop162", 9999)
            .map(line -> {
                String[] datas = line.split(",");
                return new OrderEvent(
                    Long.valueOf(datas[0]),
                    datas[1],
                    datas[2],
                    Long.parseLong(datas[3]) * 1000);
                
            })
            .assignTimestampsAndWatermarks(wms)
            .keyBy(OrderEvent::getOrderId)
            .process(new KeyedProcessFunction<Long, OrderEvent, String>() {
                
                private ValueState<OrderEvent> createState;
                private ValueState<OrderEvent> payState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    createState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("createState", OrderEvent.class));
                    payState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("payState", OrderEvent.class));
                }
                
                @Override
                public void processElement(OrderEvent orderEvent,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    if (createState.value() == null && payState.value() == null) {
                        // 定时器45分钟之后触发
                        ctx.timerService().registerEventTimeTimer(orderEvent.getEventTime() + 55 * 60 * 1000);
                    } else {
                        if(orderEvent.getOrderId() == 1L){
                            System.out.println("xxxx");
                        }
                        //任何一个不为空, 那么一定是这个不为空的那个事件注册定时器
                        OrderEvent oe = createState.value() == null ? payState.value() : createState.value();
                        
                        ctx.timerService().deleteEventTimeTimer(oe.getEventTime()  + 55 * 60 * 1000);
                    }
                    
                    if ("create".equalsIgnoreCase(orderEvent.getEventType())) {
                        // 这个数据是订单创建, 则需要判断支付是否已经来了,
                        // 如果已经来了, 表示正常
                        // 如果支付没来, 把数据放入状态, 等到支付来的时候进行判断
                        if (payState.value() == null) {
                            // 把创建存入到状态
                            createState.update(orderEvent);
                        }else if( (payState.value().getEventTime() -  orderEvent.getEventTime()) <= 45 * 60 * 1000){
                            out.collect(ctx.getCurrentKey() + " 正常创建和支付...");
                        }else{
                            out.collect(ctx.getCurrentKey() + " 创建和支付时间超过45分钟, 检测是否系统问题.");
                        }
                        
                    } else { // 这次是支付查
                        if (createState.value() == null) {
                            payState.update(orderEvent);
                        } else if((orderEvent.getEventTime() - createState.value().getEventTime()) <= 45 * 60 * 1000){
                            out.collect(ctx.getCurrentKey() + " 正常创建和支付...");
                        }else{
                            out.collect(ctx.getCurrentKey() + " 创建和支付时间超过45分钟, 检测是否系统问题.");
                        }
                    }
                }
                
                @Override
                public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                    if (createState.value() != null) {
                        out.collect("订单: " + createState.value().getOrderId() + " 有创建, 没有支付");
                        System.out.println(createState.value().getOrderId());
                    } else {
                        out.collect("订单: " + payState.value().getOrderId() + " 有支付, 没有创建, 系统问题");
                        
                    }
                }
            })
            .print();
        
        env.execute();
        
    }
}
