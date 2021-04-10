package com.atguigu.chapter08;

import com.atguigu.bean.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/10 10:30
 */
public class Flink_Project_High_Login {
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
        env
            .readTextFile("input/LoginLog.csv")
            .map(line -> {
                String[] data = line.split(",");
                return new LoginEvent(Long.valueOf(data[0]),
                                      data[1],
                                      data[2],
                                      Long.parseLong(data[3]) * 1000L);
            })
            .assignTimestampsAndWatermarks(wms)
            .keyBy(LoginEvent::getUserId)
            .process(new KeyedProcessFunction<Long, LoginEvent, String>() {
    
                private ListState<Long> failTsState;
    
                @Override
                public void open(Configuration parameters) throws Exception {
                    failTsState = getRuntimeContext().getListState(new ListStateDescriptor<Long>("failTsState", Long.class));
                }
    
                @Override
                public void processElement(LoginEvent loginEvent,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    // 需要一个集合, 存储两个失败的登录
                    if("fail".equalsIgnoreCase(loginEvent.getEventType())){
                        // 1. 把时间戳存入到状态中
                        failTsState.add(loginEvent.getEventTime());
    
                        ArrayList<Long> tss = new ArrayList<>();
                        for (Long ts : failTsState.get()) {
                            tss.add(ts);
                        }
//                        tss.sort((o1, o2) -> o1.compareTo(o2));
                        tss.sort(Long::compareTo);
                        
                        if(tss.size() == 2){
                            long deltaTs = (tss.get(1) - tss.get(0)) / 1000;
                            if(deltaTs <= 2){
                                out.collect(ctx.getCurrentKey() + "正在恶意登录, 请处理...");
                            }
    
                            tss.remove(0);
                            failTsState.update(tss);  // 更新状态
                        }
    
                    }else{
                        failTsState.clear();
                    }
                }
            })
            .print();
    
        env.execute();
    
    }
}
