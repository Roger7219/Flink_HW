package com.atguigu.chapter07;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/6 11:23
 */
public class Flink17_Timer_Project {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
    
    
        env
            .socketTextStream("hadoop162", 9999)
            .map(line -> {
                String[] data = line.split(",");
                return new WaterSensor(data[0],
                                       Long.parseLong(data[1]) * 1000,
                                       Integer.valueOf(data[2]));
            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<WaterSensor>forMonotonousTimestamps()
                    .withTimestampAssigner((ws, ts) -> ws.getTs())
            )
            .keyBy(WaterSensor::getId)
            .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                boolean isFirst = true;  // 表示是否为这个sensor的第一条数据
                int lastVc = 0;
                long timerTime = 0;
                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    // 注册定时器的时机: 1. 第一条数据进来 2. 这次相比上次下降
                    if(isFirst){
                        timerTime = ctx.timestamp() + 5000;
                        ctx.timerService().registerEventTimeTimer(timerTime);
                        isFirst = false;
                    }else if(value.getVc() <= lastVc){ // 这次的水位大于上次的水位
                        // 1. 上次注册定时器注销
                        ctx.timerService().deleteEventTimeTimer(timerTime);
                        // 2. 重新注册新的定时器
                        timerTime = ctx.timestamp() + 5000;
                        ctx.timerService().registerEventTimeTimer(timerTime);
                    }
                    // 更新水位: 把这次的水位赋值给lastVc, 给下次使用
                    lastVc = value.getVc();
                    
                }
    
                @Override
                public void onTimer(long timestamp,
                                    OnTimerContext ctx,
                                    Collector<String> out) throws Exception {
                    out.collect(ctx.getCurrentKey() + " 连续5秒水位上升!");
                    isFirst = true;
                }
            })
            .print();
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


