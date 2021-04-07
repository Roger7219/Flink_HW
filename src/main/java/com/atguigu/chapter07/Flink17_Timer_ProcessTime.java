package com.atguigu.chapter07;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/6 11:23
 */
public class Flink17_Timer_ProcessTime {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        OutputTag<WaterSensor> outputTagS2 = new OutputTag<WaterSensor>("sensor_2") {};
        OutputTag<WaterSensor> outputTagS3 = new OutputTag<WaterSensor>("other_sensor") {};
    
        env
            .socketTextStream("hadoop162", 9999)
            .map(line -> {
                String[] data = line.split(",");
                return new WaterSensor(data[0],
                                       Long.parseLong(data[1]) * 1000,
                                       Integer.valueOf(data[2]));
            })
            .keyBy(WaterSensor::getId)
            .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    if (value.getId().equalsIgnoreCase("sensor_3")) {
                        // 注册处理时间定时器
                        long now = System.currentTimeMillis();
                        ctx.timerService().registerProcessingTimeTimer(now + 5000);
                        out.collect("sensor_3注册定时器: " + now);
                    }
                }
    
                // 当定时器触发的时候, 自动回调这个方法
                @Override
                public void onTimer(long timestamp,
                                    OnTimerContext ctx,
                                    Collector<String> out) throws Exception {
                    out.collect("sensor_3 触发了定时器: " + timestamp);
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


