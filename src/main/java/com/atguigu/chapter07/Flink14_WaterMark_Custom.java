package com.atguigu.chapter07;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/6 11:23
 */
public class Flink14_WaterMark_Custom {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(3000);
        
        WatermarkStrategy<WaterSensor> wms =
            new WatermarkStrategy<WaterSensor>() {
                
                @Override
                public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                    return new MyPeriod();
                }
            }.withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                @Override
                public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                    return element.getTs();
                }
            });
        
        env
            .socketTextStream("hadoop162", 9999)
            .map(line -> {
                String[] data = line.split(",");
                return new WaterSensor(data[0],
                                       Long.parseLong(data[1]) * 1000,
                                       Integer.valueOf(data[2]));
            })
            // 分配水印
            .assignTimestampsAndWatermarks(wms)
            .keyBy(WaterSensor::getId)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                @Override
                public void process(String key,
                                    Context ctx,
                                    Iterable<WaterSensor> elements,
                                    Collector<String> out) throws Exception {
                    int count = 0;
                    for (WaterSensor ele : elements) {
                        count++;
                    }
                    
                    String msg = "当前key: " + key +
                        "窗口: [ " + ctx.window().getStart() / 1000 + ", " + ctx.window().getEnd() / 1000 + "), " +
                        "个数: " + count;
                    out.collect(msg);
                    
                }
            })
            .print();
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static class MyPeriod implements WatermarkGenerator<WaterSensor>{
        long maxTs = Long.MIN_VALUE + 3000 + 1;
    
        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            System.out.println("来一条数据执行一次...");
            // 每来一条数据, 应该更新最大时间戳
            maxTs = Math.max(maxTs, eventTimestamp);
        }
    
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            System.out.println("周期的执行...");
            // 默认每200ms向数据中插入水印
            output.emitWatermark(new Watermark(maxTs - 3000 - 1));
        }
    }
}

