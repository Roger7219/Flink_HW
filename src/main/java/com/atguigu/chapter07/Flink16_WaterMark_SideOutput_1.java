package com.atguigu.chapter07;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/6 11:23
 */
public class Flink16_WaterMark_SideOutput_1 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
    
        SingleOutputStreamOperator<String> main = env
            .socketTextStream("hadoop162", 9999)
            .map(line -> {
                String[] data = line.split(",");
                return new WaterSensor(data[0],
                                       Long.parseLong(data[1]) * 1000,
                                       Integer.valueOf(data[2]));
            })
            // 分配水印
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                        // 返回这条数据的事件时间, 必须是毫秒
                        @Override
                        public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                            return element.getTs();
                        }
                    })
            )
            .keyBy(WaterSensor::getId)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .allowedLateness(Time.seconds(2))
            .sideOutputLateData(new OutputTag<WaterSensor>("late"){})
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
            });
        
        main.print("main");
        // 获取侧输出流
        main.getSideOutput(new OutputTag<WaterSensor>("late"){}).print("side");
    
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
/*
flink如果处理迟到数据(乱序)?
1. 使用水印
2. 允许迟到
       时间到了, 先对窗口中的数据做计算, 但是窗口不关闭, 属于这个窗口数据来一条就计算一条
       达到允许的时间就会关闭窗口, 以后迟到的数据就彻底进不了窗口
       
3. 放入侧输出流

 */

