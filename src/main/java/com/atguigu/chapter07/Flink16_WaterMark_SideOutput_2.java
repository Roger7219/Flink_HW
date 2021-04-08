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
public class Flink16_WaterMark_SideOutput_2 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        
        OutputTag<WaterSensor> outputTagS2 = new OutputTag<WaterSensor>("sensor_2") {};
        OutputTag<WaterSensor> outputTagS3 = new OutputTag<WaterSensor>("other_sensor") {};
        
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
            .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                @Override
                public void process(String key,
                                    Context ctx,
                                    Iterable<WaterSensor> elements,
                                    Collector<String> out) throws Exception {
                    // sensor_1 放入主流
                    if ("sensor_1".equalsIgnoreCase(key)) {
                        for (WaterSensor ele : elements) {
                            out.collect(ele.toString());
                        }
                    } else if ("sensor_2".equalsIgnoreCase(key)) {
                        // 使用侧输出流和窗口没有关系, 只要有context都可以使用.
                        // sensor_2 放入侧输出流
                        for (WaterSensor ele : elements) {
                            ctx.output(outputTagS2, ele);
                        }
                        
                    }else{
                        for (WaterSensor ele : elements) {
                            ctx.output(outputTagS3, ele);
                        }
                    }
                    
                }
            });
        
        main.print("main");
        main.getSideOutput(outputTagS2).print("sensor_2");
        main.getSideOutput(outputTagS3).print("other");
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


