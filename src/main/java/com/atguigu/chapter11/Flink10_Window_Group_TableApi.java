package com.atguigu.chapter11;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Session;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/13 10:48
 */
public class Flink10_Window_Group_TableApi {
    
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> waterSensorStream = env
            .fromElements(new WaterSensor("sensor_1", 1000L, 10),
                          new WaterSensor("sensor_1", 2000L, 20),
                          new WaterSensor("sensor_1", 4000L, 40),
                          new WaterSensor("sensor_1", 5000L, 50),
                          new WaterSensor("sensor_2", 3000L, 30),
                          new WaterSensor("sensor_2", 6000L, 60))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((element, recordTimestamp) -> element.getTs())
            );
        
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table table = tableEnv
            .fromDataStream(waterSensorStream, $("id"), $("ts").rowtime(), $("vc"));
        // table api的方式使用分组窗口
        
        table
            //            .window(Tumble.over(lit(5).second()).on($("ts")).as("w"))
//            .window(Slide.over(lit(5).second()).every(lit(2).second()).on($("ts")).as("w"))
            .window(Session.withGap(lit(2).second()).on($("ts")).as("w"))
            .groupBy($("id"), $("w"))   // 使用窗口的目的是为了分组
            .select($("id"), $("w").start().as("w_start"), $("w").end().as("w_end"), $("vc").sum().as("vc_sum"))
            .execute()
            .print();
        
    }
    
}
