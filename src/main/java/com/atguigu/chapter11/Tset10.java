package com.atguigu.chapter11;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.sql.Time;
import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * ClassName Tset10
 * Description:
 * Create by Chenjiazhe
 * Date 2021/4/13 0013 下午 8:47
 */
public class Tset10 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<WaterSensor> ws = env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_2", 6000L, 60))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((ss, ts) -> ss.getTs()));

        StreamTableEnvironment ss = StreamTableEnvironment.create(env);

        Table table = ss.fromDataStream(ws, $("id"), $("ts").rowtime(), $("vc"));

        table.window(Tumble.over(lit(5).second()).on($("ts")).as("a"))
                .groupBy($("id"),$("a"))
                .select($("id"),$("w").start().as("w_start"),$("vc").sum().as("vc_sum"))
                .execute().print();

    }
}
