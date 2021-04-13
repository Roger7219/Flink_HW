package com.atguigu.chapter11;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.OverWindow;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/13 10:48
 */
public class Flink11_Window_Over_TableApi {
    
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> waterSensorStream = env
            .socketTextStream("hadoop162", 9999)
            .map(line -> {
                String[] split = line.split(",");
                return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((element, recordTimestamp) -> element.getTs() * 1000)
            );
        
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table table = tableEnv
            .fromDataStream(waterSensorStream, $("id"), $("ts").rowtime(), $("vc"));
      
        /*
            select
                id,
                ts,
                vc,
                sum(vc) over(partition by id order by ts)
            from s
         */
        //        OverWindow over = Over.partitionBy($("id")).orderBy($("ts")).preceding(UNBOUNDED_ROW).as("w");
//        OverWindow over = Over.partitionBy($("id")).orderBy($("ts")).preceding(rowInterval(1L)).as("w");
        
//        OverWindow over = Over.partitionBy($("id")).orderBy($("ts")).preceding(lit(2).second()).as("w");
        OverWindow over = Over.partitionBy($("id")).orderBy($("ts")).preceding(UNBOUNDED_RANGE).as("w");
        table
            .window(over)
            .select($("id"), $("ts"), $("vc"), $("vc").sum().over($("w")))
            .execute()
            .print();
        
    }
    
}
/*
RANGE:
    当多条数据的时间(处理或事件)一样,那么他们会进入同一个窗口.
        计算的时候, 会计算在一起
 */
