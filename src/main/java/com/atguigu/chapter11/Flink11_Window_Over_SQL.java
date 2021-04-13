package com.atguigu.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/13 10:48
 */
public class Flink11_Window_Over_SQL {
    
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql("create table sensor(" +
                            "id string," +
                            "ts bigint," +
                            "vc int, " +
                            "t as to_timestamp(from_unixtime(ts,'yyyy-MM-dd HH:mm:ss'))," +
                            "watermark for t as t - interval '3' second)" +
                            "with("
                            + "'connector' = 'filesystem',"
                            + "'path' = 'input/sensor.txt',"
                            + "'format' = 'csv'"
                            + ")");
        
        /*tEnv
            .sqlQuery("select id, ts, vc, " +
                          //                          " sum(vc) over(partition by id order by t rows between unbounded preceding and current row) vc_sum " +  // 默认值
                          //                          " sum(vc) over(partition by id order by t rows between 1 preceding and current row) vc_sum " +  // 默认值
//                          " sum(vc) over(partition by id order by t range between interval '1' second preceding and current row) vc_sum " +  // 默认值
                          " sum(vc) over(partition by id order by t range between unbounded preceding and current row) vc_sum " +  // 默认值
                          " from sensor")
            .execute()
            .print();*/
        
        /*tEnv
            .sqlQuery("select id, ts, vc, " +
                          " sum(vc) over(partition by id order by t rows between unbounded preceding and current row) vc_sum, " +  // 默认值
                          " max(vc) over(partition by id order by t rows between unbounded preceding and current row) vc_max, " +  // 默认值
                          " min(vc) over(partition by id order by t rows between unbounded preceding and current row) vc_min, " +  // 默认值
                          " avg(vc) over(partition by id order by t rows between unbounded preceding and current row) vc_avg " +  // 默认值
                          " from sensor")
            .execute()
            .print();*/
    
        tEnv
            .sqlQuery("select id, ts, vc, " +
                          " sum(vc) over w vc_sum, " +
                          " max(vc) over w vc_max, " +
                          " min(vc) over w vc_min, " +
                          " avg(vc) over w vc_avg " +
                          " from default_catalog.default_database.sensor " +
                          " window w as (partition by id order by t rows between unbounded preceding and current row)")
            .execute()
            .print();
    }
    
}