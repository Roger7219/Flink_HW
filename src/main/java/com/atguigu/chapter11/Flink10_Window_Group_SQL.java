package com.atguigu.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/13 10:48
 */
public class Flink10_Window_Group_SQL {
    
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql("create table sensor(" +
                            "id string," +
                            "ts bigint," +
                            "vc int, " +
                            "t as to_timestamp(from_unixtime(ts,'yyyy-MM-dd HH:mm:ss'))," +
                            "watermark for t as t - interval '5' second)" +
                            "with("
                            + "'connector' = 'filesystem',"
                            + "'path' = 'input/sensor.txt',"
                            + "'format' = 'csv'"
                            + ")");
        
        /*tEnv
            .sqlQuery("select " +
                          " id," +
                          " TUMBLE_START(t, interval '5' second), " +
                          " TUMBLE_END(t, interval '5' second)," +
                          " sum(vc) vc_sum " +
                          " from sensor" +
                          " group by id, TUMBLE(t, interval '5' second)"
            )
            .execute()
            .print();*/
    
       /* tEnv
            .sqlQuery("select " +
                          " id," +
                          " HOP_START(t, interval '2' second, interval '5' second), " +
                          " HOP_END(t, interval '2' second, interval '5' second), " +
                          " sum(vc) vc_sum " +
                          " from sensor" +
                          " group by id, HOP(t, interval '2' second, interval '5' second)"
            )
            .execute()
            .print();*/
    
        tEnv
            .sqlQuery("select " +
                          " id," +
                          " SESSION_START(t, interval '2' second), " +
                          " SESSION_END(t, interval '2' second), " +
                          " sum(vc) vc_sum " +
                          " from sensor" +
                          " group by id, SESSION(t, interval '2' second)"
            )
            .execute()
            .print();
    }
    
}
// select id, sum(a) over(partition by t order by ts desc) from t;