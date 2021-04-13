package com.atguigu.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/13 9:40
 */
public class Flink09_Time_SQL_Event {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        String sql = "create table sensor(" +
            "   id string," +
            "   ts bigint," +
            "   vc int, " +
            // 计算一个字段, 作为事件时间  from_unixtime接受的是秒
//            "   et as to_timestamp(from_unixtime(ts, 'yyyy-MM-dd HH:mm:ss')), " +  // 1000ms/1000=1s  -> "1970-01-01 00:00:01" -> timestamp
            "   et as to_timestamp(from_unixtime(ts)), " +  // 1000ms/1000=1s  -> "1970-01-01 00:00:01" -> timestamp
            // 声明水印
            "   watermark for et as et - interval '3' second " +
            ") with("
            + "'connector' = 'filesystem',"
            + "'path' = 'input/sensor.txt',"
            + "'format' = 'csv'"
            + ")";
        tenv.executeSql(sql);
        
        TableResult result = tenv.executeSql("select * from sensor");
        result.print();
        
    }
}
