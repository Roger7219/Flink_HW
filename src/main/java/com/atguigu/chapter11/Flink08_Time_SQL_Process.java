package com.atguigu.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/13 9:40
 */
public class Flink08_Time_SQL_Process {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv.executeSql("create table sensor(" +
                            "   id string," +
                            "   ts bigint," +
                            "   vc int, " +
                            "   pt as  proctime()" +
                            ") with("
                            + "'connector' = 'filesystem',"
                            + "'path' = 'input/sensor.txt',"
                            + "'format' = 'csv'"
                            + ")");
        
        TableResult result = tenv.executeSql("select * from sensor");
        result.print();
        
    }
}
