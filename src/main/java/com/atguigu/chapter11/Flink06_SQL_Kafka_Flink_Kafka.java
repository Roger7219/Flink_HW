package com.atguigu.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/12 15:27
 */
public class Flink06_SQL_Kafka_Flink_Kafka {
    public static void main(String[] args) {
    
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        
        // 1. source直接通过sql从file中读取数据
        tenv.executeSql("create table sensor( " +
                            "   id string, " +
                            "   ts bigint, " +
                            "   vc int " +
                            ")with(" +
                            "  'connector' = 'kafka', " +
                            "  'topic' = 's3', " +
                            "  'properties.bootstrap.servers' = 'hadoop162:9092', " +
                            "  'properties.group.id' = 'Flink05_SQL_Source_Kafka', " +
                            "   'scan.startup.mode' = 'latest-offset', " +
                            "   'format' = 'json' " +
                            ")");
        
        //2. sink 通过ddl创建一张表, 和kafka中的topic关联
        tenv.executeSql("create table abc( " +
                            "   id string, " +
                            "   ts bigint, " +
                            "   vc int " +
                            ")with(" +
                            "  'connector' = 'kafka', " +
                            "  'topic' = 's4', " +
                            "  'properties.bootstrap.servers' = 'hadoop162:9092', " +
                            "   'format' = 'json', " +
                            "   'sink.partitioner'='round-robin' " +
                            ")");
    
//        Table table = tenv.sqlQuery("select * from sensor where id='sensor_1'");
//        table.executeInsert("abc");
        // insert into ... select ...
        
        tenv.executeSql("insert into abc select * from sensor where id='sensor_1'");
    }
}
