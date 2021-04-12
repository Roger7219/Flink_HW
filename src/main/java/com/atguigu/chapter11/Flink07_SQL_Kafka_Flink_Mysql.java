package com.atguigu.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/12 15:27
 */
public class Flink07_SQL_Kafka_Flink_Mysql {
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
        tenv.executeSql("create table s1( " +
                            "   id string, " +
                            "   sum_vc int ," +
                            "   primary key (id) NOT ENFORCED" +
                            ")with(" +
                            "  'connector' = 'jdbc', " +
                            "  'url' = 'jdbc:mysql://hadoop162:3306/test?useSSL=false'," +
                            "  'table-name' = 's1', " +
                            "  'username' = 'root', " +
                            "  'password' = 'aaaaaa' " +
                            ")");
        
        tenv.executeSql("insert into s1 select id, sum(vc) sum_vc from sensor group by id");
        
    }
}
