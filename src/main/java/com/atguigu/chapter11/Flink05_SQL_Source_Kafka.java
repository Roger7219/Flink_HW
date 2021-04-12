package com.atguigu.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/12 15:27
 */
public class Flink05_SQL_Source_Kafka {
    public static void main(String[] args) {
    
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        
        // 直接通过sql从file中读取数据
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
    
        tenv.sqlQuery("select * from sensor").execute().print();
    
    }
}
