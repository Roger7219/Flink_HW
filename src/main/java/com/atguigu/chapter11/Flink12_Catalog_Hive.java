package com.atguigu.chapter11;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/13 15:33
 */
public class Flink12_Catalog_Hive {
    public static void main(String[] args) {
        
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    
        String catalogName = "my_hive";
        String db = "flink_1026";
    
        // 1.创建hiveCatalog
        HiveCatalog hc = new HiveCatalog(catalogName, db, "input");
        // 2. 注册hiveCatalog
        tEnv.registerCatalog(catalogName, hc);
        // 3. 设置hiveCatalog为默认
        tEnv.useCatalog(catalogName);
        tEnv.useDatabase(db);
        
        // 4. 从hive查询数据
        
        tEnv.sqlQuery("select * from stu").execute().print();
        
    }
    
}
