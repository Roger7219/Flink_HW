package com.atguigu.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/12 13:58
 */
public class Flink02_Table_Source_Kafka {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        Schema schema = new Schema()
            .field("id", DataTypes.STRING())
            .field("ts", DataTypes.BIGINT())
            .field("vc", DataTypes.INT());
        
        // 直接从Kafka读数据, 进入到一个动态表
        tenv
            .connect(new Kafka()
                         .version("universal")
                         .property("bootstrap.servers", "hadoop162:9092,hadoop163:9092")
                         .property("group.id", "Flink02_Table_Source_Kafka")
                         .topic("s1")
                         .startFromLatest()
            )
            //.withFormat(new Csv().fieldDelimiter(','))// filed的分隔符默认就是 逗号
            .withFormat(new Json())
            .withSchema(schema)
            .createTemporaryTable("sensor"); // flink内部的一个临时表名
    
        Table sensor = tenv.from("sensor");
        sensor.execute().print();
    
    }
}
