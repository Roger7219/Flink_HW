package com.atguigu.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/12 13:58
 */
public class Flink02_Table_Source_File {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        Schema schema = new Schema()
            .field("id", DataTypes.STRING())
            .field("ts", DataTypes.BIGINT())
            .field("vc", DataTypes.INT());
    
        // 直接从文件读数据, 进入到一个动态表
        tenv
            .connect(new FileSystem().path("input/sensor.txt"))
            .withFormat(new Csv().fieldDelimiter(',').lineDelimiter("\n"))
            .withSchema(schema)
            .createTemporaryTable("sensor");
        
        // 得到一个Table对象
        Table sensor = tenv.from("sensor");
        sensor.execute().print();
        
       // tenv.sqlQuery("select * from sensor").execute().print();
        
        /*sensor
            .select($("id"))
            .execute()
            .print();*/
        
    
    }
}
