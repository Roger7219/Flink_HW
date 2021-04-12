package com.atguigu.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/12 13:58
 */
public class Flink03_Table_Sink_File {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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
        Table result = sensor
            .where($("id").isEqual("sensor_1"));
        
        // 把动态表的数据, 直接写入到文件
        tenv
            // 写出的时候: 多并行度这里是文件夹, 如果只有一个并行度就是一个文件名
            .connect(new FileSystem().path("input/1026"))
            .withFormat(new Csv())
//            .withFormat(new Json()) // 文件不支持json
            .withSchema(schema)
            .createTemporaryTable("result");
        // 把动态表的数据, 写入到临时表, 由于临时表和文件直接connect在一起, 所以数据就直接进入了文件
        result.executeInsert("result");
        
    }
}
