package com.atguigu.chapter11;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Schema;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/12 13:58
 */
public class Flink04_SQL_BaseUse {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        
        Schema schema = new Schema()
            .field("id", DataTypes.STRING())
            .field("ts", DataTypes.BIGINT())
            .field("vc", DataTypes.INT());
        
        DataStreamSource<WaterSensor> waterSensorStream =
            env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                             new WaterSensor("sensor_1", 2000L, 20),
                             new WaterSensor("sensor_2", 3000L, 30),
                             new WaterSensor("sensor_1", 4000L, 40),
                             new WaterSensor("sensor_1", 5000L, 50),
                             new WaterSensor("sensor_2", 6000L, 60));
        
        Table table = tenv.fromDataStream(waterSensorStream);  // 动态表
    
        //tenv.executeSql() // 执行增删改(DML) 创建表(DDL) 的这些sql
        // 执行查询的sql
        // 查询一个未注册的表
        tenv
            .sqlQuery("select * from " + table + " where id='sensor_1'")
            .execute()
            .print();
    
    }
}
