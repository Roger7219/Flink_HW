package com.atguigu.chapter;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/12 10:40
 */
public class Flink01_Table_BaseUse {
    public static void main(String[] args) throws Exception {
        // 流->动态表->连续查询->动态表-> 流
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> waterSensorStream =
            env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                             new WaterSensor("sensor_1", 2000L, 20),
                             new WaterSensor("sensor_2", 3000L, 30),
                             new WaterSensor("sensor_1", 4000L, 40),
                             new WaterSensor("sensor_1", 5000L, 50),
                             new WaterSensor("sensor_2", 6000L, 60));
        
        // 1. 先创建table的执行环境
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        // 2. 把流转成动态表
        Table table = tenv.fromDataStream(waterSensorStream);  // 动态表
        //table.printSchema(); // 打印标表的元数据
        // 3. 在动态表上执行连续查询,得到新的动态表
        //        Table t1 = table.select("id,ts,vc");  // 动态上执行连续查询
        
        // select .. from t1 where id=..
        Table t1 = table
            .select($("id"), $("ts").as("tt"), $("vc"))
            .where($("id").isEqual("sensor_1"));
        // 4. 把查询结果的动态表转成流, 写出去
        t1.execute().print();
        //        DataStream<Row> ds = tenv.toAppendStream(t1, Row.class);
        
        //        ds.print();
        
        //        env.execute();
        
    }
}
