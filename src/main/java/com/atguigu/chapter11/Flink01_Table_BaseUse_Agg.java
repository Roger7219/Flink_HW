package com.atguigu.chapter11;

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
public class Flink01_Table_BaseUse_Agg {
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
        
        // select id, sum(vc) vc_sum where vc >20 from t group by id
        /*Table t1 = table
            .where($("vc").isGreaterOrEqual(10))
            .groupBy($("id"))
            .aggregate($("vc").sum().as("vc_sum"))
            .select($("id"), $("vc_sum"));*/
    
        Table t1 = table
            .where($("vc").isGreaterOrEqual(10))
            .groupBy($("id"))
            .select($("id"), $("vc").sum().as("sum_vc"), $("vc").count().as("vc_count"));
    
        t1.execute().print();
        
    
       /* DataStream<Tuple2<Boolean, Row>> result = tenv.toRetractStream(t1, Row.class);
        result.filter(t -> t.f0).map(t -> t.f1).print();
        env.execute();*/
    
    }
}
