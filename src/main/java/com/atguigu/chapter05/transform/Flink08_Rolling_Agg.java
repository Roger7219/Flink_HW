package com.atguigu.chapter05.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/2 8:56
 */
public class Flink08_Rolling_Agg {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        
        DataStreamSource<WaterSensor> stream = env.fromElements(new WaterSensor("sensor_1", 10L, 10),
                                                                new WaterSensor("sensor_1", 20L, 10),
                                                                new WaterSensor("sensor_1", 40L, 40),
                                                                new WaterSensor("sensor_1", 30L, 30),
                                                                new WaterSensor("sensor_2", 10L, 100),
                                                                new WaterSensor("sensor_2", 2L, 200));
        
        // select id, sum(), 'a' from a group by id
        stream
            .keyBy(WaterSensor::getId)  // ws -> ws.getId()
            //            .sum("vc")
            //            .max("vc")
            //            .min("vc")
//            .maxBy("vc", false)
            .print();
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
/*
sum max min 对非keyBy和非聚合字段, 默认选择的是第一个值
maxBy minBy  通过第二个参数来控制求他字段是否选择第一个
 */