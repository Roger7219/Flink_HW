package com.atguigu.chapter05.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/2 8:56
 */
public class Flink09_Rolling_Reduce {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        
        DataStreamSource<WaterSensor> stream = env.fromElements(new WaterSensor("sensor_1", 10L, 10),
//                                                                new WaterSensor("sensor_1", 20L, 10),
//                                                                new WaterSensor("sensor_1", 40L, 40),
//                                                                new WaterSensor("sensor_1", 30L, 30),
//                                                                new WaterSensor("sensor_2", 10L, 100),
                                                                new WaterSensor("sensor_2", 2L, 200));
    
        stream
            .keyBy(WaterSensor::getId)
            .reduce(new ReduceFunction<WaterSensor>() { // 聚合前的数据类型和聚合后的数据类型完全一致!
                @Override
                public WaterSensor reduce(WaterSensor value1,
                                          WaterSensor value2) throws Exception {
                    System.out.println("reduce ...");
//                    return new WaterSensor(value1.getId(), value1.getTs(), value1.getVc() + value2.getVc());
                    return new WaterSensor(value1.getId(), value1.getTs(), Math.max(value1.getVc(), value2.getVc()));
                }
            })
            .print();
          
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
/*
1. reduce 聚合的结果和输入数据类型必须一致
2. 如果某个key只有一个元素, 则不会触发reduce方法

 */