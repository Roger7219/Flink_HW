package com.atguigu.chapter05.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/2 9:45
 */
public class Flink10_Process_KeyBy_Post {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(30);
        
        DataStreamSource<WaterSensor> stream =
            env.fromElements(new WaterSensor("sensor_1", 10L, 10),
                             new WaterSensor("sensor_1", 20L, 10),
                             new WaterSensor("sensor_1", 40L, 40),
                             new WaterSensor("sensor_1", 30L, 30),
                             new WaterSensor("sensor_2", 10L, 100),
                             new WaterSensor("sensor_2", 2L, 200));
        
        // keyBy使用的算子中传递对象, 一个并行度一个
        stream
            .keyBy(WaterSensor::getId)
            // KeyBy之后使用process, 每种key都有自己的状态
            .process(new KeyedProcessFunction<String, WaterSensor, Integer>() {
                int sum = 0;
    
                @Override
                public void open(Configuration parameters) throws Exception {
                    // open的执行, 只和并行度有关
                    System.out.println("open ....");
                }
    
                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<Integer> out) throws Exception {
                    System.out.println(ctx.getCurrentKey());
                    sum += value.getVc();
                    out.collect(sum);
                }
            })
            .print();
           
        
        env.execute();
    }
}
