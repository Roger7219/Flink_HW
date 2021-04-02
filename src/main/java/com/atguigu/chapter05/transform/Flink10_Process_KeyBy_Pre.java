package com.atguigu.chapter05.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/2 9:45
 */
public class Flink10_Process_KeyBy_Pre {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(3);
        
        DataStreamSource<WaterSensor> stream =
            env.fromElements(new WaterSensor("sensor_1", 10L, 10),
                             new WaterSensor("sensor_1", 20L, 10),
                             new WaterSensor("sensor_1", 40L, 40),
                             new WaterSensor("sensor_1", 30L, 30),
                             new WaterSensor("sensor_2", 10L, 100),
                             new WaterSensor("sensor_2", 2L, 200));
        
        // keyBy使用的算子中传递对象, 一个并行度一个
        stream
            .process(new ProcessFunction<WaterSensor, Integer>(){  // keyBy
                
                int sum = 0;
                @Override
                public void processElement(WaterSensor value,  // 输入
                                           Context ctx,  // 上下文对象
                                           Collector<Integer> out) throws Exception {
                    sum += value.getVc();
                    out.collect(sum);
                }
            })
            .print();
        
        env.execute();
    }
}
