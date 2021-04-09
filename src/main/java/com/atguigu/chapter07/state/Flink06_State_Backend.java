package com.atguigu.chapter07.state;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/7 15:22
 */
public class Flink06_State_Backend {
    
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop162:8020/flink/state/backend/fs"));
//        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop162:8020/flink/state/backend/rocks"));
        env.enableCheckpointing(3000);
        env.getCheckpointConfig().setCheckpointTimeout(10 * 1000);
        
        env
            .socketTextStream("hadoop162", 9999)
            .map(value -> {
                String[] datas = value.split(",");
                return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                
            })
            .keyBy(WaterSensor::getId)
            .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                
                private ReducingState<Integer> state;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    state = getRuntimeContext()
                        .getReducingState(new ReducingStateDescriptor<Integer>(
                            "state",
                            (value1, value2) -> value1 + value2,
                            Integer.class));
                }
                
                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    state.add(value.getVc());
                    
                    out.collect(ctx.getCurrentKey() + " " + state.get());
                    
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
