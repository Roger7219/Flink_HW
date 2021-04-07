package com.atguigu.chapter07.state;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/7 15:22
 */
public class Flink03_State_Keyed_Value {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        env
            .socketTextStream("hadoop162", 9999)
            .map(value -> {
                String[] datas = value.split(",");
                return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                
            })
            .keyBy(WaterSensor::getId)
            .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                
                private ValueState<Integer> state;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    // 监控找谁要?
                    state = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("state", Integer.class));
                }
                
                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    // 获取上一个水位, 如果是第一次是null
                    Integer lastVc = state.value();
                    if (lastVc != null) {
                        // 不是第一次进入, 需要判断上次和这次水位是否都大于10
                        if (lastVc > 10 && value.getVc() > 10) {
                            out.collect("连续两次测量水位大于10, 红色预警!!!");
                        }
                    }
                    // 更新单值, 覆盖操作
                    state.update(value.getVc());
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
