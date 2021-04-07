package com.atguigu.chapter07.state;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/7 15:22
 */
public class Flink04_State_Keyed_List {
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
                
                private ListState<Integer> state;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    state = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("state", Integer.class));
                }
                
                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    state.add(value.getVc());
                    // 从状态中取出最高的3个水位值
                    ArrayList<Integer> vcs = new ArrayList<>();
                    for (Integer vc : state.get()) {
                        vcs.add(vc);
                    }
                    // 对List集合排序取前3
                    //                    Collections.sort(vcs, (o1, o2) -> o2.compareTo(o1));  // 原地排序
                    //                    vcs.sort((o1, o2) -> o2.compareTo(o1));  // 原地排序
                    vcs.sort(Comparator.reverseOrder());  // 原地排序
                    if(vcs.size() > 3){
                        vcs.remove(3);
                    }
                    state.update(vcs);
                    out.collect(vcs.toString());
                    
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
