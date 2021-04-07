package com.atguigu.chapter07.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Iterator;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/7 14:13
 */
public class Flink01_State_Operator_List {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(3);
        env.enableCheckpointing(1000);
        
    
        env
            .socketTextStream("hadoop162", 9999)
            .map(new MyMapFunction())
            .print();
        
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static class MyMapFunction implements MapFunction<String, String>, CheckpointedFunction{
        long count = 0;
        private ListState<Long> countState;
    
        @Override
        public String map(String value) throws Exception {
            count++;
            countState.clear();
            countState.add(count);
            return count + "";
        }
    
        // 程序会周期性的把状态进行checkpoint
        // 默认存在内存中
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            System.out.println("snapshotState...");
            countState.clear();
            countState.add(count);
        }
    
        // 程序启动或者回复的自动回调这方法
        @Override
        public void initializeState(FunctionInitializationContext ctx) throws Exception {
            System.out.println("initializeState...");
            // 获取算子状态 列表状态
            // 状态恢复的时候, 在各个并行度进行平均分配
//            countState = ctx.getOperatorStateStore().getListState(new ListStateDescriptor<Long>("countState", Long.class));
            // 把状态合并, 分发到每个并行度
            countState = ctx.getOperatorStateStore().getUnionListState(new ListStateDescriptor<Long>("countState", Long.class));
            
            // 把状态中的值读出赋值给变量
            Iterator<Long> it = countState.get().iterator();
            if (it.hasNext()) {
                count = it.next();
            }
    
        }
    }
}
