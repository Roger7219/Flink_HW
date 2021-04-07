package com.atguigu.chapter07.state;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/7 14:58
 */
public class Flink02_State_Operator_Broadcast {
    public static void main(String[] args) {
        /*
        有一个数据流中有很多的数据, 有a逻辑处理 有b逻辑 ...
        
        控制数据在一个流中, 把他广播状态, 和数据流进行connect
         */
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        
        DataStreamSource<String> dataStream = env.socketTextStream("hadoop162", 9999);
        DataStreamSource<String> controlStream = env.socketTextStream("hadoop162", 8888);
        
        // 把控制流做成广播状态
        BroadcastStream<String> controlState = controlStream
            .broadcast(new MapStateDescriptor<String, String>("bdState", String.class, String.class));
        
        dataStream
            .connect(controlState)
            .process(new BroadcastProcessFunction<String, String, String>() {
                @Override
                public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                    // 处理数据流中的数据
                    // 从广播状态读取数据
                    ReadOnlyBroadcastState<String, String> bdState = ctx
                        .getBroadcastState(new MapStateDescriptor<String, String>("bdState", String.class, String.class));
                    String aSwitch = bdState.get("switch");
                    if ("a".equalsIgnoreCase(aSwitch)) {
                        out.collect("使用a逻辑进行处理数据...");
                    } else if ("b".equalsIgnoreCase(aSwitch)) {
                        out.collect("使用b逻辑进行处理数据...");
                    } else {
                        out.collect("使用默认逻辑进行处理数据...");
                        
                    }
                }
                
                @Override
                public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                    // 处理广播流中的数据
                    // 把数据存入到广播状态
                    BroadcastState<String, String> bdState = ctx
                        .getBroadcastState(new MapStateDescriptor<String, String>("bdState", String.class, String.class));
                    bdState.put("switch", value);  // switch->a ...
                    
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
