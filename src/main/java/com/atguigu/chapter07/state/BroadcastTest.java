package com.atguigu.chapter07.state;

import com.atguigu.bean.WaterSensor;
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
 * ClassName BroadcastTest
 * Description:
 * Create by Chenjiazhe
 * Date 2021/4/8 0008 下午 8:05
 */
public class BroadcastTest {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        DataStreamSource<String> dataStream = env.socketTextStream("hadoop162", 9999);
        DataStreamSource<String> controlSteram = env.socketTextStream("hadoop162", 8888);
        //做这个流的目的是为了能用BroadcastProcessFunction，方便后面调出BroadcastState
        //这个流自带一个描述广播状态的描述器
        BroadcastStream<String> aa = controlSteram.broadcast(new MapStateDescriptor<String, String>("aa", String.class, String.class));
        dataStream.connect(aa)
                .process(new BroadcastProcessFunction<String, String, String>() {
                    @Override
                    public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                        BroadcastState<String, String> aa1 = ctx.getBroadcastState(new MapStateDescriptor<String, String>("aa", String.class, String.class));
                        aa1.put("switch", value);

                    }

                    @Override
                    public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        ReadOnlyBroadcastState<String, String> aa1 = ctx.getBroadcastState(new MapStateDescriptor<String, String>("aa", String.class, String.class));
                        String aSwitch = aa1.get("switch");
                        if ("a".equalsIgnoreCase(aSwitch)) {
                            out.collect("使用a逻辑进行处理数据...");
                        } else if ("b".equalsIgnoreCase(aSwitch)) {
                            out.collect("使用b逻辑进行处理数据...");
                        } else {
                            out.collect("使用默认逻辑进行处理数据...");
                        }
                    }
                }).print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
