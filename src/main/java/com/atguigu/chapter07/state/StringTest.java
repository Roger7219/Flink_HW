package com.atguigu.chapter07.state;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * ClassName StringTest
 * Description:
 * Create by Chenjiazhe
 * Date 2021/4/8 0008 下午 7:36
 */
public class StringTest {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        DataStreamSource<String> ds = env.socketTextStream("hadoop162", 9999);
        ds.map(x->{
            String[] datas = x.split(",");
            return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
        })
                .keyBy(x->x.getId())
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    private ValueState<Integer> aa;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        aa = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("aa", Integer.class));

                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        Integer v = aa.value();
                        if (v != null) {
                            if (v>10&&value.getVc()>10) {
                                out.collect("连续两次测量水位大于10, 红色预警!!!");
                            }

                        }
                        aa.update(value.getVc());
                    }
                }).print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
