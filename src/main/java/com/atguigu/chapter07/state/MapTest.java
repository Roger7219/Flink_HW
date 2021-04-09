package com.atguigu.chapter07.state;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * ClassName MapTest
 * Description:
 * Create by Chenjiazhe
 * Date 2021/4/8 0008 下午 5:07
 */
public class MapTest {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> ds = env.socketTextStream("hadoop162", 9999);
        ds.map(x -> {
            String[] datas = x.split(",");
            return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
        })
                .keyBy(x -> x.getId())
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    private MapState<Integer, Object> aa;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        aa = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, Object>("aa", Integer.class, Object.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        aa.put(value.getVc(),new Object());
                        out.collect(aa.keys()+"");

                    }
                });
    }
}
