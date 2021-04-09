package com.atguigu.chapter07.state;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;

/**
 * ClassName ListTest
 * Description:
 * Create by Chenjiazhe
 * Date 2021/4/8 0008 下午 7:12
 */
public class ListTest {
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

                    private ListState<Integer> aa;

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        aa.add(value.getVc());
                        Iterable<Integer> it = aa.get();
                        ArrayList<Integer> ii = new ArrayList<>();
                        for (Integer i : it) {
                            ii.add(i);
                        }
                        ii.sort(Comparator.reverseOrder());
                        if (ii.size()>4) {
                            ii.remove(3);
                        }
                        aa.update(ii);
                        out.collect(aa.get().toString());


                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        aa = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("aa", Integer.class));
                    }
                });
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
