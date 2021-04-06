package com.atguigu.chapter06;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * ClassName Test
 * Description:
 * Create by Chenjiazhe
 * Date 2021/4/6 0006 上午 8:34
 */
public class Test {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        see.setParallelism(2);

        see.readTextFile("input/UserBehavior.csv")
        .map(x -> {
            String[] split = x.split(",");
            return new UserBehavior(Long.valueOf(split[0]),Long.valueOf(split[1]),Integer.valueOf(split[2]),split[3], Long.valueOf(split[4]));
        })
                .keyBy(UserBehavior::getBehavior)
                .process(new KeyedProcessFunction<String, UserBehavior, Long>() {
                    int sum = 0;
                    @Override
                    public void processElement(UserBehavior value, Context ctx, Collector<Long> out) throws Exception {
                        if ("pv".equalsIgnoreCase(value.getBehavior())){
                            sum++;
                            out.collect((long) sum);
                        }
                    }
                }).print();
    }
}
