package com.atguigu.chapter06;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/2 15:41
 */
public class Flink01_Project_PV {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(4);
        
        // 1. 读数据
        
        // 2. 对数进行处理
        // 2.1. 封装pojo
        // 2.2 去重
        // 3.3 map成 (pv, 1)
        // 3.4 keyBy
        // 3.5 集合
    
        env
            .readTextFile("input/UserBehavior.csv")
            .map(line -> {
                String[] data = line.split(",");
                return new UserBehavior(Long.valueOf(data[0]),
                                        Long.valueOf(data[1]),
                                        Integer.valueOf(data[2]),
                                        data[3],
                                        Long.valueOf(data[4]));
            })
            .filter(ub -> "pv".equalsIgnoreCase(ub.getBehavior()))
            .map(ub -> Tuple2.of("pv", 1L)).returns(Types.TUPLE(Types.STRING, Types.LONG))
            .keyBy(t -> t.f0)
            .sum(1)
            .print();
        
        
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
