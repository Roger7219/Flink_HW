/*
package com.atguigu.chapter05.transform;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;

*/
/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/1 15:01
 *//*

public class Flink05_Split {
    public static void main(String[] args) {
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        
        DataStreamSource<Integer> stream = env.fromElements(1, 2, 3, 4, 5, 8, 9);
        
        // 在1.12之后, 如果想实现这种类似的切割的功能, 建议使用侧输出流 SideOutput
        SplitStream<Integer> splitStream = stream
            .split(new OutputSelector<Integer>() {
                @Override
                public Iterable<String> select(Integer value) {
                    if (value % 2 == 0) {
                        return Collections.singletonList("偶数");
                    } else {
                        return Collections.singletonList("奇数");
                    }
                }
            });
    
        DataStream<Integer> s1 = splitStream.select("奇数");
        s1.print();
    
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
*/
