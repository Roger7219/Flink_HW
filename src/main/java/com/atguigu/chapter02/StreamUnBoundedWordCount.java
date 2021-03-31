package com.atguigu.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/3/31 10:33
 */
public class StreamUnBoundedWordCount {
    public static void main(String[] args) throws Exception {
        
        // 1. 创建流的环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2. 读取数据: 无界流和有界流
        DataStreamSource<String> lineStream = env.socketTextStream("hadoop162", 9999);
        
        // 3. 各种转换
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneStream = lineStream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Long>> out) throws Exception {
                for (String word : line.split(" ")) {
                    out.collect(Tuple2.of(word, 1L));
                }
            }
        });
        
        // keyBy数据类型不会做任何的变化
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = wordAndOneStream.keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> value) throws Exception {
                return value.f0;
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOneKS.sum(1);
        // 4. 打印结果
        result.print();
        // 5. 执行流环境
        env.execute();
    }
}
