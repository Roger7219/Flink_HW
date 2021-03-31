package com.atguigu.chapter02;

import org.apache.flink.api.common.typeinfo.Types;
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
public class StreamBoundedWordCount_Lambda {
    public static void main(String[] args) throws Exception {
        // 1. 创建流的环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2. 读取数据: 无界流和有界流
        final DataStreamSource<String> lineStream = env.readTextFile("input/words.txt");
        // 3. 各种转换
        final SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneStream = lineStream
            .flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                for (String word : line.split(" ")) {
                    out.collect(Tuple2.of(word, 1L));
                }
            })
            .returns(Types.TUPLE(Types.STRING, Types.LONG));
        
        // keyBy数据类型不会做任何的变化   x => x.f0
        final KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = wordAndOneStream.keyBy(value -> value.f0);
        final SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOneKS.sum(1);
        // 4. 打印结果
        result.print();
        // 5. 执行流环境
        env.execute();
    }
}
/*
1. 在java中, 如果传递是一个接口类型, 并且这个接口只有一个抽象方法, 则可以使用Lambda表示式
2. 在flink中, 多了一些限制:


 */