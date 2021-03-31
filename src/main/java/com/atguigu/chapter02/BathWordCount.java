package com.atguigu.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/3/31 10:12
 */
public class BathWordCount {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 2. 从文件读数据
        final DataSource<String> lineDS = env.readTextFile("input/words.txt");
        // 3. 各种转换
        // 3.1 flatMap
        final FlatMapOperator<String, String> wordDS = lineDS.flatMap(new FlatMapFunction<String, String>() {
        
            @Override
            public void flatMap(String line, Collector<String> out) throws Exception {
                for (String word : line.split(" ")) {
                    out.collect(word);
                }
            }
        });
        // 3.2 map 每个单词配置一个1  hello->  (hello, 1L)
        final MapOperator<String, Tuple2<String, Long>> wordAndOneDS = wordDS.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String word) throws Exception {
                return Tuple2.of(word, 1L);
            }
        });
        
        // 3.3 groupBy
        final UnsortedGrouping<Tuple2<String, Long>> groupedDS = wordAndOneDS.groupBy(0);
    
        // 3.4 求和
        final AggregateOperator<Tuple2<String, Long>> result = groupedDS.sum(1);
    
        // 4. 输出结果
        result.print();
        
    }
}
/*
1. 创建SparkContext
2. 从sc出发,得到一个rdd
3. 对rdd做各种转换
4. 行动算子
5. 关闭sc

 */