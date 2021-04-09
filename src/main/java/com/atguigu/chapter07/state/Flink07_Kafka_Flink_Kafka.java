package com.atguigu.chapter07.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/9 14:15
 */
public class Flink07_Kafka_Flink_Kafka {
    public static void main(String[] args) throws Exception {
    
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop162:9092,hadoop163:9092,hadoop164:9092");
        properties.setProperty("group.id", "Flink07_Kafka_Flink_Kafka");
        properties.setProperty("auto.offset.reset", "latest");
    
    
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment()
            .setParallelism(3);
        
        env.setStateBackend(new FsStateBackend("hdfs://hadoop162:8020/flink/fs"));
        env.enableCheckpointing(3000);
    
        // 设置模式为精确一次 (这是默认值)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    
        // 确认 checkpoints 之间的时间会间隔 500 ms
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
    
        // Checkpoint 必须在一分钟内完成，否则就会被抛弃  默认是10分钟
        env.getCheckpointConfig().setCheckpointTimeout(60000);
    
        // 同一时间只允许一个 checkpoint 进行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
    
        // 开启在 job 中止后仍然保留的 externalized checkpoints
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    
    
        env
            .addSource(new FlinkKafkaConsumer<>("words", new SimpleStringSchema(), properties))
            .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                @Override
                public void flatMap(String value,
                                    Collector<Tuple2<String, Long>> out) throws Exception {
                    for (String s : value.split(" ")) {
                        out.collect(Tuple2.of(s, 1L));
                    }
                
                }
            })
            .keyBy(t -> t.f0)
            .sum(1)
            .map(t -> t.f0 + "_" + t.f1)
            .addSink(new FlinkKafkaProducer<String>("hadoop162:9092", "wc", new SimpleStringSchema()));
    
        env.execute();
    
    }
}
