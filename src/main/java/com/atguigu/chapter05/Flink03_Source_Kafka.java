package com.atguigu.chapter05;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.util.Properties;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/1 14:01
 */
public class Flink03_Source_Kafka {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
    
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop162:9092,hadoop163:9092,hadoop164:9092");
        props.put("group.id", "Flink03_Source_Kafka");
        // 消费的如果没有上次的消费记录, 则从最新位置开心消费.  如果有上次的消费记录, 则从上次的位置开始消费
        props.put("auto.offset.reset", "latest");
    
        /*DataStreamSource<String> stream = env
            .addSource(new FlinkKafkaConsumer<String>("sensor", new SimpleStringSchema(), props));
        stream.print();
        */
    
        DataStreamSource<ObjectNode> stream = env
            .addSource(new FlinkKafkaConsumer<>("sensor", new JSONKeyValueDeserializationSchema(false), props));
        stream.print();
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
