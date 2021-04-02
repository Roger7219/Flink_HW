package com.atguigu.chapter05.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/2 10:29
 */
public class Flink01_Sink_Kafka_2 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_11", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_11", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_11", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));
        
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop162:9092,hadoop163:9092");
        props.put("transaction.timeout.ms", 1000 * 60 * 15 + "");
        env
            .fromCollection(waterSensors)
            .addSink(new FlinkKafkaProducer<>(
                "sensor4",  // default-topic
                new KafkaSerializationSchema<WaterSensor>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(WaterSensor element, @Nullable Long timestamp) {
                        // 1. 让id决定去到哪个分区 或者2. 轮询去掉哪个分区
                        //return new ProducerRecord<>("sensor2", null, JSON.toJSONString(element).getBytes(StandardCharsets.UTF_8));
                        return new ProducerRecord<>("sensor4", element.getId().getBytes(StandardCharsets.UTF_8), JSON.toJSONString(element).getBytes(StandardCharsets.UTF_8));
                    }
                },
                props,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
/*
默认情况向Kafka写数据, 一个并行度对应kafka的一个分区

并行度是 m  kafka的分区数是n
如果m < n 则出现在Kafka中某些分区会没有数据, 造成数据倾斜
如果m >= n 就不会
 */