package com.atguigu.chapter07;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * ClassName Test
 * Description:
 * Create by Chenjiazhe
 * Date 2021/4/7 0007 上午 8:33
 */
public class Test {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        env.socketTextStream("hadoop162", 9999)
            .map(line -> {
                String[] data = line.split(",");
                return new WaterSensor(
                        data[0],
                        Long.parseLong(data[1]) * 1000,
                        Integer.valueOf(data[2])
                );
            })
            .assignTimestampsAndWatermarks(
                    WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>(){
                        @Override
                        public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                            return element.getTs();
                        }
                    })
            );
    }
}
