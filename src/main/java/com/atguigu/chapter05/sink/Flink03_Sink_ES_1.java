package com.atguigu.chapter05.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/2 10:29
 */
public class Flink03_Sink_ES_1 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 60));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));
        
        List<HttpHost> hosts = Arrays.asList(new HttpHost("hadoop162", 9200),
                                             new HttpHost("hadoop163", 9200),
                                             new HttpHost("hadoop164", 9200));
        ElasticsearchSink.Builder<WaterSensor> esBuilder =
            new ElasticsearchSink.Builder<>(hosts,
                                            new ElasticsearchSinkFunction<WaterSensor>() {
                                                @Override
                                                public void process(WaterSensor element,
                                                                    RuntimeContext ctx,
                                                                    RequestIndexer indexer) {
                    
                                                    IndexRequest index = Requests.indexRequest()
                                                        .index("sensor")
                                                        .type("_doc") // type理论上不能用_开头, 唯一个可以的就是_doc
                                                        .id(element.getId() + "_" + element.getTs())
                                                        .source(JSON.toJSONString(element), XContentType.JSON);  // todo
                                                    indexer.add(index);
                                                }
                                            });
        
        FlinkJedisPoolConfig redisConf = new FlinkJedisPoolConfig.Builder()
            .setHost("hadoop162")
            .setPort(6379)
            .setMaxTotal(100)
            .setMaxIdle(10)
            .setMinIdle(10)
            .setTimeout(10 * 1000)
            .build();
        
        env
            .fromCollection(waterSensors)
            .addSink(esBuilder.build());
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
