package com.atguigu.chapter11;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/12 13:58
 */
public class Flink03_Table_Sink_Kafka {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        
        Schema schema = new Schema()
            .field("id", DataTypes.STRING())
            .field("ts", DataTypes.BIGINT())
            .field("vc", DataTypes.INT());
        
        DataStreamSource<WaterSensor> waterSensorStream =
            env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                             new WaterSensor("sensor_1", 2000L, 20),
                             new WaterSensor("sensor_2", 3000L, 30),
                             new WaterSensor("sensor_1", 4000L, 40),
                             new WaterSensor("sensor_1", 5000L, 50),
                             new WaterSensor("sensor_2", 6000L, 60));
        
        Table table = tenv.fromDataStream(waterSensorStream);  // 动态表
    
        Table result = table
            .where($("id").isEqual("sensor_1"))
            .select($("id"), $("ts"), $("vc"));
    
        tenv
            .connect(new Kafka()
                         .version("universal")
                         .property("bootstrap.servers", "hadoop162:9092")
                         .topic("s2")
                         .sinkPartitionerRoundRobin())
//            .withFormat(new Csv().lineDelimiter(""))
            .withFormat(new Json())
            .withSchema(schema)
            .createTemporaryTable("s2");
        
        result.executeInsert("s2");
    
        
    
    }
}
