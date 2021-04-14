package com.atguigu.chapter11;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * ClassName Test
 * Description:
 * Create by Chenjiazhe
 * Date 2021/4/12 0012 下午 8:31
 */
public class Test {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv.executeSql(" create table sensor( " +
                "id string, " +
                "ts bigint, " +
                "vc int," +
                "t as to_timestamp(from_unixtime(ts,'yyyy-MM-dd HH:mm:ss')), " +
                " watermark for t as t - interval '5' second) " +
                "with( " +
                " 'connector' = 'filesystem' , " +
                " 'path' = 'input/sensor.txt', " +
                " 'format' = 'csv' " +
                " ) "

        );

        tenv.sqlQuery("select " +
                " id, " +
                "TUMBLE_START(t,interval '2' second), " +
                "TUMBLE_END(t,interval '2' second), " +
                "sum(vc) vc_sum " +
                "from sensor " +
                "group by id,TUMBLE(t,interval '2' second)"
        )
                .execute().print();

    }
}
