package com.atguigu.chapter05.sink;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/2 10:29
 */
public class Flink04_Sink_Custom {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
    
        env
            .socketTextStream("hadoop162", 9999)
            .map(line -> {
                String[] data = line.split(",");
                return new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2]));
            })
            .addSink(new MySqlSink());
            
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static class MySqlSink extends RichSinkFunction<WaterSensor> {
    
        private Connection conn;
        private PreparedStatement ps;
    
        @Override
        public void open(Configuration parameters) throws Exception {
            String mysqlUrl = "jdbc:mysql://hadoop162:3306/test?useSSL=false";
            // 建立都mysql的连接
            conn = DriverManager.getConnection(mysqlUrl, "root", "aaaaaa");
    
            //String sql = "insert into sensor values(?,?,?)";
//            String sql = "INSERT INTO sensor values(?, ?, ?)\n" +
//                "ON DUPLICATE KEY UPDATE vc=?";
            String sql = "replace INTO sensor values(?, ?, ?)";
            ps = conn.prepareStatement(sql);
        }
    
        @Override
        public void invoke(WaterSensor value,
                           Context context) throws Exception {
            
            ps.setString(1, value.getId());
            ps.setLong(2, value.getTs());
            ps.setInt(3, value.getVc());
//            ps.setInt(4, value.getVc());
            ps.execute();
        }
    
        @Override
        public void close() throws Exception {
            if (ps != null) {
                ps.close();
            }
            // 关闭资源
            if (conn != null) {
                conn.close();
            }
        }
    }
}
