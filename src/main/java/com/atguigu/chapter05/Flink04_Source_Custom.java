package com.atguigu.chapter05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/1 14:20
 */
public class Flink04_Source_Custom {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        
        //自定义source:  从socket读取数据的source
        env.addSource(new MySource("hadoop162", 9999)).print();
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static class MySource implements SourceFunction<WaterSensor> {
        private final String host;
        private final int port;
        private boolean cancel;
        private Socket socket;
        private BufferedReader reader;
        
        public MySource(String host, int port) {
            this.host = host;
            this.port = port;
        }
        
        //从socket读取数据的source
        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            System.out.println("run....");
            // alt+ctrl+f 把变量提升为成员变量
            socket = new Socket(host, port);
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            String line = reader.readLine();
            while (line != null && !cancel) {
                // sensor_1,1,10
                String[] data = line.split(",");
                ctx.collect(new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2])));
                line = reader.readLine();
            }
            
        }
        
        @Override
        public void cancel() {
            cancel = true;
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                
            }
        }
    }
}
