package com.atguigu.chapter05.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/1 11:36
 */
public class Flink01_Source_File {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1. 可以是文件夹,  也可以是具体的文件
        // 2. 路径可以是相对路径也可以是绝对路径.
        // 在idea是相对于project, 如果是在命令行提交, 相对于flink的家目录
        
        DataStreamSource<String> input = env.readTextFile("hdfs://hadoop162:8020/input");
    
        input.print();
    
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
