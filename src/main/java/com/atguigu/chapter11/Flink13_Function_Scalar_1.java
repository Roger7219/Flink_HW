package com.atguigu.chapter11;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/13 16:18
 */
public class Flink13_Function_Scalar_1 {
    public static void main(String[] args) {
    
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    
        DataStreamSource<String> stream = env.fromElements("hello", "atguigu", "Hello");
    
        Table table = tEnv.fromDataStream(stream, $("word"));
        tEnv.createTemporaryView("t", table);
        
        // sql中使用, 一定要先注册
        
        tEnv.createFunction("my_upper", MyToUpperCase.class);
        tEnv
            .sqlQuery("select my_upper(word, 'ABC') from t")
            .execute()
            .print();
       
    }
    
    public static class MyToUpperCase extends ScalarFunction{
        // 标量函数: 其实0或1或多个标量值, 得到一个标量
        public String eval(String s, String s1){
            return s.toUpperCase() + s1.toLowerCase();
        }
    }
}


