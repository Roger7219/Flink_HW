package com.atguigu.chapter11;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/13 16:18
 */
public class Flink13_Function_Scalar {
    public static void main(String[] args) {
    
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    
        DataStreamSource<String> stream = env.fromElements("hello", "atguigu", "Hello");
    
        Table table = tEnv.fromDataStream(stream, $("word"));
        
        // table API种使用方法1:  内联方式 inline
       /* table
            .select($("word"), call(MyToUpperCase.class, $("word")).as("upper_case"))
            .execute()
            .print();*/
        
        // table API种使用方法2: 先注册再使用
        tEnv.createTemporaryFunction("my_upper", MyToUpperCase.class);
        table
            .select($("word"), call("my_upper", $("word")).as("aa"))
            .execute()
            .print();
        
        
    
    
    }
    
    public static class MyToUpperCase extends ScalarFunction{
        // 标量函数: 其实0或1或多个标量值, 得到一个标量
        public String eval(String s){
            return s.toUpperCase();
        }
    }
}


