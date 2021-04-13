package com.atguigu.chapter11;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/13 16:18
 */
public class Flink13_Function_Table {
    public static void main(String[] args) {
    
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    
        DataStreamSource<String> stream = env.fromElements("hello,atguigu", "hello,hello", "Hello,world,atguigu", "");
    
        Table table = tEnv.fromDataStream(stream, $("line"));
        tEnv.createTemporaryView("t", table);
        
        // 内联方式
        /*table
            .joinLateral(call(Split.class, $("line"))) // 炸出来一张新表: t1 然后t1会自动和t进行join连接
//            .leftOuterJoinLateral(call(Split.class, $("line")))
            .select($("line"), $("word"), $("len"))
            .execute()
            .print();*/
        
        
        // 注册后使用
        tEnv.createTemporaryFunction("my_s", Split.class);
        table
            .joinLateral(call("my_s", $("line"))) // 炸出来一张新表: t1 然后t1会自动和t进行join连接
            //            .leftOuterJoinLateral(call(Split.class, $("line")))
            .select($("line"), $("word"), $("len"))
            .execute()
            .print();
        
       
    }
    
    // 炸完之后得到两列:  <hello, 5>
    @FunctionHint(output = @DataTypeHint("ROW<word string, len int>"))
    public static  class Split extends TableFunction<Row>{
        public void eval(String line){
            if (line.length() < 15) {
                return;
            }
            
            String[] data = line.split(",");
            for (String word : data) {
                collect(Row.of(word, word.length()));
            }
        }
    }
    
}


