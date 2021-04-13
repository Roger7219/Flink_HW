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

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/13 16:18
 */
public class Flink13_Function_Table_1 {
    public static void main(String[] args) {
    
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    
        DataStreamSource<String> stream = env.fromElements("hello,atguigu", "hello,hello", "Hello,world,atguigu", "");
    
        Table table = tEnv.fromDataStream(stream, $("line"));
        tEnv.createTemporaryView("t", table);
    
        tEnv.createTemporaryFunction("my_s", Split.class);
        
        /*tEnv.sqlQuery("select line, word, len" +
                          " from t " +
                          " join lateral table(my_s(line)) on true").execute().print();*/
    
        tEnv.sqlQuery("select line, n_word, n_len" +
                          " from t " +
                          " left join lateral table(my_s(line)) as T(n_word, n_len) on true").execute().print();
        
       
    }
    
    // 炸完之后得到两列:  <hello, 5>
    @FunctionHint(output = @DataTypeHint("ROW<word string, len int>"))
    public static  class Split extends TableFunction<Row>{
        public void eval(String line){
            if (line.length() < 1) {
                return;
            }
            
            String[] data = line.split(",");
            for (String word : data) {
                collect(Row.of(word, word.length()));
            }
        }
    }
    
}


