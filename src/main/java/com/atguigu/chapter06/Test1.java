package com.atguigu.chapter06;

import com.atguigu.bean.OrderEvent;
import com.atguigu.bean.TxEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;


/**
 * ClassName Test1
 * Description:
 * Create by Chenjiazhe
 * Date 2021/4/6 0006 下午 8:01
 */


/**
 *      connect后用keyby来进行分组，会分成两个组，用process分别进行两个计算
 *      如果涉及到两个流之间的数据交换，则需要一个公共变量（集合）来进行暂存交换的数据
 *      如果想将这两个流合并成一个流结果输出，则需要在process中new一个KeyedProcessFunction，运用这个方法的前提是在进行keyby的时候两个key的类型必须一致
 */
public class Test1 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);


        SingleOutputStreamOperator<OrderEvent> orderEvent = env.readTextFile("input/OrderLog.csv")
                .map(log -> {
                    String[] data = log.split(",");
                    return new OrderEvent(Long.valueOf(data[0]),
                            data[1],
                            data[2],
                            Long.valueOf(data[3]));
                })
                .filter(x -> "pay".equalsIgnoreCase(x.getEventType()));

        SingleOutputStreamOperator<TxEvent> txEvent = env
                .readTextFile("input/ReceiptLog.csv")
                .map(log -> {
                    String[] data = log.split(",");
                    return new TxEvent(data[0],
                            data[1],
                            Long.valueOf(data[2]));
                });


        orderEvent.connect(txEvent)
                //保证两个流的数据能同时出现在一个通道里
                .keyBy(x->x.getTxId(),x->x.getTxId())
                //k保证了这个对象的方法值检查key类型为k的数据并进行运算
                .process(new KeyedCoProcessFunction<String,OrderEvent,TxEvent,String>(){
                    private HashMap<String,OrderEvent> orderEvent = new HashMap<>();
                    private HashMap<String,TxEvent> txEvent = new HashMap<>();

                    @Override
                    public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
                        if (txEvent.containsKey(value.getTxId())) {
                            out.collect("订单: " + value.getOrderId() + " 对账成功!");
                        }else {
                            orderEvent.put(value.getTxId(),value);
                        }

                    }

                    @Override
                    public void processElement2(TxEvent value, Context ctx, Collector<String> out) throws Exception {
                        if (orderEvent.containsKey(value.getTxId())) {
                            out.collect("订单: " + orderEvent.get(value.getTxId()).getOrderId() + " 对账成功!");
                        } else {
                            txEvent.put(value.getTxId(), value);
                        }
                    }
                })
                .print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
