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
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/6 10:24
 */
public class Flink03_Project_Order {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        
        SingleOutputStreamOperator<OrderEvent> orderEventStream = env
            .readTextFile("input/OrderLog.csv")
            .map(log -> {
                String[] data = log.split(",");
                return new OrderEvent(Long.valueOf(data[0]),
                                      data[1],
                                      data[2],
                                      Long.valueOf(data[3]));
            })
            .filter(orderEvent -> "pay".equalsIgnoreCase(orderEvent.getEventType()));
        
        SingleOutputStreamOperator<TxEvent> txEventStream = env
            .readTextFile("input/ReceiptLog.csv")
            .map(log -> {
                String[] data = log.split(",");
                return new TxEvent(data[0],
                                   data[1],
                                   Long.valueOf(data[2]));
            });
        
        // connect两个流
        orderEventStream
            .connect(txEventStream)
            .keyBy(OrderEvent::getTxId, TxEvent::getTxId)
            .process(new KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>() {
                //
                private HashMap<String, OrderEvent> orderEventMap = new HashMap<>();
                private HashMap<String, TxEvent> txEventMap = new HashMap<>();
                
                @Override
                public void processElement1(OrderEvent value,
                                            Context ctx,
                                            Collector<String> out) throws Exception {
                    // 订单信息来了, 先判断对应的交易信息是否已经来了, 如果已经来了, 就对账成功
                    // 如果交易信息不在, 把自己存入到map中
                    if (txEventMap.containsKey(value.getTxId())) {
                        // 交易数据先到, 订单数据后到:  对账成功
                        out.collect("订单: " + value.getOrderId() + " 对账成功!");
                        
                    } else {
                        // 订单数据先到, 交易数据可能后到, 也可能不来
                        // 把订单数据存入到map中
                        orderEventMap.put(value.getTxId(), value);
                    }
                }
                
                @Override
                public void processElement2(TxEvent value,
                                            Context ctx,
                                            Collector<String> out) throws Exception {
                    if (orderEventMap.containsKey(value.getTxId())) {
                        out.collect("订单: " + orderEventMap.get(value.getTxId()).getOrderId() + " 对账成功!");
                    } else {
                        txEventMap.put(value.getTxId(), value);
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
