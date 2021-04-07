package com.shuanghe.orderoaydetect.process;

import com.shuanghe.orderoaydetect.model.OrderEvent;
import com.shuanghe.orderoaydetect.model.ReceiptEvent;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

/**
 * Description:
 * Date: 2021-04-07
 * Time: 21:04
 *
 * @author yushu
 */
public class TxMatchWithJoinResult extends ProcessJoinFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>> {
    @Override
    public void processElement(OrderEvent left, ReceiptEvent right, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
        out.collect(new Tuple2<OrderEvent, ReceiptEvent>(left, right));
    }
}
