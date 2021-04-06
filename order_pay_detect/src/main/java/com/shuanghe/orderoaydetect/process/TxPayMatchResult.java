package com.shuanghe.orderoaydetect.process;

import com.shuanghe.orderoaydetect.model.OrderEvent;
import com.shuanghe.orderoaydetect.model.ReceiptEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author yushu
 */
public class TxPayMatchResult extends CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>> {
    private final long timeout1;
    private final long timeout2;
    private final OutputTag<OrderEvent> orderEventOutputTag;
    private final OutputTag<ReceiptEvent> receiptEventOutputTag;

    public TxPayMatchResult(long timeout1, long timeout2, OutputTag<OrderEvent> orderEventOutputTag, OutputTag<ReceiptEvent> receiptEventOutputTag) {
        this.timeout1 = timeout1;
        this.timeout2 = timeout2;
        this.orderEventOutputTag = orderEventOutputTag;
        this.receiptEventOutputTag = receiptEventOutputTag;
    }

    /**
     * 定义状态，保存两种事件
     */
    private ValueState<OrderEvent> payEventState = null;
    private ValueState<ReceiptEvent> receiptEventState = null;

    @Override
    public void processElement1(OrderEvent value, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
        ReceiptEvent receiptEvent = receiptEventState.value();
        if (receiptEvent != null) {
            //两种事件都到了，正常输出匹配，清空状态
            out.collect(new Tuple2<>(value, receiptEvent));

            payEventState.clear();
            receiptEventState.clear();
        } else {
            //还没来，注册定时器开始等待
            ctx.timerService().registerEventTimeTimer(value.getTimestamp() + timeout1);
            payEventState.update(value);
        }
    }

    @Override
    public void processElement2(ReceiptEvent value, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
        OrderEvent orderEvent = payEventState.value();
        if (orderEvent != null) {
            //两种事件都到了，正常输出匹配，清空状态
            out.collect(new Tuple2<>(orderEvent, value));

            payEventState.clear();
            receiptEventState.clear();
        } else {
            //还没来，注册定时器开始等待
            ctx.timerService().registerEventTimeTimer(value.getTimestamp() + timeout2);
            receiptEventState.update(value);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        if (payEventState.value() != null) {
            ctx.output(orderEventOutputTag, payEventState.value());
        }
        if (receiptEventState.value() != null) {
            ctx.output(receiptEventOutputTag, receiptEventState.value());
        }
        payEventState.clear();
        receiptEventState.clear();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        payEventState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("payEventState", OrderEvent.class));
        receiptEventState = getRuntimeContext().getState(new ValueStateDescriptor<ReceiptEvent>("receiptEventState", ReceiptEvent.class));
    }
}
