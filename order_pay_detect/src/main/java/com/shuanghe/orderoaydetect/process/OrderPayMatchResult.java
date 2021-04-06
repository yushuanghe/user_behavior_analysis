package com.shuanghe.orderoaydetect.process;

import com.shuanghe.orderoaydetect.model.OrderEvent;
import com.shuanghe.orderoaydetect.model.OrderResult;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Description:
 * Date: 2021-04-06
 * Time: 18:13
 *
 * @author yushu
 */
public class OrderPayMatchResult extends KeyedProcessFunction<String, OrderEvent, OrderResult> {
    private final OutputTag<OrderResult> outputTag;
    private final long timeout;

    public OrderPayMatchResult(OutputTag<OrderResult> outputTag, long timeout) {
        this.outputTag = outputTag;
        this.timeout = timeout;
    }

    /**
     * 表示create，pay是否已经来过
     */
    private MapState<String, Boolean> booleanMapState = null;

    private ValueState<Long> timerTsState = null;

    private static final String EVENT_LOAD = "load";
    private static final String EVENT_START = "start";

    @Override
    public void processElement(OrderEvent value, Context ctx, Collector<OrderResult> out) throws Exception {
        boolean isCreated = booleanMapState.contains(EVENT_LOAD) ? booleanMapState.get(EVENT_LOAD) : false;
        boolean isPayed = booleanMapState.contains(EVENT_START) ? booleanMapState.get(EVENT_START) : false;
        long ts = timerTsState.value() == null ? 0L : timerTsState.value();

        if (EVENT_LOAD.equals(value.getEventType())) {
            //继续判断后续事件
            if (isPayed) {
                /*
                数据可能乱序
                输出主流
                 */
                out.collect(new OrderResult(value.getOrderId(), String.format("payed successfully")));
                //清空定时器，状态
                booleanMapState.clear();
                ctx.timerService().deleteEventTimeTimer(ts);
                timerTsState.clear();
            } else {
                /*
                注册定时器
                更新状态
                 */
                long timerTs = value.getTimestamp() + timeout;
                ctx.timerService().registerEventTimeTimer(timerTs);
                timerTsState.update(timerTs);
                booleanMapState.put(EVENT_LOAD, true);
            }
        }

        if (EVENT_START.equals(value.getEventType())) {
            if (isCreated) {
                if (value.getTimestamp() < ts) {
                    /*
                    未超时
                    数据可能乱序
                    输出主流
                     */
                    out.collect(new OrderResult(value.getOrderId(), String.format("payed successfully")));
                } else {
                    //输出到侧输出流
                    ctx.output(outputTag, new OrderResult(value.getOrderId(), String.format("payed but already timeout")));
                }
                //清空定时器，状态
                booleanMapState.clear();
                ctx.timerService().deleteEventTimeTimer(ts);
                timerTsState.clear();
            } else {
                //create没来，注册定时器，等到pay的时间
                //有watermark延迟
                ctx.timerService().registerEventTimeTimer(value.getTimestamp());
                booleanMapState.put(EVENT_START, true);
                timerTsState.update(value.getTimestamp());
            }
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OrderResult> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        boolean isCreated = booleanMapState.contains(EVENT_LOAD) ? booleanMapState.get(EVENT_LOAD) : false;
        boolean isPayed = booleanMapState.contains(EVENT_START) ? booleanMapState.get(EVENT_START) : false;
        long ts = timerTsState.value() == null ? 0L : timerTsState.value();

        if (isPayed) {
            //pay来了，create没到
            ctx.output(outputTag, new OrderResult(ctx.getCurrentKey(), String.format("payed but not found create log")));
        }

        if (isCreated) {
            //create来了，pay没到
            ctx.output(outputTag, new OrderResult(ctx.getCurrentKey(), String.format("order timeout")));
        }

        //清空状态
        booleanMapState.clear();
        timerTsState.clear();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        booleanMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Boolean>("booleanMapState", String.class, Boolean.class));
        timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTsState", Long.class));
    }
}
