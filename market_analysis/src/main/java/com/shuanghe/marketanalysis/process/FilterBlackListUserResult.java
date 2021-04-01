package com.shuanghe.marketanalysis.process;

import com.shuanghe.marketanalysis.model.AdClickLog;
import com.shuanghe.marketanalysis.model.BlackListUserWarning;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author yushu
 */
public class FilterBlackListUserResult extends KeyedProcessFunction<Tuple2<String, String>, AdClickLog, AdClickLog> {
    private final int threshold;

    public FilterBlackListUserResult(int threshold) {
        this.threshold = threshold;
    }

    private ValueState<Long> countState = null;
    private ValueState<Long> resetTimerTsState = null;
    private ValueState<Boolean> isBlackState = null;

    @Override
    public void processElement(AdClickLog value, Context ctx, Collector<AdClickLog> out) throws Exception {
        long curTs = ctx.timerService().currentProcessingTime();

        long curCount = countState.value() == null ? 0 : countState.value();
        long curTsState = resetTimerTsState.value() == null ? 0 : resetTimerTsState.value();

        if (curTsState < curTs) {
            //获取明天0点的时间戳
            long ts = (curTs / (1000L * 60 * 60 * 24) + 1) * (1000L * 60 * 60 * 24)
                    //时区
                    - 8 * 60 * 60 * 1000L;
            resetTimerTsState.update(ts);
            ctx.timerService().registerProcessingTimeTimer(ts);
        }

        boolean isBlack = isBlackState.value() != null && isBlackState.value();
        if (curCount >= threshold) {
            if (!isBlack) {
                //黑名单只输出一次
                isBlackState.update(true);
                //侧输出流
                ctx.output(new OutputTag<BlackListUserWarning>("warning") {
                }, new BlackListUserWarning(value.getUid(), value.getAppId(), String.format("点击超过%s次", curCount)));
            }
            return;
        }

        //正常情况
        curCount++;
        countState.update(curCount);
        out.collect(value);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickLog> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        long curTsState = resetTimerTsState.value() == null ? 0 : resetTimerTsState.value();
        if (timestamp == curTsState) {
            countState.clear();
            isBlackState.clear();
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("countState", Long.class));
        resetTimerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("resetTimerTsState", Long.class));
        isBlackState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isBlackState", Boolean.class));
    }
}
