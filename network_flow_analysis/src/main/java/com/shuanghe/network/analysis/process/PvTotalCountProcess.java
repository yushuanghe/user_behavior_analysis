package com.shuanghe.network.analysis.process;

import com.shuanghe.network.analysis.model.PvCount;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author yushu
 */
public class PvTotalCountProcess extends KeyedProcessFunction<Long, PvCount, PvCount> {
    private ValueState<Long> pvTotalValueState = null;

    @Override
    public void processElement(PvCount value, Context ctx, Collector<PvCount> out) throws Exception {
        long count = value.getCount();
        // TODO: 2021-03-30 未完成 
        long currentPv = pvTotalValueState.value();
        pvTotalValueState.update(currentPv + count);
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<PvCount> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        out.collect(new PvCount(ctx.getCurrentKey(), pvTotalValueState.value()));
        pvTotalValueState.clear();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        pvTotalValueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("pvTotalValueState", Long.class));
    }
}
