package com.shuanghe.networkanalysis.process;

import com.shuanghe.networkanalysis.model.PvCount;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author yushu
 */
public class PvTotalCountProcess extends KeyedProcessFunction<Long, PvCount, PvCount> {
    /**
     * 保存pv总和
     */
    private ValueState<Long> pvTotalValueState = null;

    @Override
    public void processElement(PvCount value, Context ctx, Collector<PvCount> out) throws Exception {
        long count = value.getCount();
        //状态需要手动判断是否初始化
        long currentPv = pvTotalValueState.value() == null ? 0 : pvTotalValueState.value();
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
