package com.shuanghe.network.analysis.aggregate;

import com.shuanghe.network.analysis.model.PvKeyByModel;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @author yushu
 */
public class PvCountAgg implements AggregateFunction<PvKeyByModel, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(PvKeyByModel value, Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}
