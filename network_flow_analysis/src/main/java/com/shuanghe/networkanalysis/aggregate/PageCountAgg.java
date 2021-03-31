package com.shuanghe.networkanalysis.aggregate;

import com.shuanghe.networkanalysis.model.ApacheLogEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @author yushu
 */
public class PageCountAgg implements AggregateFunction<ApacheLogEvent, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(ApacheLogEvent value, Long accumulator) {
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
