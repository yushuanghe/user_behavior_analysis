package com.shuanghe.hotitems.analysis.aggregate;

import com.shuanghe.hotitems.analysis.model.RawData6Event;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Description:
 * Date: 2021-03-26
 * Time: 18:39
 *
 * @author yushu
 */
public class CountAggFunc implements AggregateFunction<RawData6Event, Long, Long> {
    @Override
    public Long createAccumulator() {
        return null;
    }

    @Override
    public Long add(RawData6Event value, Long accumulator) {
        return null;
    }

    @Override
    public Long getResult(Long accumulator) {
        return null;
    }

    @Override
    public Long merge(Long a, Long b) {
        return null;
    }
}
