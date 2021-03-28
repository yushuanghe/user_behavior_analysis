package com.shuanghe.hotitems.analysis.aggregate;

import com.shuanghe.hotitems.analysis.model.RawData6Event;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Description:自定义预聚合函数 AggregateFunction ，聚合状态就是当前的count值
 * Date: 2021-03-26
 * Time: 18:39
 *
 * @author yushu
 */
public class CountAggFunc implements AggregateFunction<RawData6Event, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    /**
     * 每来一条数据调用一次add，count值加一
     *
     * @param value
     * @param accumulator
     * @return
     */
    @Override
    public Long add(RawData6Event value, Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    /**
     * sessionWindow 里做窗口合并使用
     *
     * @param a
     * @param b
     * @return
     */
    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}
