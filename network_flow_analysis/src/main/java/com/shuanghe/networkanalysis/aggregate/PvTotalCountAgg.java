package com.shuanghe.networkanalysis.aggregate;

import com.shuanghe.networkanalysis.model.PvCount;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @author yushu
 */
public class PvTotalCountAgg implements ReduceFunction<PvCount> {
    @Override
    public PvCount reduce(PvCount value1, PvCount value2) throws Exception {
        return new PvCount(value1.getWindowEnd(), value1.getCount() + value2.getCount());
    }
}
