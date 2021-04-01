package com.shuanghe.marketanalysis.keyby;

import com.shuanghe.marketanalysis.model.AdClickLog;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author yushu
 */
public class FilterBlackListKeySelector implements KeySelector<AdClickLog, Tuple2<String, String>> {
    @Override
    public Tuple2<String, String> getKey(AdClickLog value) throws Exception {
        return new Tuple2<String, String>(value.getUid(), value.getAppId());
    }
}
