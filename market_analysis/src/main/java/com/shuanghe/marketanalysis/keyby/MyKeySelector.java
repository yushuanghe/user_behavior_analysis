package com.shuanghe.marketanalysis.keyby;

import com.shuanghe.marketanalysis.model.MarketUserBehavior;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Description:
 * Date: 2021-04-01
 * Time: 21:31
 *
 * @author yushu
 */
public class MyKeySelector implements KeySelector<MarketUserBehavior, Tuple2<String, String>> {
    @Override
    public Tuple2<String, String> getKey(MarketUserBehavior value) throws Exception {
        return new Tuple2<>(value.getChannel(), value.getBehavior());
    }
}
