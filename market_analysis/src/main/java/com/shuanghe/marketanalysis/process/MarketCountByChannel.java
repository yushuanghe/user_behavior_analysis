package com.shuanghe.marketanalysis.process;

import com.shuanghe.marketanalysis.model.MarketUserBehavior;
import com.shuanghe.marketanalysis.model.MarketViewCount;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * Description:
 * Date: 2021-04-01
 * Time: 21:21
 *
 * @author yushu
 */
public class MarketCountByChannel extends ProcessWindowFunction<MarketUserBehavior, MarketViewCount, Tuple2<String, String>, TimeWindow> {
    @Override
    public void process(Tuple2<String, String> stringStringTuple2, Context context, Iterable<MarketUserBehavior> elements, Collector<MarketViewCount> out) throws Exception {
        String windowStart = new Timestamp(context.window().getStart()).toString();
        String windowEnd = new Timestamp(context.window().getEnd()).toString();
        String channel = stringStringTuple2.f0;
        String behavior = stringStringTuple2.f1;
        long count = 0;
        for (MarketUserBehavior element : elements) {
            count++;
        }

        out.collect(new MarketViewCount(windowStart, windowEnd, channel, behavior, count));
    }
}
