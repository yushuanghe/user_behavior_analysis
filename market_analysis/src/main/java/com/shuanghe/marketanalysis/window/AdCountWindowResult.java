package com.shuanghe.marketanalysis.window;

import com.shuanghe.marketanalysis.model.AdClickCountByPlacement;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @author yushu
 */
public class AdCountWindowResult extends ProcessWindowFunction<Long, AdClickCountByPlacement, String, TimeWindow> {
    @Override
    public void process(String s, Context context, Iterable<Long> elements, Collector<AdClickCountByPlacement> out) throws Exception {
        out.collect(new AdClickCountByPlacement(new Timestamp(context.window().getEnd()).toString(), s, elements.iterator().next()));
    }
}
