package com.shuanghe.network.analysis.window;

import com.shuanghe.network.analysis.model.UrlViewCount;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author yushu
 */
public class PageViewCountWindowResult implements WindowFunction<Long, UrlViewCount, String, TimeWindow> {
    @Override
    public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<UrlViewCount> out) throws Exception {
        out.collect(new UrlViewCount(s, window.getEnd(), input.iterator().next()));
    }
}
