package com.shuanghe.network.analysis.window;

import com.shuanghe.network.analysis.model.PvCount;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author yushu
 */
public class PvCountWindowResult implements WindowFunction<Long, PvCount, String, TimeWindow> {
    @Override
    public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<PvCount> out) throws Exception {
        out.collect(new PvCount(window.getEnd(), input.iterator().next()));
    }
}
