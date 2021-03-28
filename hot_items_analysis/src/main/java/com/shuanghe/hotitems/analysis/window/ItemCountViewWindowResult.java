package com.shuanghe.hotitems.analysis.window;

import com.shuanghe.hotitems.analysis.model.ItemViewCountEvent;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Description:
 * Date: 2021-03-26
 * Time: 18:46
 *
 * @author yushu
 */
public class ItemCountViewWindowResult implements WindowFunction<Long, ItemViewCountEvent, String, TimeWindow> {
    @Override
    public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<ItemViewCountEvent> out) throws Exception {
        long viewCount = input.iterator().next();
        long windowEnd = window.getEnd();
        out.collect(new ItemViewCountEvent(s, windowEnd, viewCount));
    }
}
