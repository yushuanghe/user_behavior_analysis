package com.shuanghe.networkanalysis.window;

import com.shuanghe.networkanalysis.model.RawData6Event;
import com.shuanghe.networkanalysis.model.UvCount;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

/**
 * Description:
 * Date: 2021-03-30
 * Time: 20:49
 *
 * @author yushu
 */
public class UvCountResult implements AllWindowFunction<RawData6Event, UvCount, TimeWindow> {
    @Override
    public void apply(TimeWindow window, Iterable<RawData6Event> values, Collector<UvCount> out) throws Exception {
        Set<String> set = new HashSet<>();
        for (RawData6Event event : values) {
            set.add(event.getUid());
        }
        out.collect(new UvCount(window.getEnd(), set.size()));
    }
}
