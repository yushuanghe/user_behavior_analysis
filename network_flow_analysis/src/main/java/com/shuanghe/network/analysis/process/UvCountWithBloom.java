package com.shuanghe.network.analysis.process;

import com.shuanghe.network.analysis.model.RawData6Event;
import com.shuanghe.network.analysis.model.UvCount;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Description:
 * Date: 2021-03-30
 * Time: 21:46
 *
 * @author yushu
 */
public class UvCountWithBloom extends ProcessWindowFunction<RawData6Event, UvCount,String, TimeWindow> {
    @Override
    public void process(String s, Context context, Iterable<RawData6Event> elements, Collector<UvCount> out) throws Exception {

    }
}
