package com.shuanghe.networkanalysis.trigger;

import com.shuanghe.networkanalysis.model.RawData6Event;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * Description:自定义触发器，每来一条数据，直接触发窗口计算，并清空窗口状态
 * Date: 2021-03-30
 * Time: 21:50
 *
 * @author yushu
 */
public class UvCountWithBloomTrigger extends Trigger<RawData6Event, TimeWindow> {
    @Override
    public TriggerResult onElement(RawData6Event element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.FIRE_AND_PURGE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        //nothing
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        //nothing
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

    }
}
