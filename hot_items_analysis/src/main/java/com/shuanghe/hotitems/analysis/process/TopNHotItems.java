package com.shuanghe.hotitems.analysis.process;

import com.shuanghe.hotitems.analysis.model.ItemViewCountEvent;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * 自定义 KeyedProcessFunction
 *
 * @author yushu
 */
public class TopNHotItems extends KeyedProcessFunction<Long, ItemViewCountEvent, String> {
    private final int topSize;

    /**
     * 定义状态 ListState
     */
    private ListState<ItemViewCountEvent> itemViewCountListState = null;

    public TopNHotItems(int topSize) {
        this.topSize = topSize;
    }

    @Override
    public void processElement(ItemViewCountEvent value, Context ctx, Collector<String> out) throws Exception {
        //每来一条数据直接加入 ListState
        itemViewCountListState.add(value);
        //注册一个 windowEnd +1 毫秒之后触发的定时器
        //定时器注册只看传入时间，同一时间注册多个，相当于同一个定时器
        //多个不同时间是可以用一个其他状态判断是否已经注册过定时器来唯一注册
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
    }

    /**
     * 当定时器触发，可以认为所有窗口统计结果都已到齐，可以排序输出了
     *
     * @param timestamp
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        //排序 list ，保存 ListState 里面的所有数据
        List<ItemViewCountEvent> list = new ArrayList<>();
        for (ItemViewCountEvent event : itemViewCountListState.get()) {
            list.add(event);
        }

        //清空状态
        itemViewCountListState.clear();

        //排序
        list.sort((o1, o2) -> Math.toIntExact(o2.getCount() - o1.getCount()));

        list = list.subList(0, Math.min(topSize, list.size()));

        //输出格式化
        out.collect(String.format("窗口结束时间：%s", new Timestamp(timestamp - 1)));
        for (int i = 0; i < list.size(); i++) {
            ItemViewCountEvent event = list.get(i);
            out.collect(String.format("NO：%s，appId=%s，pv=%s", i + 1, event.getAppId(), event.getCount()));
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        itemViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<>(
                "item_view_count_list", ItemViewCountEvent.class));
    }
}
