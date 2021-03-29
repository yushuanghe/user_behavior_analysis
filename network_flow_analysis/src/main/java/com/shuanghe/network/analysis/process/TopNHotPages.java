package com.shuanghe.network.analysis.process;

import com.shuanghe.network.analysis.model.UrlViewCount;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Description:
 * Date: 2021-03-29
 * Time: 20:20
 *
 * @author yushu
 */
public class TopNHotPages extends KeyedProcessFunction<Long, UrlViewCount, String> {
    private final int topN;

    ListState<UrlViewCount> urlViewCountListState = null;

    public TopNHotPages(int topN) {
        this.topN = topN;
    }

    @Override
    public void processElement(UrlViewCount value, Context ctx, Collector<String> out) throws Exception {
        urlViewCountListState.add(value);
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
    }

    /**
     * watermark只有更新时才向下游传递
     * 只有watermark更新时才会触发定时器
     *
     * @param timestamp
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        //排序输出
        List<UrlViewCount> list = new ArrayList<>();
        for (UrlViewCount event : urlViewCountListState.get()) {
            list.add(event);
        }
        urlViewCountListState.clear();
        list.sort((o1, o2) -> Math.toIntExact(o2.getCount() - o1.getCount()));

        list = list.subList(0, Math.min(topN, list.size()));

        //输出格式化
        out.collect(String.format("窗口结束时间：%s", new Timestamp(timestamp - 1)));
        for (int i = 0; i < list.size(); i++) {
            UrlViewCount event = list.get(i);
            out.collect(String.format("NO：%s，url=%s，pv=%s", i + 1, event.getUrl(), event.getCount()));
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        urlViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<UrlViewCount>("urlViewCountListState", UrlViewCount.class));
    }
}
