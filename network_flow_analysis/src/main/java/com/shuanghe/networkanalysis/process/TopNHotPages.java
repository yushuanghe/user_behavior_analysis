package com.shuanghe.networkanalysis.process;

import com.shuanghe.networkanalysis.model.UrlViewCount;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Description:
 * Date: 2021-03-29
 * Time: 20:20
 *
 * @author yushu
 */
public class TopNHotPages extends KeyedProcessFunction<Long, UrlViewCount, String> {
    private final int topN;

    //ListState<UrlViewCount> urlViewCountListState = null;
    MapState<String, Long> urlViewCountMapState = null;

    public TopNHotPages(int topN) {
        this.topN = topN;
    }

    @Override
    public void processElement(UrlViewCount value, Context ctx, Collector<String> out) throws Exception {
        //urlViewCountListState.add(value);
        urlViewCountMapState.put(value.getUrl(), value.getCount());
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        //窗口彻底关闭后触发，清空状态
        //另外注册一个定时器，1分钟之后触发，这时窗口已经彻底关闭，不再有聚合结果输出，可以清空状态
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 60000L);
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
        //判断定时器触发时间
        if (timestamp == ctx.getCurrentKey() + 60000L) {
            //窗口结束时间1分钟之后，直接清空状态
            urlViewCountMapState.clear();
            return;
        }

        ////排序输出
        //List<UrlViewCount> list = new ArrayList<>();
        //for (UrlViewCount event : urlViewCountListState.get()) {
        //    list.add(event);
        //}
        //urlViewCountListState.clear();
        //list.sort((o1, o2) -> Math.toIntExact(o2.getCount() - o1.getCount()));
        //
        //list = list.subList(0, Math.min(topN, list.size()));
        //
        ////输出格式化
        //out.collect(String.format("窗口结束时间：%s", new Timestamp(timestamp - 1)));
        //for (int i = 0; i < list.size(); i++) {
        //    UrlViewCount event = list.get(i);
        //    out.collect(String.format("NO：%s，url=%s，pv=%s", i + 1, event.getUrl(), event.getCount()));
        //}

        List<Tuple2<String, Long>> list = new ArrayList<>();
        for (Map.Entry<String, Long> entry : urlViewCountMapState.entries()) {
            list.add(new Tuple2<>(entry.getKey(), entry.getValue()));
        }

        list.sort((Tuple2<String, Long> o1, Tuple2<String, Long> o2) -> Math.toIntExact(o2.f1 - o1.f1));

        list.subList(0, Math.min(topN, list.size()));

        //输出格式化
        out.collect(String.format("窗口结束时间：%s", new Timestamp(timestamp - 1)));
        for (int i = 0; i < list.size(); i++) {
            String url = list.get(i).f0;
            long count = list.get(i).f1;
            out.collect(String.format("NO：%s，url=%s，pv=%s", i + 1, url, count));
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //urlViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<UrlViewCount>("urlViewCountListState", UrlViewCount.class));
        urlViewCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("urlViewCountMapState", String.class, Long.class));
    }
}
