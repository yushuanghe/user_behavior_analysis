package com.shuanghe.marketanalysis.source;

import com.shuanghe.marketanalysis.model.MarketUserBehavior;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.*;

/**
 * Description:自定义数据源
 * Date: 2021-03-31
 * Time: 20:35
 *
 * @author yushu
 */
public class SimulatedSource extends RichSourceFunction<MarketUserBehavior> {
    /**
     * 是否运行的标志位
     */
    private boolean running = true;

    /**
     * 定义用户行为和渠道的集合
     */
    private final List<String> behaviorSet = new ArrayList<>(Arrays.asList("start", "click", "download", "install", "uninstall"));
    private final List<String> channelSet = new ArrayList<>(Arrays.asList("appstore", "weibo", "wechat", "tieba"));

    private final Random rand = new Random();

    @Override
    public void run(SourceContext<MarketUserBehavior> ctx) throws Exception {
        //生成数据最大的数量
        long maxCounts = Long.MAX_VALUE;
        long count = 0;

        //while循环，随机产生数据
        while (running && count < maxCounts) {
            String uid = UUID.randomUUID().toString();
            String behavior = behaviorSet.get(rand.nextInt(behaviorSet.size()));
            String channel = channelSet.get(rand.nextInt(channelSet.size()));

            ctx.collect(new MarketUserBehavior(uid, behavior, channel, System.currentTimeMillis()));
            count++;
            Thread.sleep(50L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
