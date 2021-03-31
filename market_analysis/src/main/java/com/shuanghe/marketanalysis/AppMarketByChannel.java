package com.shuanghe.marketanalysis;

import com.shuanghe.marketanalysis.model.MarketUserBehavior;
import com.shuanghe.marketanalysis.source.SimulatedSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Description:
 * Date: 2021-03-31
 * Time: 20:28
 *
 * @author yushu
 */
public class AppMarketByChannel {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<MarketUserBehavior> dataStream = env.addSource(new SimulatedSource())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<MarketUserBehavior>(Time.milliseconds(1)) {
                    @Override
                    public long extractTimestamp(MarketUserBehavior element) {
                        return element.getTimestamp();
                    }
                });
        dataStream.print();

        dataStream
                .filter(data -> !"uninstall".equals(data.getBehavior()));
        //.keyBy(data->)

        env.execute("market_channel");
    }
}
