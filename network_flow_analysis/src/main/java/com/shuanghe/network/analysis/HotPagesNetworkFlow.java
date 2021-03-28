package com.shuanghe.network.analysis;

import com.shuanghe.network.analysis.aggregate.PageCountAgg;
import com.shuanghe.network.analysis.model.ApacheLogEvent;
import com.shuanghe.network.analysis.model.UrlViewCount;
import com.shuanghe.network.analysis.util.DateFormat;
import com.shuanghe.network.analysis.window.PageViewCountWindowResult;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author yushu
 */
public class HotPagesNetworkFlow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<ApacheLogEvent> inputStream = env.readTextFile("network_flow_analysis/src/main/resources/apache_access.log")
                .map(data -> {
                    String[] strings = data.split(" ");
                    return new ApacheLogEvent(strings[0], strings[1], DateFormat.formatTs(strings[3]), strings[5].substring(1), strings[6]);
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.minutes(1)) {
                    @Override
                    public long extractTimestamp(ApacheLogEvent element) {
                        return element.getTimestamp();
                    }
                });
        inputStream.print();

        DataStream<UrlViewCount> aggStream = inputStream
                .filter(data -> "GET".equals(data.getMethod()))
                .keyBy(ApacheLogEvent::getUrl)
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .aggregate(new PageCountAgg(), new PageViewCountWindowResult());
        aggStream.print("agg");

        env.execute("hot pages");
    }
}
