package com.shuanghe.hotitems.analysis;

import com.shuanghe.hotitems.analysis.aggregate.CountAggFunc;
import com.shuanghe.hotitems.analysis.map.Data6ParserMapFunc;
import com.shuanghe.hotitems.analysis.model.ItemViewCountEvent;
import com.shuanghe.hotitems.analysis.model.RawData6Event;
import com.shuanghe.hotitems.analysis.window.ItemViewWindowResult;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author yushu
 */
public class HotItems {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //从文件中读取数据
        DataStream<RawData6Event> inputStream = env.readTextFile("C:\\Users\\yushu\\studyspace\\user_behavior_analysis\\hot_items_analysis\\src\\main\\resources\\data-6.log")
                .flatMap(new Data6ParserMapFunc())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<RawData6Event>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(RawData6Event element) {
                        return element.getTimestamp();
                    }
                });
        inputStream.print();

        DataStream<ItemViewCountEvent> aggStream = inputStream.filter(data -> "start".equals(data.getBehavior()))
                .keyBy(data -> data.getAppId())
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new CountAggFunc(), new ItemViewWindowResult());

        env.execute("hot_items");
    }
}
