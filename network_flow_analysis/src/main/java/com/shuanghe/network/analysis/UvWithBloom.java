package com.shuanghe.network.analysis;

import com.shuanghe.network.analysis.map.Data6ParserMapFunc;
import com.shuanghe.network.analysis.model.RawData6Event;
import com.shuanghe.network.analysis.model.UvCount;
import com.shuanghe.network.analysis.process.UvCountWithBloom;
import com.shuanghe.network.analysis.trigger.UvCountWithBloomTrigger;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.net.URL;

/**
 * Description:
 * Date: 2021-03-30
 * Time: 21:41
 *
 * @author yushu
 */
public class UvWithBloom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //env.setParallelism(1);

        URL resource = UniqueVisitor.class.getResource("/data-6.log");
        System.out.println(resource);

        DataStream<RawData6Event> inputStream = env.readTextFile(resource.getPath())
                .flatMap(new Data6ParserMapFunc())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<RawData6Event>(Time.milliseconds(1)) {
                    @Override
                    public long extractTimestamp(RawData6Event element) {
                        return element.getTimestamp();
                    }
                });
        //inputStream.print("input");

        SingleOutputStreamOperator<UvCount> uvStream = inputStream
                .filter(data -> data.getBehavior() != null)
                .map(data -> {
                    data.setAppId("uv");
                    return data;
                })
                .keyBy(RawData6Event::getAppId)
                .timeWindow(Time.hours(1))
                //自定义触发器
                .trigger(new UvCountWithBloomTrigger())
                .process(new UvCountWithBloom());
        uvStream.print("uv");

        env.execute("uv_bloom");
    }
}
