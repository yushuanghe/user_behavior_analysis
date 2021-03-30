package com.shuanghe.network.analysis;

import com.shuanghe.network.analysis.map.Data6ParserMapFunc;
import com.shuanghe.network.analysis.model.RawData6Event;
import com.shuanghe.network.analysis.model.UvCount;
import com.shuanghe.network.analysis.window.UvCountResult;
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
 * Time: 20:43
 *
 * @author yushu
 */
public class UniqueVisitor {
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
        inputStream.print("input");

        SingleOutputStreamOperator<UvCount> uvStream = inputStream
                .filter(data -> data.getBehavior() != null)
                //不分组，基于 DataStream 开1小时滚动窗口
                .timeWindowAll(Time.hours(1))
                .apply(new UvCountResult());
        uvStream.print("uv");

        env.execute("uv_job");
    }
}
