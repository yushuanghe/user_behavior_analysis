package com.shuanghe.marketanalysis;

import com.shuanghe.marketanalysis.aggregate.AdCountAgg;
import com.shuanghe.marketanalysis.keyby.FilterBlackListKeySelector;
import com.shuanghe.marketanalysis.map.Data6ParserMapFunc;
import com.shuanghe.marketanalysis.model.AdClickCountByPlacement;
import com.shuanghe.marketanalysis.model.AdClickLog;
import com.shuanghe.marketanalysis.model.BlackListUserWarning;
import com.shuanghe.marketanalysis.process.FilterBlackListUserResult;
import com.shuanghe.marketanalysis.window.AdCountWindowResult;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.net.URL;

/**
 * @author yushu
 */
public class AdClickAnalysis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        URL url = AdClickAnalysis.class.getResource("/data-6.log");
        DataStream<AdClickLog> inputStream = env.readTextFile(url.getPath())
                .flatMap(new Data6ParserMapFunc())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<AdClickLog>(Time.milliseconds(1)) {
                    @Override
                    public long extractTimestamp(AdClickLog element) {
                        return element.getTimestamp();
                    }
                });
        //inputStream.print();

        //过滤操作，将黑名单用户报警
        SingleOutputStreamOperator<AdClickLog> filterStream = inputStream
                .keyBy(new FilterBlackListKeySelector())
                .process(new FilterBlackListUserResult(3));

        DataStream<AdClickCountByPlacement> resultStream = filterStream
                .keyBy(AdClickLog::getAppId)
                .timeWindow(Time.days(1), Time.seconds(10))
                .aggregate(new AdCountAgg(), new AdCountWindowResult());
        //resultStream.print("result");

        DataStream<BlackListUserWarning> blackListStream = filterStream
                .getSideOutput(new OutputTag<BlackListUserWarning>("warning") {
                });
        blackListStream.print("warning");

        env.execute("ad_click");
    }
}
