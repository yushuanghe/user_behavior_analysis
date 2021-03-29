package com.shuanghe.network.analysis;

import com.shuanghe.network.analysis.aggregate.PvCountAgg;
import com.shuanghe.network.analysis.map.Data6ParserMapFunc;
import com.shuanghe.network.analysis.map.MyMapper;
import com.shuanghe.network.analysis.model.PvCount;
import com.shuanghe.network.analysis.model.PvKeyByModel;
import com.shuanghe.network.analysis.model.RawData6Event;
import com.shuanghe.network.analysis.process.PvTotalCountProcess;
import com.shuanghe.network.analysis.window.PvCountWindowResult;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.net.URL;

/**
 * @author yushu
 */
public class PageView {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //env.setParallelism(1);

        URL resource = PageView.class.getResource("/data-6.log");
        System.out.println(resource);

        DataStream<RawData6Event> inputStream = env.readTextFile(resource.getPath())
                .flatMap(new Data6ParserMapFunc())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<RawData6Event>(Time.milliseconds(1)) {
                    @Override
                    public long extractTimestamp(RawData6Event element) {
                        return element.getTimestamp();
                    }
                });
        inputStream.print();

        DataStream<PvCount> pvStream = inputStream
                .filter(data -> data.getBehavior() != null)
                //定义一个pv字符串作为分组的 dummy key
                .map(new MyMapper())
                .keyBy(PvKeyByModel::getKey)
                .timeWindow(Time.hours(1))
                .aggregate(new PvCountAgg(), new PvCountWindowResult());
        pvStream.print();

        SingleOutputStreamOperator<PvCount> totalPvStream = pvStream
                .keyBy(PvCount::getWindowEnd)
                //.reduce(new PvTotalCountAgg())
                //reduce每来一条数据计算输出一次
                .process(new PvTotalCountProcess());
        totalPvStream.print();

        env.execute("pg job");
    }
}
