package com.shuanghe.networkanalysis;

import com.shuanghe.networkanalysis.aggregate.PvCountAgg;
import com.shuanghe.networkanalysis.map.Data6ParserMapFunc;
import com.shuanghe.networkanalysis.map.MyMapper;
import com.shuanghe.networkanalysis.model.PvCount;
import com.shuanghe.networkanalysis.model.PvKeyByModel;
import com.shuanghe.networkanalysis.model.RawData6Event;
import com.shuanghe.networkanalysis.process.PvTotalCountProcess;
import com.shuanghe.networkanalysis.window.PvCountWindowResult;
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
        inputStream.print("input");

        DataStream<PvCount> pvStream = inputStream
                .filter(data -> data.getBehavior() != null)
                //定义一个pv字符串作为分组的 dummy key
                .map(new MyMapper())
                .keyBy(PvKeyByModel::getKey)
                .timeWindow(Time.hours(1))
                .aggregate(new PvCountAgg(), new PvCountWindowResult());
        pvStream.print("pv");

        SingleOutputStreamOperator<PvCount> totalPvStream = pvStream
                .keyBy(PvCount::getWindowEnd)
                //.reduce(new PvTotalCountAgg())
                //reduce每来一条数据计算输出一次
                .process(new PvTotalCountProcess());
        totalPvStream.print("total_result");

        env.execute("pv_job");
    }
}
