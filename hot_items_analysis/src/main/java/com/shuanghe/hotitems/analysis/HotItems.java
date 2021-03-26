package com.shuanghe.hotitems.analysis;

import com.shuanghe.hotitems.analysis.map.Data6ParserMapFunc;
import com.shuanghe.hotitems.analysis.model.RawData6Event;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yushu
 */
public class HotItems {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //从文件中读取数据
        DataStream<RawData6Event> inputStream = env.readTextFile("C:\\Users\\yushu\\studyspace\\user_behavior_analysis\\hot_items_analysis\\src\\main\\resources\\data-6.log")
                .flatMap(new Data6ParserMapFunc());
        inputStream.print();

        env.execute("hot_items");
    }
}
