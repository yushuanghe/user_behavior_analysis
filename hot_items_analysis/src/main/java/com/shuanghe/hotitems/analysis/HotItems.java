package com.shuanghe.hotitems.analysis;

import com.shuanghe.hotitems.analysis.aggregate.CountAggFunc;
import com.shuanghe.hotitems.analysis.constant.KafkaConstant;
import com.shuanghe.hotitems.analysis.map.Data6ParserMapFunc;
import com.shuanghe.hotitems.analysis.model.ItemViewCountEvent;
import com.shuanghe.hotitems.analysis.model.RawData6Event;
import com.shuanghe.hotitems.analysis.process.TopNHotItems;
import com.shuanghe.hotitems.analysis.util.ConfigurationManager;
import com.shuanghe.hotitems.analysis.util.KafkaUtils;
import com.shuanghe.hotitems.analysis.window.ItemCountViewWindowResult;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author yushu
 */
public class HotItems {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        ParameterTool kafkaConfig = ConfigurationManager.getFlinkConfig(KafkaConstant.KAFKA_PROP_NAME);

        //从kafka读数据
        String kafkaGroupId = kafkaConfig.get(KafkaConstant.KAFKA_GROUP_ID);
        String kafkaTopic = kafkaConfig.get(KafkaConstant.KAFKA_TOPIC_NAME);
        Properties prop = KafkaUtils.getKafkaProp(kafkaConfig, kafkaGroupId);

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(kafkaTopic, new SimpleStringSchema(), prop);
        KafkaUtils.setKafkaOffset(kafkaConsumer, kafkaConfig);

        //从文件中读取数据
        //DataStream<RawData6Event> inputStream = env.readTextFile("C:\\Users\\yushu\\studyspace\\user_behavior_analysis\\hot_items_analysis\\src\\main\\resources\\data-6.log")
        DataStream<RawData6Event> inputStream = env.addSource(kafkaConsumer)
                .flatMap(new Data6ParserMapFunc())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<RawData6Event>(Time.milliseconds(1)) {
                    @Override
                    public long extractTimestamp(RawData6Event element) {
                        return element.getTimestamp();
                    }
                });
        inputStream.print();

        DataStream<ItemViewCountEvent> aggStream = inputStream
                .filter(data -> "start".equals(data.getBehavior()))
                .keyBy(RawData6Event::getAppId)
                //.timeWindow(Time.hours(1), Time.minutes(5))
                .timeWindow(Time.hours(1), Time.seconds(1))
                //需要窗口信息，所以加上 WindowFunction
                .aggregate(new CountAggFunc(), new ItemCountViewWindowResult());
        aggStream.print();

        DataStream<String> resultStream = aggStream
                //按照窗口分组，收集当前窗口内的商品count数据
                .keyBy(ItemViewCountEvent::getWindowEnd)
                .process(new TopNHotItems(5));
        resultStream.print();

        env.execute("hot_items");
    }
}
