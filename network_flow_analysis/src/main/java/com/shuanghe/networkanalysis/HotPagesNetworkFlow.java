package com.shuanghe.networkanalysis;

import com.shuanghe.networkanalysis.aggregate.PageCountAgg;
import com.shuanghe.networkanalysis.constant.KafkaConstant;
import com.shuanghe.networkanalysis.model.ApacheLogEvent;
import com.shuanghe.networkanalysis.model.UrlViewCount;
import com.shuanghe.networkanalysis.process.TopNHotPages;
import com.shuanghe.networkanalysis.util.ConfigurationManager;
import com.shuanghe.networkanalysis.util.DateFormat;
import com.shuanghe.networkanalysis.util.KafkaUtils;
import com.shuanghe.networkanalysis.window.PageViewCountWindowResult;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.OutputTag;

import java.util.Properties;

/**
 * @author yushu
 */
public class HotPagesNetworkFlow {
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
        /*
        乱序数据三重保障
        watermark
        窗口延迟关闭
        侧输出流
         */
        OutputTag<ApacheLogEvent> outputTag = new OutputTag<ApacheLogEvent>("late") {
        };
        //DataStream<ApacheLogEvent> inputStream = env.readTextFile("network_flow_analysis/src/main/resources/apache_access.log")
        DataStream<ApacheLogEvent> inputStream = env.addSource(kafkaConsumer)
                .map(data -> {
                    String[] strings = data.split(" ");
                    return new ApacheLogEvent(strings[0], strings[1], DateFormat.formatTs(strings[3]), strings[5].substring(1), strings[6]);
                })
                //乱序数据处理1
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(ApacheLogEvent element) {
                        return element.getTimestamp();
                    }
                });
        inputStream.print();

        SingleOutputStreamOperator<UrlViewCount> aggStream = inputStream
                .filter(data -> "GET".equals(data.getMethod()))
                .keyBy(ApacheLogEvent::getUrl)
                .timeWindow(Time.minutes(10), Time.seconds(5))
                //乱序数据处理2
                //处理不延迟，窗口正常触发聚合操作，窗口延迟关闭，晚来数据继续累加输出
                //延迟关闭窗口，数据进来直接聚合处理输出
                .allowedLateness(Time.minutes(1))
                //乱序数据处理3
                .sideOutputLateData(outputTag)
                .aggregate(new PageCountAgg(), new PageViewCountWindowResult());
        aggStream.print("agg");

        //侧输出流接收 数据所在所有窗口都关闭的数据
        aggStream.getSideOutput(outputTag).print("late");

        DataStream<String> resultStream = aggStream
                .keyBy(UrlViewCount::getWindowEnd)
                .process(new TopNHotPages(3));
        resultStream.print("result");

        env.execute("hot_pages");
    }
}
