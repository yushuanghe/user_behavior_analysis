package com.shuanghe.orderoaydetect;

import com.shuanghe.orderoaydetect.cep.MyBeginCondition;
import com.shuanghe.orderoaydetect.cep.MyFollowedCondition;
import com.shuanghe.orderoaydetect.cep.OrderPaySelect;
import com.shuanghe.orderoaydetect.cep.OrderTimeoutSelect;
import com.shuanghe.orderoaydetect.constant.KafkaConstant;
import com.shuanghe.orderoaydetect.map.Data6ParserMapFunc;
import com.shuanghe.orderoaydetect.model.OrderEvent;
import com.shuanghe.orderoaydetect.model.OrderResult;
import com.shuanghe.orderoaydetect.util.ConfigurationManager;
import com.shuanghe.orderoaydetect.util.KafkaUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.util.Properties;

/**
 * CEP基于watermark处理乱序数据，需要watermark涨到时间点才会处理
 *
 * @author yushu
 */
public class OrderTimeout {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        ParameterTool kafkaConfig = ConfigurationManager.getFlinkConfig(KafkaConstant.KAFKA_PROP_NAME);

        //从kafka读数据
        String kafkaGroupId = kafkaConfig.get(KafkaConstant.KAFKA_GROUP_ID);
        String kafkaTopic = kafkaConfig.get(KafkaConstant.KAFKA_TOPIC_NAME);
        Properties prop = KafkaUtils.getKafkaProp(kafkaConfig, kafkaGroupId);

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(kafkaTopic, new SimpleStringSchema(), prop);
        KafkaUtils.setKafkaOffset(kafkaConsumer, kafkaConfig);

        //从文件中读取数据
        //URL resource = OrderTimeout.class.getResource("/data-6.log");
        //KeyedStream<OrderEvent, String> orderEventDataStream = env.readTextFile(resource.getPath())
        KeyedStream<OrderEvent, String> orderEventDataStream = env.addSource(kafkaConsumer)
                .flatMap(new Data6ParserMapFunc())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderEvent>(Time.milliseconds(1)) {
                    @Override
                    public long extractTimestamp(OrderEvent element) {
                        return element.getTimestamp();
                    }
                })
                .keyBy(OrderEvent::getOrderId);

        //1、定义一个pattern
        Pattern<OrderEvent, OrderEvent> pattern = Pattern
                .<OrderEvent>begin("create").<OrderEvent>where(new MyBeginCondition())
                .<OrderEvent>followedBy("pay").<OrderEvent>where(new MyFollowedCondition())
                .within(Time.seconds(5));

        //2、将pattern应用到数据流上，进行模式匹配
        PatternStream<OrderEvent> patternStream = CEP.pattern(orderEventDataStream, pattern);

        //3、定义侧输出流标签，处理超时事件
        OutputTag<OrderResult> outputTag = new OutputTag<OrderResult>("orderTimeout") {
        };

        //4、调用select方法，提取并处理匹配的成功支付事件以及超时事件
        SingleOutputStreamOperator<OrderResult> resultDataStream = patternStream.select(outputTag, new OrderTimeoutSelect(), new OrderPaySelect());
        resultDataStream.print("result");

        resultDataStream.getSideOutput(outputTag).print("late");

        env.execute("order_timeout");
    }
}
