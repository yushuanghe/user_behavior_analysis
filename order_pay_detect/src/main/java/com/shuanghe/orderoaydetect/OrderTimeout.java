package com.shuanghe.orderoaydetect;

import com.shuanghe.orderoaydetect.cep.MyBeginCondition;
import com.shuanghe.orderoaydetect.cep.MyFollowedCondition;
import com.shuanghe.orderoaydetect.map.Data6ParserMapFunc;
import com.shuanghe.orderoaydetect.model.OrderEvent;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.net.URL;

/**
 * @author yushu
 */
public class OrderTimeout {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        URL resource = OrderTimeout.class.getResource("/data-6.log");
        KeyedStream<OrderEvent, String> orderEventDataStream = env.readTextFile(resource.getPath())
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
                .within(Time.minutes(15));

        //2、将pattern应用到数据流上，进行模式匹配


        env.execute("order_timeout");
    }
}
