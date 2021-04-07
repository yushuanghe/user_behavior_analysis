package com.shuanghe.orderoaydetect;

import com.shuanghe.orderoaydetect.map.Data6ParserMapFunc;
import com.shuanghe.orderoaydetect.model.OrderEvent;
import com.shuanghe.orderoaydetect.model.OrderResult;
import com.shuanghe.orderoaydetect.process.OrderPayMatchResult;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.net.URL;

/**
 * Description:
 * Date: 2021-04-06
 * Time: 18:10
 *
 * @author yushu
 */
public class OrderTimeoutWithoutCep {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //从文件中读取数据
        URL resource = OrderTimeoutWithoutCep.class.getResource("/data-6.log");
        KeyedStream<OrderEvent, String> orderEventDataStream = env.readTextFile(resource.getPath())
                .flatMap(new Data6ParserMapFunc())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderEvent>(Time.milliseconds(1)) {
                    @Override
                    public long extractTimestamp(OrderEvent element) {
                        return element.getTimestamp();
                    }
                })
                .keyBy(OrderEvent::getOrderId);

        //自定义 KeyedProcessFunction 实现复杂事件的检测
        OutputTag<OrderResult> outputTag = new OutputTag<OrderResult>("timeout") {
        };
        SingleOutputStreamOperator<OrderResult> orderResultStream = orderEventDataStream
                .process(new OrderPayMatchResult(outputTag, 1 * 1000L));

        orderResultStream.print("result");
        orderResultStream.getSideOutput(outputTag).print("late");

        env.execute("order_timeout_without_cep");
    }
}
