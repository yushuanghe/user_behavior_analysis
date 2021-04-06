package com.shuanghe.orderoaydetect;

import com.shuanghe.orderoaydetect.map.Data10ParserMapFunc;
import com.shuanghe.orderoaydetect.map.Data6ParserMapFunc;
import com.shuanghe.orderoaydetect.model.OrderEvent;
import com.shuanghe.orderoaydetect.model.ReceiptEvent;
import com.shuanghe.orderoaydetect.process.TxPayMatchResult;
import com.shuanghe.orderoaydetect.util.StringUtilsPlus;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.net.URL;

/**
 * @author yushu
 */
public class TxMatch {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //从文件中读取数据
        URL resource6 = OrderTimeout.class.getResource("/data-6.log");
        KeyedStream<OrderEvent, String> orderEventDataStream = env.readTextFile(resource6.getPath())
                .flatMap(new Data6ParserMapFunc())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderEvent>(Time.milliseconds(1)) {
                    @Override
                    public long extractTimestamp(OrderEvent element) {
                        return element.getTimestamp();
                    }
                })
                .filter(data -> "start".equals(data.getEventType()))
                .filter(data -> StringUtilsPlus.isNotBlank(data.getTxId()))
                .keyBy(OrderEvent::getTxId);
        //orderEventDataStream.print();

        URL resource10 = OrderTimeout.class.getResource("/data-10.log");
        KeyedStream<ReceiptEvent, String> receiptEventDataStream = env.readTextFile(resource10.getPath())
                .flatMap(new Data10ParserMapFunc())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ReceiptEvent>(Time.milliseconds(1)) {
                    @Override
                    public long extractTimestamp(ReceiptEvent element) {
                        return element.getTimestamp();
                    }
                })
                .keyBy(ReceiptEvent::getTxId);
        //receiptEventDataStream.print();

        /*
        合流
        KeyedStream connect 后也是 keyBy 的
         */
        OutputTag<OrderEvent> orderEventOutputTag = new OutputTag<OrderEvent>("order_event_late") {
        };
        OutputTag<ReceiptEvent> receiptEventOutputTag = new OutputTag<ReceiptEvent>("receipt_event_late") {
        };
        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> resultStream =
                orderEventDataStream.connect(receiptEventDataStream)
                        .process(new TxPayMatchResult(5 * 1000L, 3 * 1000L, orderEventOutputTag, receiptEventOutputTag));
        resultStream.print("result");
        resultStream.getSideOutput(orderEventOutputTag).print("order_late");
        resultStream.getSideOutput(receiptEventOutputTag).print("receipt_late");

        env.execute("tx_match");
    }
}
