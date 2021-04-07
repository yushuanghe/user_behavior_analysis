package com.shuanghe.orderoaydetect;

import com.shuanghe.orderoaydetect.map.Data10ParserMapFunc;
import com.shuanghe.orderoaydetect.map.Data6ParserMapFunc;
import com.shuanghe.orderoaydetect.model.OrderEvent;
import com.shuanghe.orderoaydetect.model.ReceiptEvent;
import com.shuanghe.orderoaydetect.process.TxMatchWithJoinResult;
import com.shuanghe.orderoaydetect.util.StringUtilsPlus;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.net.URL;

/**
 * Description:
 * Date: 2021-04-07
 * Time: 20:42
 *
 * @author yushu
 */
public class TxMatchWithJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //从文件中读取数据
        URL resource6 = TxMatchWithJoin.class.getResource("/data-6.log");
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

        URL resource10 = TxMatchWithJoin.class.getResource("/data-10.log");
        KeyedStream<ReceiptEvent, String> receiptEventDataStream = env.readTextFile(resource10.getPath())
                .flatMap(new Data10ParserMapFunc())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ReceiptEvent>(Time.milliseconds(1)) {
                    @Override
                    public long extractTimestamp(ReceiptEvent element) {
                        return element.getTimestamp();
                    }
                })
                .keyBy(ReceiptEvent::getTxId);

        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> resultStream = orderEventDataStream
                //内联
                .intervalJoin(receiptEventDataStream)
                .between(Time.minutes(-3L), Time.minutes(5L))
                .process(new TxMatchWithJoinResult());
        resultStream.print("result");

        env.execute("tx_match_with_join");
    }
}
