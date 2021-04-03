package com.shuanghe.loginfaildetect;

import com.shuanghe.loginfaildetect.map.Data6ParserMapFunc;
import com.shuanghe.loginfaildetect.model.LoginEvent;
import com.shuanghe.loginfaildetect.model.LoginFailWarning;
import com.shuanghe.loginfaildetect.process.LoginFailWarningResult;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.net.URL;

/**
 * 基于定时器实现
 * 有bug
 * 1、时效性低，定时器时间未到不触发报警
 * 2、已达到报警条件，后面来正确数据后清空状态，删除定时器，不报警
 *
 * @author yushu
 */
public class LoginFail {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        URL resource = LoginFail.class.getResource("/data-6.log");
        DataStream<LoginEvent> inputStream = env.readTextFile(resource.getPath())
                .flatMap(new Data6ParserMapFunc())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.milliseconds(1)) {
                    @Override
                    public long extractTimestamp(LoginEvent element) {
                        return element.getTimestamp();
                    }
                });
        //inputStream.print();

        //两秒之内，连续load，输出报警信息
        DataStream<LoginFailWarning> loginFailWarningDataStream = inputStream
                .keyBy(LoginEvent::getUid)
                .process(new LoginFailWarningResult(2, 2));
        loginFailWarningDataStream.print("result");

        env.execute("login_fail");
    }
}
