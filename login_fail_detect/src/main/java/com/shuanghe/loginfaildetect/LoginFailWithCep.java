package com.shuanghe.loginfaildetect;

import com.shuanghe.loginfaildetect.cep.LoginFailEventMatchCep;
import com.shuanghe.loginfaildetect.cep.MyLoadConditionCep;
import com.shuanghe.loginfaildetect.map.Data6ParserMapFunc;
import com.shuanghe.loginfaildetect.model.LoginEvent;
import com.shuanghe.loginfaildetect.model.LoginFailWarning;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.net.URL;

/**
 * @author yushu
 */
public class LoginFailWithCep {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        URL resource = LoginFailWithCep.class.getResource("/data-6.log");
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
        /*
        1、定义一个匹配的模式
        要求是一个load事件后，紧跟另一个load事件
         */
        Pattern<LoginEvent, LoginEvent> loginFailPattern = Pattern
                .<LoginEvent>begin("load1").where(new MyLoadConditionCep())
                //next 紧跟着
                .next("load2").where(new MyLoadConditionCep())
                //连续三次load
                .next("load3").where(new MyLoadConditionCep())
                //时间约束
                .within(Time.seconds(2L));

        /*
        2、将模式应用到数据流上，等到一个 PatternStream
         */
        PatternStream<LoginEvent> patternStream = CEP.pattern(inputStream.keyBy(LoginEvent::getUid), loginFailPattern);

        /*
        3、检出符合模式的数据流，调用 select 方法
         */
        DataStream<LoginFailWarning> loginFailWarningDataStream = patternStream.select(new LoginFailEventMatchCep());
        loginFailWarningDataStream.print("result");

        env.execute("login_fail_cep");
    }
}
