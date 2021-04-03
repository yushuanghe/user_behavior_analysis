package com.shuanghe.loginfaildetect.map;

import com.alibaba.fastjson.JSONObject;
import com.shuanghe.loginfaildetect.model.LoginEvent;
import com.shuanghe.loginfaildetect.util.SimpleDataFormatter;
import com.shuanghe.loginfaildetect.util.StringUtilsPlus;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * Description:
 * Date: 2021-03-25
 * Time: 20:52
 *
 * @author yushu
 */
public class Data6ParserMapFunc implements FlatMapFunction<String, LoginEvent> {
    @Override
    public void flatMap(String value, Collector<LoginEvent> out) throws Exception {
        if (StringUtilsPlus.isBlank(value)) {
            return;
        }

        JSONObject jsonObject = JSONObject.parseObject(value);
        String message = jsonObject.get("message").toString();
        Map<String, Object> map = SimpleDataFormatter.format(message);
        if (map == null) {
            return;
        }

        String uid = String.valueOf(map.get("uid"));
        String ip = String.valueOf(map.get("ip"));
        String event = String.valueOf(map.get("category"));
        long timestamp = 0;
        try {
            timestamp = Long.parseLong(String.valueOf(map.get("ts")));
        } catch (Exception ignored) {
        }

        out.collect(new LoginEvent(uid, ip, event, timestamp));
    }
}
