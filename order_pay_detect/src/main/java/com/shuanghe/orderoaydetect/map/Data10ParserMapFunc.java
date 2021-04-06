package com.shuanghe.orderoaydetect.map;

import com.alibaba.fastjson.JSONObject;
import com.shuanghe.orderoaydetect.model.ReceiptEvent;
import com.shuanghe.orderoaydetect.util.SimpleDataFormatter;
import com.shuanghe.orderoaydetect.util.StringUtilsPlus;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * @author yushu
 */
public class Data10ParserMapFunc implements FlatMapFunction<String, ReceiptEvent> {
    @Override
    public void flatMap(String value, Collector<ReceiptEvent> out) throws Exception {
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
        String packageName = String.valueOf(map.get("package_name"));
        long timestamp = 0;
        try {
            timestamp = Long.parseLong(String.valueOf(map.get("ts")));
        } catch (Exception ignored) {
        }

        out.collect(new ReceiptEvent(uid, packageName, timestamp));
    }
}
