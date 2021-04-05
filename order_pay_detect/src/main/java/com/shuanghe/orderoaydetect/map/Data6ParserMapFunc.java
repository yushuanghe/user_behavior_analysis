package com.shuanghe.orderoaydetect.map;

import com.alibaba.fastjson.JSONObject;
import com.shuanghe.orderoaydetect.model.OrderEvent;
import com.shuanghe.orderoaydetect.util.SimpleDataFormatter;
import com.shuanghe.orderoaydetect.util.StringUtilsPlus;
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
public class Data6ParserMapFunc implements FlatMapFunction<String, OrderEvent> {
    @Override
    public void flatMap(String value, Collector<OrderEvent> out) throws Exception {
        if (StringUtilsPlus.isBlank(value)) {
            return;
        }

        JSONObject jsonObject = JSONObject.parseObject(value);
        String message = jsonObject.get("message").toString();
        Map<String, Object> map = SimpleDataFormatter.format(message);
        if (map == null) {
            return;
        }

        String requestId = String.valueOf(map.get("request_id"));
        String placementId = String.valueOf(map.get("placement_id"));
        String event = String.valueOf(map.get("category"));
        long timestamp = 0;
        try {
            timestamp = Long.parseLong(String.valueOf(map.get("ts")));
        } catch (Exception ignored) {
        }

        out.collect(new OrderEvent(requestId, event, placementId, timestamp));
    }
}
