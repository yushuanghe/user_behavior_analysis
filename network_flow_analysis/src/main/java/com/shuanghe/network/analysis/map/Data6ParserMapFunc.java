package com.shuanghe.network.analysis.map;

import com.alibaba.fastjson.JSONObject;
import com.shuanghe.network.analysis.model.RawData6Event;
import com.shuanghe.network.analysis.util.SimpleDataFormatter;
import com.shuanghe.network.analysis.util.StringUtilsPlus;
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
public class Data6ParserMapFunc implements FlatMapFunction<String, RawData6Event> {
    @Override
    public void flatMap(String value, Collector<RawData6Event> out) throws Exception {
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
        String appId = String.valueOf(map.get("dcAppId"));
        int channelId = 0;
        try {
            channelId = Integer.parseInt(String.valueOf(map.get("aggr_channel_id")));
        } catch (Exception ignored) {
        }
        String behavior = String.valueOf(map.get("category"));
        long timestamp = 0;
        try {
            timestamp = Long.parseLong(String.valueOf(map.get("ts")));
        } catch (Exception ignored) {
        }

        out.collect(new RawData6Event(uid, appId, channelId, behavior, timestamp));
    }
}
