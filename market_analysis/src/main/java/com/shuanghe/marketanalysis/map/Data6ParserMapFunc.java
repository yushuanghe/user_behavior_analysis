package com.shuanghe.marketanalysis.map;

import com.alibaba.fastjson.JSONObject;
import com.shuanghe.marketanalysis.model.AdClickLog;
import com.shuanghe.marketanalysis.util.SimpleDataFormatter;
import com.shuanghe.marketanalysis.util.StringUtilsPlus;
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
public class Data6ParserMapFunc implements FlatMapFunction<String, AdClickLog> {
    @Override
    public void flatMap(String value, Collector<AdClickLog> out) throws Exception {
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
        int adType = 0;
        try {
            adType = Integer.parseInt(String.valueOf(map.get("adtype")));
        } catch (Exception ignored) {
        }
        String appId = String.valueOf(map.get("dcAppId"));
        String placementId = String.valueOf(map.get("placement_id"));
        long timestamp = 0;
        try {
            timestamp = Long.parseLong(String.valueOf(map.get("ts")));
        } catch (Exception ignored) {
        }

        out.collect(new AdClickLog(uid, adType, appId, placementId, timestamp));
    }
}
