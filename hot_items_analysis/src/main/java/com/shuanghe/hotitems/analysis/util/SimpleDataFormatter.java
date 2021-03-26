package com.shuanghe.hotitems.analysis.util;

import org.apache.log4j.Logger;

import java.text.ParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Description:解析dc原始日志，添加ts字段
 * Date: 2021-02-24
 * Time: 17:27
 *
 * @author yushu
 */
public class SimpleDataFormatter {
    private static final Logger LOG = Logger.getLogger(SimpleDataFormatter.class);
    //private static final Log LOG = LogFactory.getLog(SimpleDataFormatter.class);

    protected final static Pattern DATA_PATTERN = Pattern.compile("^([0-9a-zA-Z]+_[0-9a-zA-Z]+)\t([^\t]+?)\t([0-9]+)\t([^\t]+?)\t(20\\d{2}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})$");

    protected static final String TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static String[] intFields = {"bid_price", "advertiser_price", "price", "request", "response", "channelId", "acType"};
    private static String[] longFields = {"timestamp"};
    /**
     * 维度+度量 所有字段
     */
    private static Set<String> requestfieldsSet = new HashSet<>();
    /**
     * 度量
     */
    private static Set<String> requestfieldsMeasureIntSet = new HashSet<>();

    static {
        //requestfieldsSet = new HashSet<String>(
        //        Arrays.asList(Constant.REQUEST_USED_FILEDS.split(Constant.ARRAY_SPLIT_KEY)));
        //requestfieldsMeasureIntSet = new HashSet<String>(
        //        Arrays.asList(Constant.REQUEST_USED_FILEDS_MEASURE_INT.split(Constant.ARRAY_SPLIT_KEY)));
        //LOG.info(DcType.request + " 中的字段，不在此列表的字段会被过滤:" + requestfieldsSet);
    }

    /**
     * 解析dc数据
     *
     * @param source
     * @return
     */
    public static Map<String, Object> format(String source) {
        Map<String, Object> payload = null;
        Matcher matcher = DATA_PATTERN.matcher(source);
        if (matcher.matches()) {
            payload = new HashMap<>(64);
            String appId = matcher.group(1);
            String gid = matcher.group(2);
            String type = matcher.group(3);
            String payloadStr = matcher.group(4);
            String dateString = matcher.group(5);

            String[] items = payloadStr.split("\\|");
            for (String value : items) {
                if (value == null) {
                    continue;
                }
                String[] item = value.split("~", 2);
                if (null == item || item.length != 2) {
                    continue;
                }
                //// 如果是3号点 只收录 REQUEST_USED_FILEDS 中的字段
                //if (type.equals(DcType.request.getEventValue()) && !requestfieldsSet.contains(item[0])) {
                //    continue;
                //}
                // 度量 - int
                if (requestfieldsMeasureIntSet.contains(item[0])) {
                    try {
                        payload.put(item[0], Integer.parseInt(item[1]));
                    } catch (Exception e) {
                    }
                }
                // 维度
                else {
                    payload.put(item[0], item[1]);
                }
            }
            payload.put("gid", gid);
            payload.put("dcAppId", appId);
            payload.put("acType", type);
            long ts = 0;
            try {
                ts = DateUtils.getTs(dateString);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            payload.put("ts", ts);
            String fDay = DateFormat.fDayFormat(dateString);
            //String fhour = DateFormat.fhourFromat(dateString);
            payload.put("fDay", fDay);
            //payload.put("fhour", fhour);

            String appidPair[] = appId.split("\\_");
            payload.put("ssp", appidPair[0]);
            // appid channelId appId三个字段值理论上是一样的
            payload.put("appid", appidPair[1]);
            payload.put("channelId", appidPair[1]);
            payload.put("appId", payload.get("adver_app_id"));
            for (String value : intFields) {
                if (payload.containsKey(value)) {
                    //payload.put(value, CastUtils.parseInt(payload.get(value)));
                }
            }


            //// 处理3号点
            //if (type.equals(DcType.request.getEventValue())) {
            //    // 3号点里面缺少event字段，request作为补全event字段
            //    payload.put("event", Event.request.name());
            //
            //    // 将3号点buctet_type（3号点书写名称错误）改成bucket_type
            //    if (!payload.containsKey("bucket_type") && payload.containsKey("buctet_type")) {
            //        payload.put("bucket_type", payload.get("buctet_type"));
            //    }
            //    payload.remove("buctet_type");
            //}

            //// 8号点 添加主副广告类型
            //if (DcType.track.getEventValue().equals(type)) {
            //    // 设置主副广告的分类维度。（参数 s=closecard 代表副广告，无s参数代表主广告）（family_type=1:主广告；family_type=2：副广告）
            //    if ("closecard".equals(payload.get("s"))) {
            //        payload.put("family_type", "2");
            //    } else {
            //        payload.put("family_type", "1");
            //    }
            //} else {
            //    payload.put("family_type", "1");
            //}
            //payload.remove("s");

            //// 处理8号点event
            //if (DcType.track.getEventValue().equals(type) && payload.containsKey("event")) {
            //    // 伴随click，实时计算时候合并到click。
            //    // 旧click打点规则，包含click、companion_click、demo_game_click，将这些按照新规则拆分成event\click_area\click_scene\auto_click
            //    // companion_click统一规划至click，并且click_scene补全为companion
            //	/*if (Event.companion_click.name().equals(payload.get("event"))) {
            //		payload.put("event", Event.click.name());
            //		payload.put("click_area", "");
            //		payload.put("click_scene", "companion_click");
            //		payload.put("auto_click", "");
            //	}
            //	if (Event.demo_game_click.name().equals(payload.get("event"))) {
            //		payload.put("event", Event.click.name());
            //		payload.put("click_area", "");
            //		payload.put("click_scene", "demo_game_click");
            //		payload.put("auto_click", "");
            //	}
            //	if (Event.full_video_click.name().equals(payload.get("event"))) {
            //		payload.put("event", Event.click.name());
            //		payload.put("click_area", "");
            //		payload.put("click_scene", "full_video_click");
            //		payload.put("auto_click", "");
            //	}
            //	// click的click_scene补全为endcard
            //	if (Event.click.name().equals(payload.get("event"))) {
            //		if (payload.get("click_area") == null) {
            //			payload.put("click_area", "");
            //		}
            //		if (payload.get("click_scene") == null) {
            //			payload.put("click_scene", "endcard_click");
            //		}
            //		if (payload.get("auto_click") == null) {
            //			payload.put("auto_click", "");
            //		}
            //	}*/
            //
            //    // 8号点 : adtype=4则将show_skip合并到finish、adtype=2则将ad_close合并到finish
            //    if (payload.containsKey("adtype")) {
            //        if ((Adtype.full_video.getType().equals(payload.get("adtype")) && Event.show_skip.name().equals(payload.get("event"))) ||
            //                (Adtype.screen.getType().equals(payload.get("adtype")) && Event.ad_close.name().equals(payload.get("event")))
            //        ) {
            //            payload.put("event", Event.finish.name());
            //        }
            //    }
            //}

            //// 80号点里面缺少event字段，补全event字段。（如果是80号点，当key包含e和clickid（requestId），将e改为event）
            //if (DcType.pay.getEventValue().equals(type)) {
            //    if (payload.get("e") != null && payload.containsKey("clickid")) {
            //        payload.put("event", payload.get("e"));
            //        payload.remove("e");
            //    } else {
            //        return null;
            //    }
            //}

            //// 替换时间戳格式
            //for (String value : longFields) {
            //    if (payload.containsKey(value)) {
            //        payload.put(value, CastUtils.parseLong(payload.get(value)));
            //    }
            //}

        }
        return payload;

    }
}
