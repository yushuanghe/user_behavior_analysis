package com.shuanghe.loginfaildetect.util;

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
        }
        return payload;

    }
}
