package com.shuanghe.hotitemsanalysis.util;

import com.shuanghe.hotitemsanalysis.constant.CommonConstant;

/**
 * Description:
 * Date: 2021-03-22
 * Time: 11:23
 *
 * @author yushu
 */
public class DateFormat {
    public static final String DATE_FORMAT = "yyyy-MM-dd";

    public static String fDayFormat(String dateString) {
        if (StringUtilsPlus.isBlank(dateString)) {
            return CommonConstant.DEFAULT_DATA_DATE;
        }
        dateString = dateString.trim();
        if (dateString.length() < 10) {
            return CommonConstant.DEFAULT_DATA_DATE;
        }
        return dateString.trim().substring(0, 10);
    }
}
