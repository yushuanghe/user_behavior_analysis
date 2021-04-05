package com.shuanghe.orderoaydetect.util;

import com.shuanghe.orderoaydetect.constant.CommonConstant;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

/**
 * @author yushu
 */
public class DateFormat {
    public static long formatTs(String dateStr) throws ParseException {
        //[21/Jul/2014:10:00:00
        return new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss", Locale.US).parse(dateStr).getTime();
    }

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
