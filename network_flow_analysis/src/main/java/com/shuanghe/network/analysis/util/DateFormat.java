package com.shuanghe.network.analysis.util;

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
}
