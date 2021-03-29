package com.shuanghe.network.analysis.util;

import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Description:日期时间工具类
 * <p>
 * Date: 2018/09/27
 * Time: 16:57
 *
 * @author Shuanghe Yu
 */
public class DateUtils {
    private static final Logger logger = Logger.getLogger(DateUtils.class);

    public static final String TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String DATE_FORMAT = "yyyy-MM-dd";
    public static final String DATEKEY_FORMAT = "yyyyMMdd";
    public static final String MINUTE_FORMAT = "yyyyMMddHHmm";
    public static final String SHORT_HOUR_FORMAT = "HH:mm";

    /**
     * 锁对象
     */
    private static final Object LOCK_OBJ = new Object();

    /**
     * 存放不同的日期模板格式的sdf的Map
     */
    private static final Map<String, ThreadLocal<SimpleDateFormat>> sdfMap = new HashMap<>();

    /**
     * 返回一个ThreadLocal的sdf,每个线程只会new一次sdf
     *
     * @param pattern
     * @return
     */
    private static SimpleDateFormat getSdf(final String pattern) {
        ThreadLocal<SimpleDateFormat> tl = sdfMap.get(pattern);

        // 此处的双重判断和同步是为了防止sdfMap这个单例被多次put重复的sdf
        if (tl == null) {
            synchronized (LOCK_OBJ) {
                tl = sdfMap.get(pattern);
                if (tl == null) {
                    // 只有Map中还没有这个pattern的sdf才会生成新的sdf并放入map
                    logger.info("put new sdf of pattern " + pattern + " to map");

                    // 这里是关键,使用ThreadLocal<SimpleDateFormat>替代原来直接new SimpleDateFormat
                    tl = ThreadLocal.withInitial(() -> {
                        logger.info("thread: " + Thread.currentThread() + " init pattern: " + pattern);
                        return new SimpleDateFormat(pattern);
                    });
                    sdfMap.put(pattern, tl);
                }
            }
        }

        return tl.get();
    }

    /**
     * 是用ThreadLocal<SimpleDateFormat>来获取SimpleDateFormat,这样每个线程只会有一个SimpleDateFormat
     *
     * @param date
     * @param pattern
     * @return
     */
    public static String format(Date date, String pattern) {
        return getSdf(pattern).format(date);
    }

    public static Date parse(String dateStr, String pattern) throws ParseException {
        return getSdf(pattern).parse(dateStr);
    }

    /**
     * 判断一个时间是否在另一个时间之前
     *
     * @param time1 第一个时间
     * @param time2 第二个时间
     * @return 判断结果
     */
    public static boolean before(String time1, String time2) {
        try {
            Date dateTime1 = getSdf(TIME_FORMAT).parse(time1);
            Date dateTime2 = getSdf(TIME_FORMAT).parse(time2);

            if (dateTime1.before(dateTime2)) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 判断一个时间是否在另一个时间之后
     *
     * @param time1 第一个时间
     * @param time2 第二个时间
     * @return 判断结果
     */
    public static boolean after(String time1, String time2) {
        try {
            Date dateTime1 = getSdf(TIME_FORMAT).parse(time1);
            Date dateTime2 = getSdf(TIME_FORMAT).parse(time2);

            if (dateTime1.after(dateTime2)) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 计算时间差值（单位为秒）
     *
     * @param time1 时间1
     * @param time2 时间2
     * @return 差值
     */
    public static int minus(String time1, String time2) {
        try {
            Date datetime1 = getSdf(TIME_FORMAT).parse(time1);
            Date datetime2 = getSdf(TIME_FORMAT).parse(time2);

            long millisecond = datetime1.getTime() - datetime2.getTime();

            return Integer.parseInt(String.valueOf(millisecond / 1000));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * 获取年月日和小时
     *
     * @param datetime 时间（yyyy-MM-dd HH:mm:ss）
     * @return 结果
     */
    public static String getDateHour(String datetime) {
        String date = datetime.split(" ")[0];
        String hourMinuteSecond = datetime.split(" ")[1];
        String hour = hourMinuteSecond.split(":")[0];
        return date + "_" + hour;
    }

    /**
     * 获取当天日期（yyyy-MM-dd）
     *
     * @return 当天日期
     */
    public static String getTodayDate() {
        return getSdf(DATE_FORMAT).format(new Date());
    }

    /**
     * 获取昨天的日期（yyyy-MM-dd）
     *
     * @return 昨天的日期
     */
    public static String getYesterdayDate() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(Calendar.DAY_OF_YEAR, -1);

        Date date = cal.getTime();

        return getSdf(DATE_FORMAT).format(date);
    }

    /**
     * 获取n天前的日期
     *
     * @param n
     * @return yyyy-MM-dd
     */
    public static String getDaysAgoDate(int n) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(Calendar.DAY_OF_YEAR, -n);

        Date date = cal.getTime();

        return getSdf(DATE_FORMAT).format(date);
    }

    /**
     * 获得指定日期的前一天
     *
     * @param ds yyyy-MM-dd
     * @return
     */
    public static String getSpecifiedDayBefore(String ds) {
        //可以用new Date().toLocalString()传递参数
        Calendar c = Calendar.getInstance();
        Date date = null;
        try {
            date = getSdf(DATE_FORMAT).parse(ds);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        c.setTime(date);
        int day = c.get(Calendar.DATE);
        c.set(Calendar.DATE, day - 1);

        String dayBefore = formatDate(c.getTime());
        return dayBefore;
    }

    /**
     * 获得指定日期的后一天
     *
     * @param ds yyyy-MM-dd
     * @return
     */
    public static String getSpecifiedDayAfter(String ds) {
        Calendar c = Calendar.getInstance();
        Date date = null;
        try {
            date = getSdf(DATE_FORMAT).parse(ds);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        c.setTime(date);
        int day = c.get(Calendar.DATE);
        c.set(Calendar.DATE, day + 1);

        String dayAfter = formatDate(c.getTime());
        return dayAfter;
    }

    /**
     * 获得指定日期的后n天
     *
     * @param ds yyyy-MM-dd
     * @return
     */
    public static String getSpecifiedDayAfter(String ds, int n) {
        Calendar c = Calendar.getInstance();
        Date date = null;
        try {
            date = getSdf(DATE_FORMAT).parse(ds);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        c.setTime(date);
        int day = c.get(Calendar.DATE);
        c.set(Calendar.DATE, day + n);

        String dayAfter = formatDate(c.getTime());
        return dayAfter;
    }

    /**
     * 格式化日期（yyyy-MM-dd）
     *
     * @param date Date对象
     * @return 格式化后的日期
     */
    public static String formatDate(Date date) {
        return getSdf(DATE_FORMAT).format(date);
    }

    /**
     * 格式化时间（yyyy-MM-dd HH:mm:ss）
     *
     * @param date Date对象
     * @return 格式化后的时间
     */
    public static String formatTime(Date date) {
        return getSdf(TIME_FORMAT).format(date);
    }

    /**
     * 根据时间字符串返回date
     *
     * @param date
     * @return
     */
    public static Date parseTime(String date) {
        try {
            return getSdf(TIME_FORMAT).parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 格式化日期key
     *
     * @param date
     * @return
     */
    public static String formatDateKey(Date date) {
        return getSdf(DATEKEY_FORMAT).format(date);
    }

    /**
     * 格式化日期key
     *
     * @param datekey
     * @return
     */
    public static Date parseDateKey(String datekey) {
        try {
            return getSdf(DATEKEY_FORMAT).parse(datekey);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 格式化时间，保留到分钟级别
     * yyyyMMddHHmm
     *
     * @param date
     * @return
     */
    public static String formatTimeMinute(Date date) {
        return getSdf(MINUTE_FORMAT).format(date);
    }

    /**
     * 获取当前时间
     *
     * @return HH:mm
     */
    public static String getCurrentShortHour() {
        return getSdf(SHORT_HOUR_FORMAT).format(new Date());
    }

    public static long getTs(String date) throws ParseException {
        return getSdf(TIME_FORMAT).parse(date).getTime();
    }
}