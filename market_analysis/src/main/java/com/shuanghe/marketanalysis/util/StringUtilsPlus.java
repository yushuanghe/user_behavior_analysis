package com.shuanghe.marketanalysis.util;

import org.apache.commons.lang3.StringUtils;

import java.util.*;

/**
 * Description:
 * Date: 2019-06-04
 * Time: 11:02
 *
 * @author yushuanghe
 */
public class StringUtilsPlus {
    private static final String[] NULL_STRS = {"-", "_", "null", "NULL"};
    /**
     * 左匹配
     */
    private static final String LEFT_MATCH_TYPE = "L";
    /**
     * 右匹配
     */
    private static final String RIGHT_MATCH_TYPE = "R";
    /**
     * 全匹配
     */
    private static final String FULL_MATCH_TYPE = "F";
    /**
     * 中间匹配
     */
    private static final String MIDDLE_MATCH_TYPE = "M";

    public static boolean isNotBlank(String str) {
        boolean result = false;
        if (str != null && StringUtils.isNotBlank(str.trim())) {
            str = str.trim();

            for (String nullValue : NULL_STRS) {
                if (str.equals(nullValue)) {
                    return false;
                }
            }
            result = true;
        }
        return result;
    }

    public static boolean isBlank(String str) {
        return !isNotBlank(str);
    }

    /**
     * 字符串根据分隔符排重
     *
     * @param text
     * @param separator
     * @param sb
     * @return
     */
    public static String distinctLine(String text, String separator, StringBuilderPlus sb) {
        String result = null;
        sb = sb.deleteAll();
        if (isBlank(text)) {
            return null;
        }
        String[] strs = text.split(separator);
        Set<String> set = new HashSet<>(Arrays.asList(strs));
        for (String str : set) {
            sb.append(str).append(separator);
        }
        sb = sb.deleteLastChar();
        result = sb.toString();
        return result;
    }

    /**
     * 字符串根据分隔符排重,排序
     *
     * @param text
     * @param separator
     * @param sb
     * @return
     */
    public static String sortLine(String text, String separator, StringBuilderPlus sb) {
        String result = null;
        sb = sb.deleteAll();
        if (isBlank(text)) {
            return null;
        }
        String[] strs = text.split(separator);
        List<Integer> list = new ArrayList<>();
        for (String str : strs) {
            list.add(Integer.valueOf(str));
        }
        Set<Integer> set = new TreeSet<>(list);

        for (int str : set) {
            sb.append(String.valueOf(str)).append(separator);
        }
        sb = sb.deleteLastChar();
        result = sb.toString();
        return result;
    }

    /**
     * 字符串根据分隔符排重,排序
     *
     * @param text
     * @param separator
     * @param sb
     * @return
     */
    public static String sortLineByString(String text, String separator, StringBuilderPlus sb) {
        String result = null;
        sb = sb.deleteAll();
        if (isBlank(text)) {
            return null;
        }
        String[] strs = text.split(separator);
        List<String> list = new ArrayList<>(Arrays.asList(strs));
        Set<String> set = new TreeSet<>(list);

        for (String str : set) {
            sb.append(String.valueOf(str)).append(separator);
        }
        sb = sb.deleteLastChar();
        result = sb.toString();
        return result;
    }

    /**
     * 取最大的string
     *
     * @param params
     * @return
     */
    public static String getMaxString(String[] params) {
        String result = null;
        for (String str : params) {
            if (result == null || str.compareTo(result) > 0) {
                result = str;
            }
        }
        return result;
    }

    /**
     * 判断文本中是否包含关键词
     *
     * @param line      文本
     * @param keyword   关键词
     * @param matchType 匹配类型
     * @return
     */
    public static boolean containsKeyword(String line, String keyword, String matchType) {
        boolean result = false;
        switch (matchType) {
            case LEFT_MATCH_TYPE:
                result = line.startsWith(keyword);
                break;
            case RIGHT_MATCH_TYPE:
                result = line.endsWith(keyword);
                break;
            case FULL_MATCH_TYPE:
                result = line.equals(keyword);
                break;
            case MIDDLE_MATCH_TYPE:
                result = line.contains(keyword);
                break;
            default:
                break;
        }
        return result;
    }

    /**
     * 把数字格式化成等长字符串
     * 1->001
     *
     * @param param  数字
     * @param length 字符串长度
     * @return
     */
    public static String formatIntStr(int param, int length) {
        int strLength = String.valueOf(param).length();
        if (strLength < length) {
            StringBuilderPlus sb = new StringBuilderPlus();
            for (int i = 1; i <= (length - strLength); i++) {
                sb.append("0");
            }
            return String.format("%s%s", sb.toString(), param);
        } else {
            return String.valueOf(param);
        }
    }
}
