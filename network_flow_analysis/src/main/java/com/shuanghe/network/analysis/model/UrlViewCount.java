package com.shuanghe.network.analysis.model;

import java.io.Serializable;

/**
 * 窗口聚合结果
 *
 * @author yushu
 */
public class UrlViewCount implements Serializable {
    private String url;
    private long windowEnd;
    private long count;

    public UrlViewCount(String url, long windowEnd, long count) {
        this.url = url;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "UrlViewCount{" +
                "url='" + url + '\'' +
                ", windowEnd=" + windowEnd +
                ", count=" + count +
                '}';
    }
}
