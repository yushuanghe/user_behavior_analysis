package com.shuanghe.marketanalysis.model;

import java.io.Serializable;

/**
 * @author yushu
 */
public class AdClickCountByPlacement implements Serializable {
    private String windowEnd;
    private String appId;
    private long count;

    public AdClickCountByPlacement(String windowEnd, String appId, long count) {
        this.windowEnd = windowEnd;
        this.appId = appId;
        this.count = count;
    }

    @Override
    public String toString() {
        return "AdClickCountByPlacement{" +
                "windowEnd='" + windowEnd + '\'' +
                ", appId='" + appId + '\'' +
                ", count=" + count +
                '}';
    }

    public String getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(String windowEnd) {
        this.windowEnd = windowEnd;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
