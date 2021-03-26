package com.shuanghe.hotitems.analysis.model;

import java.io.Serializable;

/**
 * 定义窗口聚合结果
 *
 * @author yushu
 */
public class ItemViewCountEvent implements Serializable {
    private String appId;
    private long windowEnd;
    private long count;

    public ItemViewCountEvent(String appId, long windowEnd, long count) {
        this.appId = appId;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
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
}
