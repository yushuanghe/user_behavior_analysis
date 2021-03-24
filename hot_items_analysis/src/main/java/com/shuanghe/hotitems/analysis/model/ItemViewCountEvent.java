package com.shuanghe.hotitems.analysis.model;

import java.io.Serializable;

/**
 * 定义窗口聚合结果
 */
public class ItemViewCountEvent implements Serializable {
    private String itemId;
    private long windowEnd;
    private long count;

    public ItemViewCountEvent(String itemId, long windowEnd, long count) {
        this.itemId = itemId;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
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
