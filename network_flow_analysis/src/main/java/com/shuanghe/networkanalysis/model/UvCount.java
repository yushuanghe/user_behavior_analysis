package com.shuanghe.networkanalysis.model;

import java.io.Serializable;

/**
 * Description:
 * Date: 2021-03-30
 * Time: 20:44
 *
 * @author yushu
 */
public class UvCount implements Serializable {
    private long windowEnd;
    private long count;

    public UvCount(long windowEnd, long count) {
        this.windowEnd = windowEnd;
        this.count = count;
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
        return "UvCount{" +
                "windowEnd=" + windowEnd +
                ", count=" + count +
                '}';
    }
}
