package com.shuanghe.network.analysis.model;

import java.io.Serializable;

public class PvCount implements Serializable {
    private long windowEnd;
    private long count;

    public PvCount(long windowEnd, long count) {
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
        return "PvCount{" +
                "windowEnd=" + windowEnd +
                ", count=" + count +
                '}';
    }
}
