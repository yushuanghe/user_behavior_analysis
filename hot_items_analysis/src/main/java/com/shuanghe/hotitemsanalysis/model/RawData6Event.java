package com.shuanghe.hotitemsanalysis.model;

import java.io.Serializable;

/**
 * 定义输入数据
 *
 * @author yushu
 */
public class RawData6Event implements Serializable {
    private String uid;
    private String appId;
    private int channelId;
    private String behavior;
    private long timestamp;

    /**
     * table api 需要
     */
    public RawData6Event() {
    }

    public RawData6Event(String uid, String appId, int channelId, String behavior, long timestamp) {
        this.uid = uid;
        this.appId = appId;
        this.channelId = channelId;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public int getChannelId() {
        return channelId;
    }

    public void setChannelId(int channelId) {
        this.channelId = channelId;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "RawData6Event{" +
                "uid='" + uid + '\'' +
                ", appId='" + appId + '\'' +
                ", channelId=" + channelId +
                ", behavior='" + behavior + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
