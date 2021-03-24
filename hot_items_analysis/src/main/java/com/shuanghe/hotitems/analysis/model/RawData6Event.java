package com.shuanghe.hotitems.analysis.model;

import java.io.Serializable;

/**
 * 定义输入数据
 */
public class RawData6Event implements Serializable {
    private String uid;
    private int appId;
    private String appName;
    private String behavior;
    private long timestamp;

    public RawData6Event(String uid, int appId, String appName, String behavior, long timestamp) {
        this.uid = uid;
        this.appId = appId;
        this.appName = appName;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public int getAppId() {
        return appId;
    }

    public void setAppId(int appId) {
        this.appId = appId;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
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
}
