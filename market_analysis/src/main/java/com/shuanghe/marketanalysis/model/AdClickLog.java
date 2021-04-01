package com.shuanghe.marketanalysis.model;

import java.io.Serializable;

/**
 * @author yushu
 */
public class AdClickLog implements Serializable {
    private String uid;
    private int adType;
    private String appId;
    private String placementId;
    private long timestamp;

    public AdClickLog(String uid, int adType, String appId, String placementId, long timestamp) {
        this.uid = uid;
        this.adType = adType;
        this.appId = appId;
        this.placementId = placementId;
        this.timestamp = timestamp;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public int getAdType() {
        return adType;
    }

    public void setAdType(int adType) {
        this.adType = adType;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getPlacementId() {
        return placementId;
    }

    public void setPlacementId(String placementId) {
        this.placementId = placementId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "AdClickLog{" +
                "uid='" + uid + '\'' +
                ", adType=" + adType +
                ", appId='" + appId + '\'' +
                ", placementId='" + placementId + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
